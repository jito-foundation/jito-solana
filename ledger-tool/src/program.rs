use {
    crate::{args::*, canonicalize_ledger_path, ledger_utils::*},
    clap::{App, AppSettings, Arg, ArgMatches, SubCommand},
    log::*,
    serde_derive::{Deserialize, Serialize},
    serde_json::Result,
    solana_bpf_loader_program::{
        create_vm, load_program_from_bytes, serialization::serialize_parameters,
        syscalls::create_program_runtime_environment_v1,
    },
    solana_cli_output::{OutputFormat, QuietDisplay, VerboseDisplay},
    solana_ledger::blockstore_options::AccessType,
    solana_program_runtime::{
        invoke_context::InvokeContext,
        loaded_programs::{
            LoadProgramMetrics, ProgramCacheEntryType, DELAY_VISIBILITY_SLOT_OFFSET,
        },
        with_mock_invoke_context,
    },
    solana_rbpf::{
        assembler::assemble, elf::Executable, static_analysis::Analysis,
        verifier::RequisiteVerifier,
    },
    solana_runtime::bank::Bank,
    solana_sdk::{
        account::{create_account_shared_data_for_test, AccountSharedData},
        account_utils::StateMut,
        bpf_loader_upgradeable::{self, UpgradeableLoaderState},
        pubkey::Pubkey,
        slot_history::Slot,
        sysvar,
        transaction_context::{IndexOfAccount, InstructionAccount},
    },
    std::{
        collections::HashMap,
        fmt::{self, Debug, Formatter},
        fs::File,
        io::{Read, Seek, Write},
        path::Path,
        process::exit,
        sync::Arc,
        time::{Duration, Instant},
    },
};

// The ELF magic number [ELFMAG0, ELFMAG1, ELFGMAG2, ELFMAG3] as defined by
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/elf.h
const ELF_MAGIC_NUMBER: [u8; 4] = [0x7f, 0x45, 0x4c, 0x46];

#[derive(Serialize, Deserialize, Debug)]
struct Account {
    key: String,
    owner: Option<String>,
    is_signer: Option<bool>,
    is_writable: Option<bool>,
    lamports: Option<u64>,
    data: Option<Vec<u8>>,
}
#[derive(Serialize, Deserialize)]
struct Input {
    program_id: String,
    accounts: Vec<Account>,
    instruction_data: Vec<u8>,
}
fn load_accounts(path: &Path) -> Result<Input> {
    let file = File::open(path).unwrap();
    let input: Input = serde_json::from_reader(file)?;
    info!("Program input:");
    info!("program_id: {}", &input.program_id);
    info!("accounts {:?}", &input.accounts);
    info!("instruction_data {:?}", &input.instruction_data);
    info!("----------------------------------------");
    Ok(input)
}

fn load_blockstore(ledger_path: &Path, arg_matches: &ArgMatches<'_>) -> Arc<Bank> {
    let process_options = parse_process_options(ledger_path, arg_matches);

    let genesis_config = open_genesis_config_by(ledger_path, arg_matches);
    info!("genesis hash: {}", genesis_config.hash());
    let blockstore = open_blockstore(ledger_path, arg_matches, AccessType::Secondary);
    let LoadAndProcessLedgerOutput { bank_forks, .. } = load_and_process_ledger_or_exit(
        arg_matches,
        &genesis_config,
        Arc::new(blockstore),
        process_options,
        None,
        true,
    );
    let bank = bank_forks.read().unwrap().working_bank();
    bank
}

pub trait ProgramSubCommand {
    fn program_subcommand(self) -> Self;
}

impl ProgramSubCommand for App<'_, '_> {
    fn program_subcommand(self) -> Self {
        let program_arg = Arg::with_name("PROGRAM")
            .help(
                "Program file to use. This is either an ELF shared-object file to be executed, or \
                 an assembly file to be assembled and executed.",
            )
            .required(true)
            .index(1);

        let load_genesis_config_arg = load_genesis_arg();
        let snapshot_config_args = snapshot_args();

        self.subcommand(
            SubCommand::with_name("program")
        .about("Run to test, debug, and analyze on-chain programs.")
        .setting(AppSettings::InferSubcommands)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("cfg")
                .about("generates Control Flow Graph of the program.")
                .arg(&program_arg)
        )
        .subcommand(
            SubCommand::with_name("disassemble")
                .about("dumps disassembled code of the program.")
                .arg(&program_arg)
        )
        .subcommand(
            SubCommand::with_name("run")
                .about(
                    "The command executes on-chain programs in a mocked environment.",
                )
                .arg(
                    Arg::with_name("input")
                        .help(
                            r#"Input for the program to run on, where FILE is a name of a JSON file
with input data, or BYTES is the number of 0-valued bytes to allocate for program parameters"

The input data for a program execution have to be in JSON format
and the following fields are required
{
    "program_id": "DozgQiYtGbdyniV2T74xMdmjZJvYDzoRFFqw7UR5MwPK",
    "accounts": [
        {
            "key": "524HMdYYBy6TAn4dK5vCcjiTmT2sxV6Xoue5EXrz22Ca",
            "owner": "BPFLoaderUpgradeab1e11111111111111111111111",
            "is_signer": false,
            "is_writable": true,
            "lamports": 1000,
            "data": [0, 0, 0, 3]
        }
    ],
    "instruction_data": [31, 32, 23, 24]
}
"#,
                        )
                        .short("i")
                        .long("input")
                        .value_name("FILE / BYTES")
                        .takes_value(true)
                        .default_value("0"),
                )
                .arg(&load_genesis_config_arg)
                .args(&snapshot_config_args)
                .arg(
                    Arg::with_name("memory")
                        .help("Heap memory for the program to run on")
                        .short("m")
                        .long("memory")
                        .value_name("BYTES")
                        .takes_value(true)
                        .default_value("0"),
                )
                .arg(
                    Arg::with_name("mode")
                        .help(
                            "Mode of execution, where 'interpreter' runs \
                             the program in the virtual machine's interpreter, 'debugger' is the same as 'interpreter' \
                             but hosts a GDB interface, and 'jit' precompiles the program to native machine code \
                             before execting it in the virtual machine.",
                        )
                        .short("e")
                        .long("mode")
                        .takes_value(true)
                        .value_name("VALUE")
                        .possible_values(&["interpreter", "debugger", "jit"])
                        .default_value("jit"),
                )
                .arg(
                    Arg::with_name("instruction limit")
                        .help("Limit the number of instructions to execute")
                        .long("limit")
                        .takes_value(true)
                        .value_name("COUNT")
                        .default_value("9223372036854775807"),
                )
                .arg(
                    Arg::with_name("port")
                        .help("Port to use for the connection with a remote debugger")
                        .long("port")
                        .takes_value(true)
                        .value_name("PORT")
                        .default_value("9001"),
                )
                .arg(
                    Arg::with_name("trace")
                        .help("Output instruction trace")
                        .short("t")
                        .long("trace")
                        .takes_value(true)
                        .value_name("FILE"),
                )
                .arg(&program_arg)
        )
        )
    }
}

#[derive(Serialize)]
struct Output {
    result: String,
    instruction_count: u64,
    execution_time: Duration,
    log: Vec<String>,
}

impl fmt::Display for Output {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Program output:")?;
        writeln!(f, "Result: {}", self.result)?;
        writeln!(f, "Instruction Count: {}", self.instruction_count)?;
        writeln!(f, "Execution time: {} us", self.execution_time.as_micros())?;
        for line in &self.log {
            writeln!(f, "{line}")?;
        }
        Ok(())
    }
}

impl QuietDisplay for Output {}
impl VerboseDisplay for Output {}

// Replace with std::lazy::Lazy when stabilized.
// https://github.com/rust-lang/rust/issues/74465
struct LazyAnalysis<'a, 'b> {
    analysis: Option<Analysis<'a>>,
    executable: &'a Executable<InvokeContext<'b>>,
}

impl<'a, 'b> LazyAnalysis<'a, 'b> {
    fn new(executable: &'a Executable<InvokeContext<'b>>) -> Self {
        Self {
            analysis: None,
            executable,
        }
    }

    fn analyze(&mut self) -> &Analysis {
        if let Some(ref analysis) = self.analysis {
            return analysis;
        }
        self.analysis
            .insert(Analysis::from_executable(self.executable).unwrap())
    }
}

fn output_trace(
    matches: &ArgMatches<'_>,
    trace: &[[u64; 12]],
    frame: usize,
    analysis: &mut LazyAnalysis,
) {
    if matches.value_of("trace").unwrap() == "stdout" {
        writeln!(&mut std::io::stdout(), "Frame {frame}").unwrap();
        analysis
            .analyze()
            .disassemble_trace_log(&mut std::io::stdout(), trace)
            .unwrap();
    } else {
        let filename = format!("{}.{}", matches.value_of("trace").unwrap(), frame);
        let mut fd = File::create(filename).unwrap();
        writeln!(&fd, "Frame {frame}").unwrap();
        analysis
            .analyze()
            .disassemble_trace_log(&mut fd, trace)
            .unwrap();
    }
}

fn load_program<'a>(
    filename: &Path,
    program_id: Pubkey,
    invoke_context: &InvokeContext<'a>,
) -> Executable<InvokeContext<'a>> {
    let mut file = File::open(filename).unwrap();
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic).unwrap();
    file.rewind().unwrap();
    let is_elf = magic == ELF_MAGIC_NUMBER;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    let slot = Slot::default();
    let log_collector = invoke_context.get_log_collector();
    let loader_key = bpf_loader_upgradeable::id();
    let mut load_program_metrics = LoadProgramMetrics {
        program_id: program_id.to_string(),
        ..LoadProgramMetrics::default()
    };
    let account_size = contents.len();
    let program_runtime_environment = create_program_runtime_environment_v1(
        invoke_context.get_feature_set(),
        invoke_context.get_compute_budget(),
        false, /* deployment */
        true,  /* debugging_features */
    )
    .unwrap();
    // Allowing mut here, since it may be needed for jit compile, which is under a config flag
    #[allow(unused_mut)]
    let mut verified_executable = if is_elf {
        let result = load_program_from_bytes(
            log_collector,
            &mut load_program_metrics,
            &contents,
            &loader_key,
            account_size,
            slot,
            Arc::new(program_runtime_environment),
            false,
        );
        match result {
            Ok(loaded_program) => match loaded_program.program {
                ProgramCacheEntryType::Loaded(program) => Ok(program),
                _ => unreachable!(),
            },
            Err(err) => Err(format!("Loading executable failed: {err:?}")),
        }
    } else {
        assemble::<InvokeContext>(
            std::str::from_utf8(contents.as_slice()).unwrap(),
            Arc::new(program_runtime_environment),
        )
        .map_err(|err| format!("Assembling executable failed: {err:?}"))
        .and_then(|executable| {
            executable
                .verify::<RequisiteVerifier>()
                .map_err(|err| format!("Verifying executable failed: {err:?}"))?;
            Ok(executable)
        })
    }
    .unwrap();
    #[cfg(all(not(target_os = "windows"), target_arch = "x86_64"))]
    verified_executable.jit_compile().unwrap();
    unsafe {
        std::mem::transmute::<Executable<InvokeContext<'static>>, Executable<InvokeContext<'a>>>(
            verified_executable,
        )
    }
}

enum Action {
    Cfg,
    Dis,
}

fn process_static_action(action: Action, matches: &ArgMatches<'_>) {
    let transaction_accounts = Vec::new();
    let program_id = Pubkey::new_unique();
    with_mock_invoke_context!(invoke_context, transaction_context, transaction_accounts);
    let program = matches.value_of("PROGRAM").unwrap();
    let verified_executable = load_program(Path::new(program), program_id, &invoke_context);
    let mut analysis = LazyAnalysis::new(&verified_executable);
    match action {
        Action::Cfg => {
            let mut file = File::create("cfg.dot").unwrap();
            analysis
                .analyze()
                .visualize_graphically(&mut file, None)
                .unwrap();
        }
        Action::Dis => {
            let stdout = std::io::stdout();
            analysis.analyze().disassemble(&mut stdout.lock()).unwrap();
        }
    };
}

pub fn program(ledger_path: &Path, matches: &ArgMatches<'_>) {
    let matches = match matches.subcommand() {
        ("cfg", Some(arg_matches)) => {
            process_static_action(Action::Cfg, arg_matches);
            return;
        }
        ("disassemble", Some(arg_matches)) => {
            process_static_action(Action::Dis, arg_matches);
            return;
        }
        ("run", Some(arg_matches)) => arg_matches,
        _ => unreachable!(),
    };
    let ledger_path = canonicalize_ledger_path(ledger_path);
    let bank = load_blockstore(&ledger_path, matches);
    let loader_id = bpf_loader_upgradeable::id();
    let mut transaction_accounts = Vec::new();
    let mut instruction_accounts = Vec::new();
    let mut program_id = Pubkey::new_unique();
    let mut cached_account_keys = vec![];

    let instruction_data = match matches.value_of("input").unwrap().parse::<usize>() {
        Ok(allocation_size) => {
            let pubkey = Pubkey::new_unique();
            transaction_accounts.push((
                pubkey,
                AccountSharedData::new(0, allocation_size, &Pubkey::new_unique()),
            ));
            instruction_accounts.push(InstructionAccount {
                index_in_transaction: 0,
                index_in_caller: 0,
                index_in_callee: 0,
                is_signer: false,
                is_writable: true,
            });
            vec![]
        }
        Err(_) => {
            let input = load_accounts(Path::new(matches.value_of("input").unwrap())).unwrap();
            program_id = input.program_id.parse::<Pubkey>().unwrap_or_else(|err| {
                eprintln!(
                    "Invalid program ID in input {}, error {}",
                    input.program_id, err,
                );
                program_id
            });
            // Maps a public key to the transaction account index
            let mut txn_acct_indices =
                HashMap::<Pubkey, usize>::with_capacity(input.accounts.len());
            instruction_accounts = input
                .accounts
                .into_iter()
                .map(|account_info| {
                    let pubkey = account_info.key.parse::<Pubkey>().unwrap_or_else(|err| {
                        eprintln!("Invalid key in input {}, error {}", account_info.key, err);
                        exit(1);
                    });
                    let data = account_info.data.unwrap_or_default();
                    let space = data.len();
                    let account = if let Some(account) = bank.get_account_with_fixed_root(&pubkey) {
                        let owner = *account.owner();
                        if bpf_loader_upgradeable::check_id(&owner) {
                            if let Ok(UpgradeableLoaderState::Program {
                                programdata_address,
                            }) = account.state()
                            {
                                debug!("Program data address {}", programdata_address);
                                if bank
                                    .get_account_with_fixed_root(&programdata_address)
                                    .is_some()
                                {
                                    cached_account_keys.push(pubkey);
                                }
                            }
                        }
                        // Override account data and lamports from input file if provided
                        if space > 0 {
                            let lamports = account_info.lamports.unwrap_or(account.lamports());
                            let mut account = AccountSharedData::new(lamports, space, &owner);
                            account.set_data_from_slice(&data);
                            account
                        } else {
                            account
                        }
                    } else {
                        let owner = account_info
                            .owner
                            .unwrap_or(Pubkey::new_unique().to_string());
                        let owner = owner.parse::<Pubkey>().unwrap_or_else(|err| {
                            eprintln!("Invalid owner key in input {owner}, error {err}");
                            Pubkey::new_unique()
                        });
                        let lamports = account_info.lamports.unwrap_or(0);
                        let mut account = AccountSharedData::new(lamports, space, &owner);
                        account.set_data_from_slice(&data);
                        account
                    };
                    let txn_acct_index = if let Some(idx) = txn_acct_indices.get(&pubkey) {
                        *idx
                    } else {
                        let idx = transaction_accounts.len();
                        txn_acct_indices.insert(pubkey, idx);
                        transaction_accounts.push((pubkey, account));
                        idx
                    };
                    InstructionAccount {
                        index_in_transaction: txn_acct_index as IndexOfAccount,
                        index_in_caller: txn_acct_index as IndexOfAccount,
                        index_in_callee: txn_acct_index as IndexOfAccount,
                        is_signer: account_info.is_signer.unwrap_or(false),
                        is_writable: account_info.is_writable.unwrap_or(false),
                    }
                })
                .collect();
            input.instruction_data
        }
    };
    let program_index: u16 = instruction_accounts.len().try_into().unwrap();
    transaction_accounts.push((
        loader_id,
        AccountSharedData::new(0, 0, &solana_sdk::native_loader::id()),
    ));
    transaction_accounts.push((
        program_id, // ID of the loaded program. It can modify accounts with the same owner key
        AccountSharedData::new(0, 0, &loader_id),
    ));
    transaction_accounts.push((
        sysvar::epoch_schedule::id(),
        create_account_shared_data_for_test(bank.epoch_schedule()),
    ));
    let interpreted = matches.value_of("mode").unwrap() != "jit";
    with_mock_invoke_context!(invoke_context, transaction_context, transaction_accounts);

    // Adding `DELAY_VISIBILITY_SLOT_OFFSET` to slots to accommodate for delay visibility of the program
    let mut program_cache_for_tx_batch =
        bank.new_program_cache_for_tx_batch_for_slot(bank.slot() + DELAY_VISIBILITY_SLOT_OFFSET);
    for key in cached_account_keys {
        program_cache_for_tx_batch.replenish(
            key,
            bank.load_program(&key, false, bank.epoch())
                .expect("Couldn't find program account"),
        );
        debug!("Loaded program {}", key);
    }
    invoke_context.program_cache_for_tx_batch = &mut program_cache_for_tx_batch;

    invoke_context
        .transaction_context
        .get_next_instruction_context()
        .unwrap()
        .configure(
            &[program_index, program_index.saturating_add(1)],
            &instruction_accounts,
            &instruction_data,
        );
    invoke_context.push().unwrap();
    let (_parameter_bytes, regions, account_lengths) = serialize_parameters(
        invoke_context.transaction_context,
        invoke_context
            .transaction_context
            .get_current_instruction_context()
            .unwrap(),
        true, // copy_account_data
    )
    .unwrap();

    let program = matches.value_of("PROGRAM").unwrap();
    let verified_executable = load_program(Path::new(program), program_id, &invoke_context);
    let mut analysis = LazyAnalysis::new(&verified_executable);
    create_vm!(
        vm,
        &verified_executable,
        regions,
        account_lengths,
        &mut invoke_context,
    );
    let (mut vm, _, _) = vm.unwrap();
    let start_time = Instant::now();
    if matches.value_of("mode").unwrap() == "debugger" {
        vm.debug_port = Some(matches.value_of("port").unwrap().parse::<u16>().unwrap());
    }
    let (instruction_count, result) = vm.execute_program(&verified_executable, interpreted);
    let duration = Instant::now() - start_time;
    if matches.occurrences_of("trace") > 0 {
        // top level trace is stored in syscall_context
        if let Some(Some(syscall_context)) = vm.context_object_pointer.syscall_context.last() {
            let trace = syscall_context.trace_log.as_slice();
            output_trace(matches, trace, 0, &mut analysis);
        }
        // the remaining traces are saved in InvokeContext when
        // corresponding syscall_contexts are popped
        let traces = vm.context_object_pointer.get_traces();
        for (frame, trace) in traces.iter().filter(|t| !t.is_empty()).enumerate() {
            output_trace(matches, trace, frame + 1, &mut analysis);
        }
    }
    drop(vm);

    let output = Output {
        result: format!("{result:?}"),
        instruction_count,
        execution_time: duration,
        log: invoke_context
            .get_log_collector()
            .unwrap()
            .borrow()
            .get_recorded_content()
            .to_vec(),
    };
    let output_format = OutputFormat::from_matches(matches, "output_format", false);
    println!("{}", output_format.formatted_string(&output));
}
