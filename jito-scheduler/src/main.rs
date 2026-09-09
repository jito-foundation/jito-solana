use std::{
    path::PathBuf,
    process::ExitCode,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

#[cfg(unix)]
fn start() -> Result<(), Box<dyn std::error::Error>> {
    let mut ipc_path = None;
    let mut workers = 8;
    let mut check_workers = 4;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!(
                    "jito-scheduler (--ledger PATH | --ipc-path PATH) [--workers N] \
                     [--check-workers N]\n\nRuns Jito scheduling policy using the validator's \
                     opt-in shared-memory interface.\nThe validator must enable Jito scheduler \
                     bindings. Reconnects after a lost validator session;\nin-flight outcomes are \
                     discarded without replay. Protocol errors exit unsuccessfully."
                );
                return Ok(());
            }
            "--ledger" => {
                if ipc_path.is_some() {
                    return Err("specify exactly one IPC path or ledger".into());
                }
                ipc_path = Some(
                    PathBuf::from(args.next().ok_or("missing ledger path")?)
                        .join("scheduler_bindings.ipc"),
                );
            }
            "--ipc-path" => {
                if ipc_path.is_some() {
                    return Err("specify exactly one IPC path or ledger".into());
                }
                ipc_path = Some(PathBuf::from(args.next().ok_or("missing IPC path")?));
            }
            "--workers" => workers = args.next().ok_or("missing worker count")?.parse()?,
            "--check-workers" => {
                check_workers = args.next().ok_or("missing check worker count")?.parse()?
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    if workers == 0
        || workers > agave_scheduling_utils::handshake::MAX_JITO_WORKERS
        || check_workers == 0
        || check_workers > 64
    {
        return Err("workers must be in 1..=61 and check workers in 1..=64".into());
    }
    let ipc_path = ipc_path.ok_or("specify --ledger PATH or --ipc-path PATH")?;
    let mut waiting = false;
    loop {
        use agave_scheduling_utils::handshake::ClientHandshakeError;
        let session = match agave_scheduling_utils::handshake::client::connect(
            &ipc_path,
            jito_scheduler::client_logon(workers, check_workers),
            Duration::from_secs(5),
        ) {
            Ok(session) => {
                waiting = false;
                session
            }
            Err(error @ (ClientHandshakeError::Io(_) | ClientHandshakeError::TimedOut)) => {
                if !waiting {
                    eprintln!("jito-scheduler: waiting for validator: {error}");
                    waiting = true;
                }
                std::thread::sleep(Duration::from_millis(250));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match jito_scheduler::run(
            session,
            Arc::new(AtomicBool::new(false)),
            Default::default(),
        ) {
            Err(jito_scheduler::SchedulerError::SessionTimeout) => {
                eprintln!(
                    "jito-scheduler: validator session lost; abandoning unknown outcomes and \
                     reconnecting"
                );
                std::thread::sleep(Duration::from_millis(250));
            }
            Err(error) => return Err(error.into()),
            Ok(_) => return Ok(()),
        }
    }
}

fn main() -> ExitCode {
    #[cfg(unix)]
    match start() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("jito-scheduler: {error}");
            ExitCode::FAILURE
        }
    }
    #[cfg(not(unix))]
    {
        eprintln!("jito-scheduler requires Unix shared-memory support");
        ExitCode::FAILURE
    }
}
