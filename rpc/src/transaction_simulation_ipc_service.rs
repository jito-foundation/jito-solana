//! Low-latency transaction simulation over a local Unix socket.
//!
//! Aperture can decode transactions directly from shreds and pipeline them over
//! one persistent connection. Agave simulates each transaction against either
//! the requested frozen bank or the highest frozen bank, then returns a
//! request-correlated result without using JSON-RPC.

use {
    crossbeam_channel::{Receiver, Sender, TrySendError, bounded},
    prost::{Enumeration, Message},
    solana_runtime::bank_forks::BankForks,
    solana_transaction::{TransactionVerificationMode, versioned::VersionedTransaction},
    std::{
        fs,
        io::{self, ErrorKind, Read, Write},
        net::Shutdown,
        os::unix::{
            fs::PermissionsExt,
            net::{UnixListener, UnixStream},
        },
        path::{Path, PathBuf},
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

pub const TRANSACTION_SIMULATION_IPC_PROTOCOL_VERSION: u32 = 1;
const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
const JOB_QUEUE_CAPACITY: usize = 8_192;
const RESPONSE_QUEUE_CAPACITY: usize = 1_024;
const LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(1);
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_millis(100);
type ResponseFrame = Arc<[u8]>;

#[derive(Clone, PartialEq, Message)]
pub struct IpcMessage {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(oneof = "ipc_message::Payload", tags = "2, 3")]
    pub payload: Option<ipc_message::Payload>,
}

pub mod ipc_message {
    use super::{SimulationRequest, SimulationResult};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Payload {
        #[prost(message, tag = "2")]
        SimulationRequest(SimulationRequest),
        #[prost(message, tag = "3")]
        SimulationResult(SimulationResult),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct SimulationRequest {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    /// Canonical Solana wire encoding of `VersionedTransaction`.
    #[prost(bytes = "vec", tag = "2")]
    pub transaction: Vec<u8>,
    #[prost(uint64, optional, tag = "3")]
    pub bank_slot: Option<u64>,
    #[prost(bool, tag = "4")]
    pub sig_verify: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub enum SimulationStatus {
    Unspecified = 0,
    Succeeded = 1,
    Failed = 2,
    InvalidTransaction = 3,
    BankNotAvailable = 4,
    ServerOverloaded = 5,
    InternalError = 6,
}

#[derive(Clone, PartialEq, Message)]
pub struct TransactionReturnData {
    #[prost(bytes = "vec", tag = "1")]
    pub program_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SimulationResult {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub signature: Vec<u8>,
    #[prost(uint64, optional, tag = "3")]
    pub bank_slot: Option<u64>,
    #[prost(enumeration = "SimulationStatus", tag = "4")]
    pub status: i32,
    #[prost(string, optional, tag = "5")]
    pub error: Option<String>,
    #[prost(string, repeated, tag = "6")]
    pub logs: Vec<String>,
    #[prost(uint64, optional, tag = "7")]
    pub compute_units_consumed: Option<u64>,
    #[prost(uint64, optional, tag = "8")]
    pub fee: Option<u64>,
    #[prost(message, optional, tag = "9")]
    pub return_data: Option<TransactionReturnData>,
    #[prost(uint64, tag = "10")]
    pub simulated_at_unix_nanos: u64,
    #[prost(uint64, tag = "11")]
    pub processing_time_nanos: u64,
}

pub struct TransactionSimulationIpcService {
    shutdown: Arc<AtomicBool>,
    listener_thread: Option<JoinHandle<()>>,
    worker_threads: Vec<JoinHandle<()>>,
}

impl TransactionSimulationIpcService {
    pub fn new(
        socket_path: PathBuf,
        bank_forks: Arc<RwLock<BankForks>>,
        worker_count: usize,
        exit: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        if worker_count == 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "transaction simulation IPC requires at least one worker",
            ));
        }

        let listener = bind_listener(&socket_path)?;
        let shutdown = Arc::new(AtomicBool::default());
        let (job_sender, job_receiver) = bounded(JOB_QUEUE_CAPACITY);
        let mut worker_threads = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let job_receiver = job_receiver.clone();
            let bank_forks = bank_forks.clone();
            match Builder::new()
                .name(format!("solTxSimIpc{worker_index:02}"))
                .spawn(move || run_worker(job_receiver, bank_forks))
            {
                Ok(thread) => worker_threads.push(thread),
                Err(err) => {
                    drop(job_sender);
                    for thread in worker_threads {
                        let _ = thread.join();
                    }
                    let _ = fs::remove_file(&socket_path);
                    return Err(err);
                }
            }
        }
        drop(job_receiver);

        let thread_shutdown = shutdown.clone();
        let thread_socket_path = socket_path.clone();
        let listener_job_sender = job_sender.clone();
        let listener_thread =
            match Builder::new()
                .name("solTxSimIpc".to_string())
                .spawn(move || {
                    run_listener(
                        listener,
                        listener_job_sender,
                        exit,
                        thread_shutdown,
                        &thread_socket_path,
                    );
                }) {
                Ok(thread) => thread,
                Err(err) => {
                    drop(job_sender);
                    for thread in worker_threads {
                        let _ = thread.join();
                    }
                    let _ = fs::remove_file(&socket_path);
                    return Err(err);
                }
            };
        drop(job_sender);

        Ok(Self {
            shutdown,
            listener_thread: Some(listener_thread),
            worker_threads,
        })
    }

    pub fn join(mut self) -> thread::Result<()> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> thread::Result<()> {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(listener_thread) = self.listener_thread.take() {
            listener_thread.join()?;
        }
        for worker_thread in self.worker_threads.drain(..) {
            worker_thread.join()?;
        }
        Ok(())
    }
}

impl Drop for TransactionSimulationIpcService {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

struct SimulationJob {
    request: SimulationRequest,
    response_sender: Sender<ResponseFrame>,
    response_control_stream: Arc<UnixStream>,
}

struct Connection {
    control_stream: UnixStream,
    thread: JoinHandle<()>,
}

fn bind_listener(socket_path: &Path) -> io::Result<UnixListener> {
    if socket_path.exists() {
        match UnixStream::connect(socket_path) {
            Ok(_) => {
                return Err(io::Error::new(
                    ErrorKind::AddrInUse,
                    format!(
                        "transaction simulation IPC socket is already active: {}",
                        socket_path.display()
                    ),
                ));
            }
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::ConnectionRefused | ErrorKind::NotFound
                ) =>
            {
                fs::remove_file(socket_path)?;
            }
            Err(err) => return Err(err),
        }
    }

    let listener = UnixListener::bind(socket_path)?;
    listener.set_nonblocking(true)?;
    let mut permissions = fs::metadata(socket_path)?.permissions();
    permissions.set_mode(0o660);
    fs::set_permissions(socket_path, permissions)?;
    Ok(listener)
}

fn run_listener(
    listener: UnixListener,
    job_sender: Sender<SimulationJob>,
    exit: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    socket_path: &Path,
) {
    info!(
        "transaction simulation IPC service listening on {}",
        socket_path.display()
    );
    let mut connections = Vec::new();
    while !exit.load(Ordering::Relaxed) && !shutdown.load(Ordering::Relaxed) {
        accept_connections(&listener, &job_sender, &mut connections);
        reap_connections(&mut connections);
        thread::sleep(LISTENER_POLL_INTERVAL);
    }
    for connection in connections {
        let _ = connection.control_stream.shutdown(Shutdown::Both);
        let _ = connection.thread.join();
    }
    if let Err(err) = fs::remove_file(socket_path) {
        if err.kind() != ErrorKind::NotFound {
            warn!(
                "failed to remove transaction simulation IPC socket {}: {err}",
                socket_path.display()
            );
        }
    }
    info!("transaction simulation IPC service stopped");
}

fn accept_connections(
    listener: &UnixListener,
    job_sender: &Sender<SimulationJob>,
    connections: &mut Vec<Connection>,
) {
    loop {
        let (stream, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(err) if err.kind() == ErrorKind::WouldBlock => return,
            Err(err) => {
                warn!("failed to accept transaction simulation IPC client: {err}");
                return;
            }
        };
        if let Err(err) = stream.set_nonblocking(false) {
            warn!("failed to configure transaction simulation IPC client: {err}");
            continue;
        }
        let control_stream = match stream.try_clone() {
            Ok(stream) => stream,
            Err(err) => {
                warn!("failed to clone transaction simulation IPC client: {err}");
                continue;
            }
        };
        let job_sender = job_sender.clone();
        match Builder::new()
            .name("solTxSimIpcCl".to_string())
            .spawn(move || run_connection(stream, job_sender))
        {
            Ok(thread) => connections.push(Connection {
                control_stream,
                thread,
            }),
            Err(err) => warn!("failed to start transaction simulation IPC client: {err}"),
        }
    }
}

fn reap_connections(connections: &mut Vec<Connection>) {
    let mut index = 0;
    while index < connections.len() {
        if connections[index].thread.is_finished() {
            let connection = connections.swap_remove(index);
            let _ = connection.thread.join();
        } else {
            index += 1;
        }
    }
}

fn run_connection(mut stream: UnixStream, job_sender: Sender<SimulationJob>) {
    let mut writer_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(err) => {
            warn!("failed to clone transaction simulation IPC writer: {err}");
            return;
        }
    };
    if let Err(err) = writer_stream.set_write_timeout(Some(CLIENT_WRITE_TIMEOUT)) {
        warn!("failed to configure transaction simulation IPC writer: {err}");
        return;
    }
    let writer_control_stream = match writer_stream.try_clone() {
        Ok(stream) => stream,
        Err(err) => {
            warn!("failed to clone transaction simulation IPC writer control: {err}");
            return;
        }
    };
    let (response_sender, response_receiver): (Sender<ResponseFrame>, Receiver<ResponseFrame>) =
        bounded(RESPONSE_QUEUE_CAPACITY);
    let response_control_stream = match stream.try_clone() {
        Ok(stream) => Arc::new(stream),
        Err(err) => {
            warn!("failed to clone transaction simulation IPC response control: {err}");
            return;
        }
    };
    let writer_thread = Builder::new()
        .name("solTxSimIpcWr".to_string())
        .spawn(move || {
            while let Ok(frame) = response_receiver.recv() {
                if writer_stream.write_all(&frame).is_err() {
                    break;
                }
            }
            let _ = writer_control_stream.shutdown(Shutdown::Both);
        });
    let Ok(writer_thread) = writer_thread else {
        warn!("failed to start transaction simulation IPC response writer");
        return;
    };

    loop {
        let message = match read_frame(&mut stream) {
            Ok(message) => message,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset | ErrorKind::BrokenPipe
                ) =>
            {
                break;
            }
            Err(err) => {
                warn!("invalid transaction simulation IPC frame: {err}");
                break;
            }
        };
        let Some(ipc_message::Payload::SimulationRequest(request)) = message.payload else {
            warn!("transaction simulation IPC client sent a non-request payload");
            break;
        };
        if message.protocol_version != TRANSACTION_SIMULATION_IPC_PROTOCOL_VERSION {
            let response = error_result(
                request.request_id,
                SimulationStatus::InvalidTransaction,
                format!(
                    "unsupported protocol version {}, expected {}",
                    message.protocol_version, TRANSACTION_SIMULATION_IPC_PROTOCOL_VERSION
                ),
            );
            if response_sender.try_send(encode_result(response)).is_err() {
                break;
            }
            continue;
        }

        let job = SimulationJob {
            request,
            response_sender: response_sender.clone(),
            response_control_stream: response_control_stream.clone(),
        };
        match job_sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                let response = error_result(
                    job.request.request_id,
                    SimulationStatus::ServerOverloaded,
                    "transaction simulation queue is full".to_string(),
                );
                if response_sender.try_send(encode_result(response)).is_err() {
                    break;
                }
            }
            Err(TrySendError::Disconnected(_)) => break,
        }
    }

    drop(response_sender);
    let _ = writer_thread.join();
}

fn read_frame(stream: &mut UnixStream) -> io::Result<IpcMessage> {
    let mut length = [0u8; size_of::<u32>()];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid frame length {length}"),
        ));
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload)?;
    IpcMessage::decode(payload.as_slice())
        .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))
}

fn run_worker(job_receiver: Receiver<SimulationJob>, bank_forks: Arc<RwLock<BankForks>>) {
    while let Ok(job) = job_receiver.recv() {
        let response = simulate(job.request, &bank_forks);
        if job
            .response_sender
            .try_send(encode_result(response))
            .is_err()
        {
            let _ = job.response_control_stream.shutdown(Shutdown::Both);
        }
    }
}

fn simulate(request: SimulationRequest, bank_forks: &Arc<RwLock<BankForks>>) -> SimulationResult {
    let started_at = Instant::now();
    let bank = {
        let bank_forks = bank_forks.read().unwrap();
        match request.bank_slot {
            Some(slot) => bank_forks.get(slot),
            None => bank_forks.highest_frozen_bank(),
        }
    };
    let Some(bank) = bank else {
        return timed_error_result(
            request.request_id,
            SimulationStatus::BankNotAvailable,
            request.bank_slot.map_or_else(
                || "no frozen bank is available".to_string(),
                |slot| format!("bank {slot} is not available"),
            ),
            started_at,
        );
    };
    if !bank.is_frozen() {
        return timed_error_result(
            request.request_id,
            SimulationStatus::BankNotAvailable,
            format!("bank {} is not frozen", bank.slot()),
            started_at,
        );
    }

    let transaction = match wincode::deserialize::<VersionedTransaction>(&request.transaction) {
        Ok(transaction) => transaction,
        Err(err) => {
            return timed_error_result(
                request.request_id,
                SimulationStatus::InvalidTransaction,
                format!("invalid transaction encoding: {err}"),
                started_at,
            );
        }
    };
    let signature = transaction
        .signatures
        .first()
        .map(|signature| signature.as_ref().to_vec())
        .unwrap_or_default();
    let verification_mode = if request.sig_verify {
        TransactionVerificationMode::FullVerification
    } else {
        TransactionVerificationMode::HashOnly
    };
    let transaction = match bank.verify_transaction(transaction, verification_mode) {
        Ok(transaction) => transaction,
        Err(err) => {
            let mut response = timed_error_result(
                request.request_id,
                SimulationStatus::InvalidTransaction,
                format!("{err:?}"),
                started_at,
            );
            response.signature = signature;
            response.bank_slot = Some(bank.slot());
            return response;
        }
    };
    let result = bank.simulate_transaction(&transaction, false);
    let status = if result.result.is_ok() {
        SimulationStatus::Succeeded
    } else {
        SimulationStatus::Failed
    };
    let error = result.result.err().map(|err| format!("{err:?}"));
    let return_data = result.return_data.map(|return_data| TransactionReturnData {
        program_id: return_data.program_id.to_bytes().to_vec(),
        data: return_data.data,
    });

    SimulationResult {
        request_id: request.request_id,
        signature,
        bank_slot: Some(bank.slot()),
        status: status.into(),
        error,
        logs: result.logs,
        compute_units_consumed: Some(result.units_consumed),
        fee: result.fee,
        return_data,
        simulated_at_unix_nanos: unix_nanos(),
        processing_time_nanos: duration_nanos(started_at.elapsed()),
    }
}

fn error_result(request_id: u64, status: SimulationStatus, error: String) -> SimulationResult {
    SimulationResult {
        request_id,
        signature: Vec::new(),
        bank_slot: None,
        status: status.into(),
        error: Some(error),
        logs: Vec::new(),
        compute_units_consumed: None,
        fee: None,
        return_data: None,
        simulated_at_unix_nanos: unix_nanos(),
        processing_time_nanos: 0,
    }
}

fn timed_error_result(
    request_id: u64,
    status: SimulationStatus,
    error: String,
    started_at: Instant,
) -> SimulationResult {
    let mut response = error_result(request_id, status, error);
    response.processing_time_nanos = duration_nanos(started_at.elapsed());
    response
}

fn encode_result(result: SimulationResult) -> Arc<[u8]> {
    encode_frame(&IpcMessage {
        protocol_version: TRANSACTION_SIMULATION_IPC_PROTOCOL_VERSION,
        payload: Some(ipc_message::Payload::SimulationResult(result)),
    })
}

pub fn encode_frame(message: &IpcMessage) -> Arc<[u8]> {
    let payload_len = message.encoded_len();
    let mut frame = Vec::with_capacity(size_of::<u32>() + payload_len);
    frame.extend_from_slice(&(payload_len as u32).to_be_bytes());
    message
        .encode(&mut frame)
        .expect("encoding into a preallocated Vec cannot fail");
    frame.into()
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        prost::Message,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_system_transaction as system_transaction,
        std::{io::Read, sync::atomic::AtomicBool, time::Duration},
        tempfile::TempDir,
    };

    #[test]
    fn test_simulates_over_unix_stream() {
        let genesis = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        bank.freeze();
        let transaction = system_transaction::transfer(
            &genesis.mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank.last_blockhash(),
        );
        let bank_forks = BankForks::new_rw_arc(bank);
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("transaction-simulation.sock");
        let exit = Arc::new(AtomicBool::default());
        let service =
            TransactionSimulationIpcService::new(socket_path.clone(), bank_forks, 2, exit.clone())
                .unwrap();
        let mut stream = UnixStream::connect(&socket_path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let request = IpcMessage {
            protocol_version: TRANSACTION_SIMULATION_IPC_PROTOCOL_VERSION,
            payload: Some(ipc_message::Payload::SimulationRequest(SimulationRequest {
                request_id: 42,
                transaction: bincode::serialize(&VersionedTransaction::from(transaction)).unwrap(),
                bank_slot: None,
                sig_verify: true,
            })),
        };
        stream.write_all(&encode_frame(&request)).unwrap();
        let response = read_response(&mut stream);

        assert_eq!(response.request_id, 42);
        assert_eq!(response.bank_slot, Some(0));
        assert_eq!(response.status, SimulationStatus::Succeeded as i32);
        assert!(response.error.is_none());
        assert!(response.compute_units_consumed.is_some());
        assert!(response.processing_time_nanos > 0);

        exit.store(true, Ordering::Relaxed);
        service.join().unwrap();
        assert!(!socket_path.exists());
    }

    #[test]
    fn test_rejects_missing_requested_bank() {
        let genesis = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        bank.freeze();
        let bank_forks = BankForks::new_rw_arc(bank);
        let response = simulate(
            SimulationRequest {
                request_id: 7,
                transaction: vec![],
                bank_slot: Some(999),
                sig_verify: false,
            },
            &bank_forks,
        );
        assert_eq!(response.request_id, 7);
        assert_eq!(response.status, SimulationStatus::BankNotAvailable as i32);
        assert_eq!(response.error.as_deref(), Some("bank 999 is not available"));
    }

    fn read_response(stream: &mut UnixStream) -> SimulationResult {
        let mut length = [0u8; size_of::<u32>()];
        stream.read_exact(&mut length).unwrap();
        let mut payload = vec![0u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut payload).unwrap();
        let message = IpcMessage::decode(payload.as_slice()).unwrap();
        let Some(ipc_message::Payload::SimulationResult(result)) = message.payload else {
            panic!("expected simulation result");
        };
        result
    }
}
