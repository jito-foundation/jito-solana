//! The `rpc_banks_service` module implements the Solana Banks RPC API.

use {
    crate::banks_server::start_tcp_server,
    futures::{future::FutureExt, pin_mut, prelude::stream::StreamExt, select},
    solana_client::connection_cache::ConnectionCache,
    solana_gossip::cluster_info::ClusterInfo,
    solana_runtime::{bank_forks::BankForks, commitment::BlockCommitmentCache},
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
    },
    tokio::{
        runtime::Runtime,
        time::{self, Duration},
    },
    tokio_stream::wrappers::IntervalStream,
};

pub struct RpcBanksService {
    thread_hdl: JoinHandle<()>,
}

/// Run the TCP service until `exit` is set to true
async fn start_abortable_tcp_server(
    listen_addr: SocketAddr,
    cluster_info: Arc<ClusterInfo>,
    bank_forks: Arc<RwLock<BankForks>>,
    block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    connection_cache: Arc<ConnectionCache>,
    exit: Arc<AtomicBool>,
) {
    let server = start_tcp_server(
        listen_addr,
        cluster_info,
        bank_forks.clone(),
        block_commitment_cache.clone(),
        connection_cache,
        exit.clone(),
    )
    .fuse();
    let interval = IntervalStream::new(time::interval(Duration::from_millis(100))).fuse();
    pin_mut!(server, interval);
    loop {
        select! {
            _ = server => {},
            _ = interval.select_next_some() => {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }
}

impl RpcBanksService {
    fn run(
        listen_addr: SocketAddr,
        cluster_info: Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        connection_cache: Arc<ConnectionCache>,
        exit: Arc<AtomicBool>,
    ) {
        let server = start_abortable_tcp_server(
            listen_addr,
            cluster_info,
            bank_forks,
            block_commitment_cache,
            connection_cache,
            exit,
        );
        Runtime::new().unwrap().block_on(server);
    }

    pub fn new(
        listen_addr: SocketAddr,
        cluster_info: Arc<ClusterInfo>,
        bank_forks: &Arc<RwLock<BankForks>>,
        block_commitment_cache: &Arc<RwLock<BlockCommitmentCache>>,
        connection_cache: &Arc<ConnectionCache>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let bank_forks = bank_forks.clone();
        let block_commitment_cache = block_commitment_cache.clone();
        let connection_cache = connection_cache.clone();
        let exit = exit.clone();
        let thread_hdl = Builder::new()
            .name("solRpcBanksSvc".to_string())
            .spawn(move || {
                Self::run(
                    listen_addr,
                    cluster_info,
                    bank_forks,
                    block_commitment_cache,
                    connection_cache,
                    exit,
                )
            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_gossip::legacy_contact_info::LegacyContactInfo as ContactInfo,
        solana_runtime::bank::Bank,
        solana_sdk::signature::Keypair,
        solana_streamer::socket::SocketAddrSpace,
        std::net::{IpAddr, Ipv4Addr},
    };

    #[test]
    fn test_rpc_banks_server_exit() {
        let bank_forks = Arc::new(RwLock::new(BankForks::new(Bank::default_for_tests())));
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let connection_cache = Arc::new(ConnectionCache::default());
        let exit = Arc::new(AtomicBool::new(false));
        let addr = "127.0.0.1:0".parse().unwrap();
        let contact_info = ContactInfo {
            tpu: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            ..ContactInfo::default()
        };
        let cluster_info: Arc<ClusterInfo> = Arc::new(ClusterInfo::new(
            contact_info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::new(false),
        ));
        let service = RpcBanksService::new(
            addr,
            cluster_info,
            &bank_forks,
            &block_commitment_cache,
            &connection_cache,
            &exit,
        );
        exit.store(true, Ordering::Relaxed);
        service.join().unwrap();
    }
}
