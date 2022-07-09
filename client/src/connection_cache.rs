use {
    crate::{
        nonblocking::{
            quic_client::{QuicClient, QuicLazyInitializedEndpoint},
            tpu_connection::NonblockingConnection,
        },
        tpu_connection::{BlockingConnection, ClientStats},
    },
    indexmap::map::{Entry, IndexMap},
    rand::{thread_rng, Rng},
    solana_measure::measure::Measure,
    solana_sdk::{quic::QUIC_PORT_OFFSET, timing::AtomicInterval},
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
    },
};

// Should be non-zero
static MAX_CONNECTIONS: usize = 1024;

/// Used to decide whether the TPU and underlying connection cache should use
/// QUIC connections.
pub const DEFAULT_TPU_USE_QUIC: bool = false;

/// Default TPU connection pool size per remote address
pub const DEFAULT_TPU_CONNECTION_POOL_SIZE: usize = 4;

#[derive(Default)]
pub struct ConnectionCacheStats {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    eviction_time_ms: AtomicU64,
    sent_packets: AtomicU64,
    total_batches: AtomicU64,
    batch_success: AtomicU64,
    batch_failure: AtomicU64,
    get_connection_ms: AtomicU64,
    get_connection_lock_ms: AtomicU64,
    get_connection_hit_ms: AtomicU64,
    get_connection_miss_ms: AtomicU64,

    // Need to track these separately per-connection
    // because we need to track the base stat value from quinn
    pub total_client_stats: ClientStats,
}

const CONNECTION_STAT_SUBMISSION_INTERVAL: u64 = 2000;

impl ConnectionCacheStats {
    pub fn add_client_stats(
        &self,
        client_stats: &ClientStats,
        num_packets: usize,
        is_success: bool,
    ) {
        self.total_client_stats.total_connections.fetch_add(
            client_stats.total_connections.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.connection_reuse.fetch_add(
            client_stats.connection_reuse.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.connection_errors.fetch_add(
            client_stats.connection_errors.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.zero_rtt_accepts.fetch_add(
            client_stats.zero_rtt_accepts.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.zero_rtt_rejects.fetch_add(
            client_stats.zero_rtt_rejects.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_client_stats.make_connection_ms.fetch_add(
            client_stats.make_connection_ms.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.sent_packets
            .fetch_add(num_packets as u64, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        if is_success {
            self.batch_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.batch_failure.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn report(&self) {
        datapoint_info!(
            "quic-client-connection-stats",
            (
                "cache_hits",
                self.cache_hits.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "cache_misses",
                self.cache_misses.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "cache_evictions",
                self.cache_evictions.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "eviction_time_ms",
                self.eviction_time_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_ms",
                self.get_connection_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_lock_ms",
                self.get_connection_lock_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_hit_ms",
                self.get_connection_hit_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_connection_miss_ms",
                self.get_connection_miss_ms.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "make_connection_ms",
                self.total_client_stats
                    .make_connection_ms
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "total_connections",
                self.total_client_stats
                    .total_connections
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "connection_reuse",
                self.total_client_stats
                    .connection_reuse
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "connection_errors",
                self.total_client_stats
                    .connection_errors
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "zero_rtt_accepts",
                self.total_client_stats
                    .zero_rtt_accepts
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "zero_rtt_rejects",
                self.total_client_stats
                    .zero_rtt_rejects
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "congestion_events",
                self.total_client_stats.congestion_events.load_and_reset(),
                i64
            ),
            (
                "tx_streams_blocked_uni",
                self.total_client_stats
                    .tx_streams_blocked_uni
                    .load_and_reset(),
                i64
            ),
            (
                "tx_data_blocked",
                self.total_client_stats.tx_data_blocked.load_and_reset(),
                i64
            ),
            (
                "tx_acks",
                self.total_client_stats.tx_acks.load_and_reset(),
                i64
            ),
            (
                "num_packets",
                self.sent_packets.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "total_batches",
                self.total_batches.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "batch_failure",
                self.batch_failure.swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

pub struct ConnectionCache {
    map: RwLock<IndexMap<SocketAddr, ConnectionPool>>,
    stats: Arc<ConnectionCacheStats>,
    last_stats: AtomicInterval,
    connection_pool_size: usize,
    tpu_udp_socket: Option<Arc<UdpSocket>>,
}

/// Models the pool of connections
struct ConnectionPool {
    /// The connections in the pool
    connections: Vec<Arc<BaseTpuConnection>>,

    /// Connections in this pool share the same endpoint
    endpoint: Option<Arc<QuicLazyInitializedEndpoint>>,
}

impl ConnectionPool {
    /// Get a connection from the pool. It must have at least one connection in the pool.
    /// This randomly picks a connection in the pool.
    fn borrow_connection(&self) -> Arc<BaseTpuConnection> {
        let mut rng = thread_rng();
        let n = rng.gen_range(0, self.connections.len());
        self.connections[n].clone()
    }

    /// Check if we need to create a new connection. If the count of the connections
    /// is smaller than the pool size.
    fn need_new_connection(&self, required_pool_size: usize) -> bool {
        self.connections.len() < required_pool_size
    }
}

impl ConnectionCache {
    pub fn new(connection_pool_size: usize) -> Self {
        // The minimum pool size is 1.
        let connection_pool_size = 1.max(connection_pool_size);
        Self {
            tpu_udp_socket: None,
            connection_pool_size,
            ..Self::default()
        }
    }

    pub fn with_udp(connection_pool_size: usize) -> Self {
        // The minimum pool size is 1.
        let connection_pool_size = 1.max(connection_pool_size);
        Self {
            connection_pool_size,
            ..Self::default()
        }
    }

    pub fn use_quic(&self) -> bool {
        matches!(self.tpu_udp_socket, None)
    }

    fn create_endpoint(&self) -> Option<Arc<QuicLazyInitializedEndpoint>> {
        if self.use_quic() {
            Some(Arc::new(QuicLazyInitializedEndpoint::new()))
        } else {
            None
        }
    }

    /// Create a lazy connection object under the exclusive lock of the cache map if there is not
    /// enough unsed connections in the connection pool for the specified address.
    /// Returns CreateConnectionResult.
    fn create_connection(
        &self,
        lock_timing_ms: &mut u64,
        addr: &SocketAddr,
    ) -> CreateConnectionResult {
        let mut get_connection_map_lock_measure = Measure::start("get_connection_map_lock_measure");
        let mut map = self.map.write().unwrap();
        get_connection_map_lock_measure.stop();
        *lock_timing_ms = lock_timing_ms.saturating_add(get_connection_map_lock_measure.as_ms());
        // Read again, as it is possible that between read lock dropped and the write lock acquired
        // another thread could have setup the connection.

        let (to_create_connection, endpoint) =
            map.get(addr)
                .map_or((true, self.create_endpoint()), |pool| {
                    (
                        pool.need_new_connection(self.connection_pool_size),
                        pool.endpoint.clone(),
                    )
                });

        let (cache_hit, num_evictions, eviction_timing_ms) = if to_create_connection {
            let connection = match &self.tpu_udp_socket {
                Some(socket) => BaseTpuConnection::Udp(socket.clone()),
                None => BaseTpuConnection::Quic(Arc::new(QuicClient::new(
                    endpoint.as_ref().unwrap().clone(),
                    *addr,
                ))),
            };

            let connection = Arc::new(connection);

            // evict a connection if the cache is reaching upper bounds
            let mut num_evictions = 0;
            let mut get_connection_cache_eviction_measure =
                Measure::start("get_connection_cache_eviction_measure");
            while map.len() >= MAX_CONNECTIONS {
                let mut rng = thread_rng();
                let n = rng.gen_range(0, MAX_CONNECTIONS);
                map.swap_remove_index(n);
                num_evictions += 1;
            }
            get_connection_cache_eviction_measure.stop();

            match map.entry(*addr) {
                Entry::Occupied(mut entry) => {
                    let pool = entry.get_mut();
                    pool.connections.push(connection);
                }
                Entry::Vacant(entry) => {
                    entry.insert(ConnectionPool {
                        connections: vec![connection],
                        endpoint,
                    });
                }
            }
            (
                false,
                num_evictions,
                get_connection_cache_eviction_measure.as_ms(),
            )
        } else {
            (true, 0, 0)
        };

        let pool = map.get(addr).unwrap();
        let connection = pool.borrow_connection();

        CreateConnectionResult {
            connection,
            cache_hit,
            connection_cache_stats: self.stats.clone(),
            num_evictions,
            eviction_timing_ms,
        }
    }

    fn get_or_add_connection(&self, addr: &SocketAddr) -> GetConnectionResult {
        let mut get_connection_map_lock_measure = Measure::start("get_connection_map_lock_measure");
        let map = self.map.read().unwrap();
        get_connection_map_lock_measure.stop();

        let port_offset = if self.use_quic() { QUIC_PORT_OFFSET } else { 0 };

        let addr = SocketAddr::new(addr.ip(), addr.port() + port_offset);

        let mut lock_timing_ms = get_connection_map_lock_measure.as_ms();

        let report_stats = self
            .last_stats
            .should_update(CONNECTION_STAT_SUBMISSION_INTERVAL);

        let mut get_connection_map_measure = Measure::start("get_connection_hit_measure");
        let CreateConnectionResult {
            connection,
            cache_hit,
            connection_cache_stats,
            num_evictions,
            eviction_timing_ms,
        } = match map.get(&addr) {
            Some(pool) => {
                if pool.need_new_connection(self.connection_pool_size) {
                    // create more connection and put it in the pool
                    drop(map);
                    self.create_connection(&mut lock_timing_ms, &addr)
                } else {
                    let connection = pool.borrow_connection();
                    CreateConnectionResult {
                        connection,
                        cache_hit: true,
                        connection_cache_stats: self.stats.clone(),
                        num_evictions: 0,
                        eviction_timing_ms: 0,
                    }
                }
            }
            None => {
                // Upgrade to write access by dropping read lock and acquire write lock
                drop(map);
                self.create_connection(&mut lock_timing_ms, &addr)
            }
        };
        get_connection_map_measure.stop();

        GetConnectionResult {
            connection,
            cache_hit,
            report_stats,
            map_timing_ms: get_connection_map_measure.as_ms(),
            lock_timing_ms,
            connection_cache_stats,
            num_evictions,
            eviction_timing_ms,
        }
    }

    fn get_connection_and_log_stats(
        &self,
        addr: &SocketAddr,
    ) -> (Arc<BaseTpuConnection>, Arc<ConnectionCacheStats>) {
        let mut get_connection_measure = Measure::start("get_connection_measure");
        let GetConnectionResult {
            connection,
            cache_hit,
            report_stats,
            map_timing_ms,
            lock_timing_ms,
            connection_cache_stats,
            num_evictions,
            eviction_timing_ms,
        } = self.get_or_add_connection(addr);

        if report_stats {
            connection_cache_stats.report();
        }

        if cache_hit {
            connection_cache_stats
                .cache_hits
                .fetch_add(1, Ordering::Relaxed);
            connection_cache_stats
                .get_connection_hit_ms
                .fetch_add(map_timing_ms, Ordering::Relaxed);
        } else {
            connection_cache_stats
                .cache_misses
                .fetch_add(1, Ordering::Relaxed);
            connection_cache_stats
                .get_connection_miss_ms
                .fetch_add(map_timing_ms, Ordering::Relaxed);
            connection_cache_stats
                .cache_evictions
                .fetch_add(num_evictions, Ordering::Relaxed);
            connection_cache_stats
                .eviction_time_ms
                .fetch_add(eviction_timing_ms, Ordering::Relaxed);
        }

        get_connection_measure.stop();
        connection_cache_stats
            .get_connection_lock_ms
            .fetch_add(lock_timing_ms, Ordering::Relaxed);
        connection_cache_stats
            .get_connection_ms
            .fetch_add(get_connection_measure.as_ms(), Ordering::Relaxed);

        (connection, connection_cache_stats)
    }

    pub fn get_connection(&self, addr: &SocketAddr) -> BlockingConnection {
        let (connection, connection_cache_stats) = self.get_connection_and_log_stats(addr);
        connection.new_blocking_connection(*addr, connection_cache_stats)
    }

    pub fn get_nonblocking_connection(&self, addr: &SocketAddr) -> NonblockingConnection {
        let (connection, connection_cache_stats) = self.get_connection_and_log_stats(addr);
        connection.new_nonblocking_connection(*addr, connection_cache_stats)
    }
}

impl Default for ConnectionCache {
    fn default() -> Self {
        Self {
            map: RwLock::new(IndexMap::with_capacity(MAX_CONNECTIONS)),
            stats: Arc::new(ConnectionCacheStats::default()),
            last_stats: AtomicInterval::default(),
            connection_pool_size: DEFAULT_TPU_CONNECTION_POOL_SIZE,
            tpu_udp_socket: (!DEFAULT_TPU_USE_QUIC).then(|| {
                Arc::new(
                    solana_net_utils::bind_with_any_port(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))
                        .expect("Unable to bind to UDP socket"),
                )
            }),
        }
    }
}

enum BaseTpuConnection {
    Udp(Arc<UdpSocket>),
    Quic(Arc<QuicClient>),
}
impl BaseTpuConnection {
    fn new_blocking_connection(
        &self,
        addr: SocketAddr,
        stats: Arc<ConnectionCacheStats>,
    ) -> BlockingConnection {
        use crate::{quic_client::QuicTpuConnection, udp_client::UdpTpuConnection};
        match self {
            BaseTpuConnection::Udp(udp_socket) => {
                UdpTpuConnection::new_from_addr(udp_socket.clone(), addr).into()
            }
            BaseTpuConnection::Quic(quic_client) => {
                QuicTpuConnection::new_with_client(quic_client.clone(), stats).into()
            }
        }
    }

    fn new_nonblocking_connection(
        &self,
        addr: SocketAddr,
        stats: Arc<ConnectionCacheStats>,
    ) -> NonblockingConnection {
        use crate::nonblocking::{quic_client::QuicTpuConnection, udp_client::UdpTpuConnection};
        match self {
            BaseTpuConnection::Udp(udp_socket) => {
                UdpTpuConnection::new_from_addr(udp_socket.try_clone().unwrap(), addr).into()
            }
            BaseTpuConnection::Quic(quic_client) => {
                QuicTpuConnection::new_with_client(quic_client.clone(), stats).into()
            }
        }
    }
}

struct GetConnectionResult {
    connection: Arc<BaseTpuConnection>,
    cache_hit: bool,
    report_stats: bool,
    map_timing_ms: u64,
    lock_timing_ms: u64,
    connection_cache_stats: Arc<ConnectionCacheStats>,
    num_evictions: u64,
    eviction_timing_ms: u64,
}

struct CreateConnectionResult {
    connection: Arc<BaseTpuConnection>,
    cache_hit: bool,
    connection_cache_stats: Arc<ConnectionCacheStats>,
    num_evictions: u64,
    eviction_timing_ms: u64,
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            connection_cache::{ConnectionCache, MAX_CONNECTIONS},
            tpu_connection::TpuConnection,
        },
        rand::{Rng, SeedableRng},
        rand_chacha::ChaChaRng,
        std::net::SocketAddr,
    };

    fn get_addr(rng: &mut ChaChaRng) -> SocketAddr {
        let a = rng.gen_range(1, 255);
        let b = rng.gen_range(1, 255);
        let c = rng.gen_range(1, 255);
        let d = rng.gen_range(1, 255);

        let addr_str = format!("{}.{}.{}.{}:80", a, b, c, d);

        addr_str.parse().expect("Invalid address")
    }

    #[test]
    fn test_connection_cache() {
        solana_logger::setup();
        // Allow the test to run deterministically
        // with the same pseudorandom sequence between runs
        // and on different platforms - the cryptographic security
        // property isn't important here but ChaChaRng provides a way
        // to get the same pseudorandom sequence on different platforms
        let mut rng = ChaChaRng::seed_from_u64(42);

        // Generate a bunch of random addresses and create TPUConnections to them
        // Since TPUConnection::new is infallible, it should't matter whether or not
        // we can actually connect to those addresses - TPUConnection implementations should either
        // be lazy and not connect until first use or handle connection errors somehow
        // (without crashing, as would be required in a real practical validator)
        let connection_cache = ConnectionCache::default();
        let addrs = (0..MAX_CONNECTIONS)
            .into_iter()
            .map(|_| {
                let addr = get_addr(&mut rng);
                connection_cache.get_connection(&addr);
                addr
            })
            .collect::<Vec<_>>();
        {
            let map = connection_cache.map.read().unwrap();
            assert!(map.len() == MAX_CONNECTIONS);
            addrs.iter().for_each(|a| {
                let conn = &map.get(a).expect("Address not found").connections[0];
                let conn = conn.new_blocking_connection(*a, connection_cache.stats.clone());
                assert!(a.ip() == conn.tpu_addr().ip());
            });
        }

        let addr = get_addr(&mut rng);
        connection_cache.get_connection(&addr);

        let map = connection_cache.map.read().unwrap();
        assert!(map.len() == MAX_CONNECTIONS);
        let _conn = map.get(&addr).expect("Address not found");
    }
}
