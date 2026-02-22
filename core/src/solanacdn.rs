use std::collections::{hash_map::DefaultHasher, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Once;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use ed25519_dalek_v2::SigningKey;
use quinn::Endpoint;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch};

use solana_keypair::Keypair;
use solana_ledger::shred::ShredId as LedgerShredId;
use solana_packet::PACKET_DATA_SIZE;
use solana_pubkey::Pubkey;
use solana_sha256_hasher as sha256_hasher;

use solanacdn_protocol::crypto::{random_nonce_16, PubkeyBytes};
use solanacdn_protocol::frame::{
    decode_envelope, encode_envelope, FrameError, DEFAULT_MAX_FRAME_BYTES,
};
use solanacdn_protocol::messages::{
    AgentToPop, AuthRefresh, AuthRequest, AuthRequestPayload, AuthWithSessionToken,
    ControlRequest, ControlResponse, Heartbeat, HeartbeatStats, PopToAgent, Shred, ShredBatch,
    ShredId, ShredKind, StreamKind, VoteDatagram,
};

static GLOBAL: ArcSwapOption<SolanaCdnHandle> = ArcSwapOption::const_empty();

// FNV-1a 128-bit for stable, dependency-free IDs (helps dedupe mirrored packets).
const FNV1A_128_OFFSET_BASIS: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV1A_128_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

const TX_DEDUP_TTL_MS: u64 = 2_000;
const TX_SIG_DEDUP_MAX_ENTRIES: usize = 300_000;
const DEFAULT_VOTE_DEDUP_TTL_MS: u64 = 2_000;
const DEFAULT_VOTE_DEDUP_MAX_ENTRIES: usize = 200_000;
const VOTE_TUNNEL_ALLOWED_DST_TTL_MS: u64 = 60_000;
const VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES: usize = 4_096;

const POP_EGRESS_IP_TTL_MS: u64 = 10 * 60_000;
const POP_EGRESS_IP_MAX_ENTRIES: usize = 50_000;
const PIPE_API_VERIFY_MAX_POP_ENDPOINTS: usize = 32;

// Tighten framing limits beyond the protocol default (16MiB). These are chosen to be generous for
// expected message sizes while capping per-frame allocations if a POP misbehaves.
const CTRL_MAX_FRAME_BYTES: usize = 1 * 1024 * 1024;
const SHREDS_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const VOTES_MAX_FRAME_BYTES: usize = 256 * 1024;

// Bound CPU/memory for decoding large multi-shred batches.
const PUSH_SHRED_BATCH_MAX_SHREDS: usize = 4_096;

const SCDN_PROTOCOL_VIOLATION_CLOSE_CODE: u32 = 1;

fn try_first_signature_bytes_from_wire_tx(payload: &[u8]) -> Option<[u8; 64]> {
    let (sig_count, consumed) = parse_shortvec_len(payload)?;
    if sig_count == 0 {
        return None;
    }
    let start = consumed;
    let end = start.checked_add(64)?;
    if payload.len() < end {
        return None;
    }
    let mut sig = [0u8; 64];
    sig.copy_from_slice(&payload[start..end]);
    Some(sig)
}

fn parse_shortvec_len(input: &[u8]) -> Option<(usize, usize)> {
    // Solana shortvec: 7-bit groups, MSB is continuation.
    let mut value: usize = 0;
    let mut shift: u32 = 0;
    for (idx, byte) in input.iter().copied().take(3).enumerate() {
        value |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value, idx + 1));
        }
        shift = shift.saturating_add(7);
    }
    None
}

fn wire_tx_has_program_id(payload: &[u8], program_id: &[u8; 32]) -> bool {
    // Wire tx format:
    // - signatures: shortvec len + 64-byte signatures
    // - message (legacy) OR versioned message (v0) with version prefix byte.
    let (sig_count, consumed) = match parse_shortvec_len(payload) {
        Some(v) => v,
        None => return false,
    };
    if sig_count == 0 {
        return false;
    }
    let sig_bytes = match sig_count.checked_mul(64) {
        Some(v) => v,
        None => return false,
    };
    let msg_start = match consumed.checked_add(sig_bytes) {
        Some(v) => v,
        None => return false,
    };
    let msg = match payload.get(msg_start..) {
        Some(v) => v,
        None => return false,
    };
    if msg.is_empty() {
        return false;
    }

    // Message header: legacy starts immediately; v0 starts with 0x80|version.
    let mut cursor: usize = 0;
    if msg[0] & 0x80 != 0 {
        let version = msg[0] & 0x7f;
        if version != 0 {
            return false;
        }
        cursor = 1;
    }

    cursor = match cursor.checked_add(3) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    let (key_count, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
        Some(v) => v,
        None => return false,
    };
    cursor = match cursor.checked_add(consumed) {
        Some(v) => v,
        None => return false,
    };

    let keys_start = cursor;
    let keys_bytes = match key_count.checked_mul(32) {
        Some(v) => v,
        None => return false,
    };
    cursor = match keys_start.checked_add(keys_bytes) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    // recent_blockhash
    cursor = match cursor.checked_add(32) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    let (ix_count, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
        Some(v) => v,
        None => return false,
    };
    cursor = match cursor.checked_add(consumed) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    for _ in 0..ix_count {
        let program_id_index = match msg.get(cursor) {
            Some(v) => *v as usize,
            None => return false,
        };
        cursor = match cursor.checked_add(1) {
            Some(v) => v,
            None => return false,
        };

        let (account_count, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(consumed) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(account_count) {
            Some(v) => v,
            None => return false,
        };
        if cursor > msg.len() {
            return false;
        }

        let (data_len, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(consumed) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(data_len) {
            Some(v) => v,
            None => return false,
        };
        if cursor > msg.len() {
            return false;
        }

        if program_id_index < key_count {
            let start = match keys_start.checked_add(program_id_index.saturating_mul(32)) {
                Some(v) => v,
                None => return false,
            };
            let end = match start.checked_add(32) {
                Some(v) => v,
                None => return false,
            };
            let key_bytes = match msg.get(start..end) {
                Some(v) => v,
                None => return false,
            };
            if key_bytes == program_id.as_slice() {
                return true;
            }
        }
    }

    false
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataPlaneMode {
    Off,
    Auto,
    Always,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PopPubkeyPinningMode {
    Off,
    Warn,
    Enforce,
}

impl Default for PopPubkeyPinningMode {
    fn default() -> Self {
        Self::Warn
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TvuShredIngestMode {
    /// Always ingest turbine TVU shreds from any peer (normal validator behavior).
    All,
    /// When connected to SolanaCDN, only ingest shreds sourced from POPs or local reinjection.
    /// When disconnected, fall back to normal P2P ingest.
    SolanaCdnOnly,
    /// Prefer SolanaCDN shreds when healthy, but allow P2P turbine shreds if SolanaCDN is
    /// connected-but-stalled.
    SolanaCdnPreferred,
}

impl Default for TvuShredIngestMode {
    fn default() -> Self {
        Self::All
    }
}

#[derive(Clone, Debug)]
pub struct SolanaCdnConfig {
    pub pop_endpoints: Vec<SocketAddr>,

    pub control_endpoint: Option<SocketAddr>,
    pub control_server_name: String,
    pub control_tls_insecure_skip_verify: bool,
    pub control_tls_ca_cert_path: Option<PathBuf>,
    pub control_refresh_ms: u64,

    pub server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_ca_cert_path: Option<PathBuf>,
    /// If Pipe-managed discovery returns a POP pubkey for a given endpoint, optionally enforce that
    /// the connected POP presents the expected pubkey during auth.
    pub pop_pubkey_pinning: PopPubkeyPinningMode,

    pub udp_mode: DataPlaneMode,

    pub pipe_api_base_url: String,
    pub pipe_api_token: Option<String>,
    pub pipe_api_timeout_ms: u64,
    pub pipe_api_tls_insecure_skip_verify: bool,
    pub pipe_api_tls_ca_cert_path: Option<PathBuf>,
    pub pipe_api_tls_bootstrap: bool,

    /// Optional Prometheus/HTTP status listener for SolanaCDN integration.
    pub metrics_listen_addr: Option<SocketAddr>,

    /// If enabled, measure “race” outcomes between SolanaCDN-delivered shreds and gossip shreds.
    /// This requires ingesting shreds from both paths (do not use `--solanacdn-only`/hybrid).
    pub race_enabled: bool,
    /// Deterministic sampling: track 1/(2^sample_bits) shreds. 0 means “track all” (not
    /// recommended).
    pub race_sample_bits: u8,
    /// Max age (ms) to wait for the other source before dropping a sampled shred from the race
    /// tracker.
    pub race_window_ms: u64,

    pub publish_shreds: bool,
    pub publish_discarded_shreds: bool,
    pub subscribe_shreds: bool,
    pub inject_shreds: bool,
    /// If disabled, drop repair shreds (risk: can stall if shreds are missing).
    pub repair_shreds: bool,
    /// How to gate turbine TVU shreds when SolanaCDN is enabled.
    pub tvu_shred_ingest_mode: TvuShredIngestMode,
    /// In `SolanaCdnPreferred` mode, treat SolanaCDN as stalled when no POP-delivered shreds have
    /// been observed for this many milliseconds.
    pub tvu_shred_hybrid_stale_ms: u64,
    pub direct_shreds_from_pop: bool,
    pub vote_tunnel: bool,
    pub vote_dedup_ttl_ms: u64,
    pub vote_dedup_max_entries: usize,

    pub shreds_queue_len: usize,
    pub votes_queue_len: usize,
}

impl SolanaCdnConfig {
    pub fn new(pop_addr: SocketAddr) -> Self {
        Self {
            pop_endpoints: vec![pop_addr],
            control_endpoint: None,
            control_server_name: "solanacdn-control".to_string(),
            control_tls_insecure_skip_verify: false,
            control_tls_ca_cert_path: None,
            control_refresh_ms: 1_000,
            server_name: "solanacdn-pop".to_string(),
            tls_insecure_skip_verify: false,
            tls_ca_cert_path: None,
            pop_pubkey_pinning: PopPubkeyPinningMode::Warn,
            udp_mode: DataPlaneMode::Auto,
            pipe_api_base_url: "https://api.pipedev.network".to_string(),
            pipe_api_token: None,
            pipe_api_timeout_ms: 2_000,
            pipe_api_tls_insecure_skip_verify: false,
            pipe_api_tls_ca_cert_path: None,
            pipe_api_tls_bootstrap: false,
            metrics_listen_addr: None,
            race_enabled: true,
            race_sample_bits: 12,
            race_window_ms: 5_000,
            publish_shreds: true,
            publish_discarded_shreds: true,
            subscribe_shreds: true,
            inject_shreds: true,
            repair_shreds: true,
            tvu_shred_ingest_mode: TvuShredIngestMode::All,
            tvu_shred_hybrid_stale_ms: 2_000,
            direct_shreds_from_pop: true,
            vote_tunnel: true,
            vote_dedup_ttl_ms: DEFAULT_VOTE_DEDUP_TTL_MS,
            vote_dedup_max_entries: DEFAULT_VOTE_DEDUP_MAX_ENTRIES,
            shreds_queue_len: 8192,
            votes_queue_len: 1024,
        }
    }
}

impl Default for SolanaCdnConfig {
    fn default() -> Self {
        Self {
            pop_endpoints: Vec::new(),
            control_endpoint: None,
            control_server_name: "solanacdn-control".to_string(),
            control_tls_insecure_skip_verify: false,
            control_tls_ca_cert_path: None,
            control_refresh_ms: 1_000,
            server_name: "solanacdn-pop".to_string(),
            tls_insecure_skip_verify: false,
            tls_ca_cert_path: None,
            pop_pubkey_pinning: PopPubkeyPinningMode::Warn,
            udp_mode: DataPlaneMode::Auto,
            pipe_api_base_url: "https://api.pipedev.network".to_string(),
            pipe_api_token: None,
            pipe_api_timeout_ms: 2_000,
            pipe_api_tls_insecure_skip_verify: false,
            pipe_api_tls_ca_cert_path: None,
            pipe_api_tls_bootstrap: false,
            metrics_listen_addr: None,
            race_enabled: true,
            race_sample_bits: 12,
            race_window_ms: 5_000,
            publish_shreds: true,
            publish_discarded_shreds: true,
            subscribe_shreds: true,
            inject_shreds: true,
            repair_shreds: true,
            tvu_shred_ingest_mode: TvuShredIngestMode::All,
            tvu_shred_hybrid_stale_ms: 2_000,
            direct_shreds_from_pop: true,
            vote_tunnel: true,
            vote_dedup_ttl_ms: DEFAULT_VOTE_DEDUP_TTL_MS,
            vote_dedup_max_entries: DEFAULT_VOTE_DEDUP_MAX_ENTRIES,
            shreds_queue_len: 8192,
            votes_queue_len: 1024,
        }
    }
}

#[derive(Clone, Debug)]
struct ShredPublish {
    kind: ShredKind,
    payload: Bytes,
}

#[derive(Clone, Debug)]
struct VotePublish {
    dst: SocketAddr,
    payload: Bytes,
}

#[derive(Debug)]
struct VoteInjectSockets {
    v4: UdpSocket,
    v6: Option<UdpSocket>,
}

impl VoteInjectSockets {
    async fn bind() -> std::io::Result<Self> {
        let v4 = UdpSocket::bind("0.0.0.0:0").await?;
        let v6 = match std::net::UdpSocket::bind("[::]:0") {
            Ok(sock) => {
                if let Err(e) = sock.set_nonblocking(true) {
                    debug!("solanacdn: failed to set nonblocking IPv6 vote socket: {e}");
                    None
                } else {
                    match UdpSocket::from_std(sock) {
                        Ok(sock) => Some(sock),
                        Err(e) => {
                            debug!("solanacdn: failed to wrap IPv6 vote socket: {e}");
                            None
                        }
                    }
                }
            }
            Err(e) => {
                debug!("solanacdn: failed to bind IPv6 vote socket: {e}");
                None
            }
        };
        Ok(Self { v4, v6 })
    }

    async fn send_to(&self, payload: &[u8], dst: SocketAddr) -> std::io::Result<usize> {
        match dst {
            SocketAddr::V4(_) => self.v4.send_to(payload, dst).await,
            SocketAddr::V6(_) => {
                if let Some(sock) = &self.v6 {
                    sock.send_to(payload, dst).await
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        "no IPv6 vote socket available",
                    ))
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
enum UplinkMsg {
    Shred(ShredPublish),
    Vote(VotePublish),
}

#[derive(Clone, Debug)]
struct SessionUplink {
    tx: mpsc::Sender<UplinkMsg>,
}

#[derive(Clone, Copy, Debug, Default)]
struct RateSample {
    at_ms: u64,
    rx_shred_payloads: u64,
    tunneled_vote_packets: u64,
}

#[derive(Debug, Default)]
struct RateState {
    last: Option<RateSample>,
    rx_shred_payloads_per_sec: f64,
    tunneled_vote_packets_per_sec: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RaceSource {
    SolanaCdn,
    Gossip,
}

impl RaceSource {
    fn as_str(&self) -> &'static str {
        match self {
            Self::SolanaCdn => "solanacdn",
            Self::Gossip => "gossip",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RaceEntry {
    solanacdn_first_ms: Option<u64>,
    solanacdn_endpoint: Option<SocketAddr>,
    gossip_first_ms: Option<u64>,
    gossip_src_ip: Option<IpAddr>,
    first_seen_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct RaceSample {
    gossip_src_ip: IpAddr,
    delta_ms: i64,
}

const RACE_LEAD_BUCKETS_MS: [u64; 12] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000];
const RACE_DELTA_BUCKETS_MS: [i64; 25] = [
    -5_000, -2_000, -1_000, -500, -200, -100, -50, -20, -10, -5, -2, -1, 0, 1, 2, 5, 10, 20, 50,
    100, 200, 500, 1_000, 2_000, 5_000,
];

#[derive(Clone, Debug)]
struct RaceHistogramSnapshot {
    lead_bucket_counts_by_winner: [(RaceSource, [u64; RACE_LEAD_BUCKETS_MS.len()]); 2],
    lead_sum_ms_by_winner: [(RaceSource, u64); 2],
    lead_count_by_winner: [(RaceSource, u64); 2],
    delta_bucket_counts: [u64; RACE_DELTA_BUCKETS_MS.len()],
    delta_sum_ms: i64,
    delta_count: u64,
    delta_by_pop_endpoint: Vec<(SocketAddr, RaceDeltaHistogramSnapshot)>,
    delta_by_hour_utc: [RaceDeltaHistogramSnapshot; 24],
}

#[derive(Clone, Debug, Default)]
struct RaceMetricsSnapshot {
    enabled: bool,
    sample_bits: u8,
    window_ms: u64,
    inflight: usize,
    pairs_total: u64,
    wins_solanacdn_total: u64,
    wins_gossip_total: u64,
    ties_total: u64,
    last_winner: Option<RaceSource>,
    last_lead_ms: Option<u64>,
    last_shred_slot: Option<u64>,
    histogram: Option<RaceHistogramSnapshot>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct RaceDeltaHistogramSnapshot {
    bucket_counts: [u64; RACE_DELTA_BUCKETS_MS.len()],
    sum_ms: i64,
    count: u64,
}

#[derive(Clone, Debug)]
struct RaceHistogram {
    solanacdn_bucket_counts: [u64; RACE_LEAD_BUCKETS_MS.len()],
    gossip_bucket_counts: [u64; RACE_LEAD_BUCKETS_MS.len()],
    solanacdn_sum_ms: u64,
    gossip_sum_ms: u64,
    solanacdn_count: u64,
    gossip_count: u64,
    delta: RaceDeltaHistogram,
    delta_by_pop_endpoint: HashMap<SocketAddr, RaceDeltaHistogram>,
    delta_by_hour_utc: [RaceDeltaHistogram; 24],
}

impl RaceHistogram {
    fn new() -> Self {
        let delta_by_hour_utc = std::array::from_fn(|_| RaceDeltaHistogram::new());
        Self {
            solanacdn_bucket_counts: [0u64; RACE_LEAD_BUCKETS_MS.len()],
            gossip_bucket_counts: [0u64; RACE_LEAD_BUCKETS_MS.len()],
            solanacdn_sum_ms: 0,
            gossip_sum_ms: 0,
            solanacdn_count: 0,
            gossip_count: 0,
            delta: RaceDeltaHistogram::new(),
            delta_by_pop_endpoint: HashMap::new(),
            delta_by_hour_utc,
        }
    }

    fn observe(&mut self, winner: RaceSource, lead_ms: u64) {
        let (buckets, sum_ms, count) = match winner {
            RaceSource::SolanaCdn => (
                &mut self.solanacdn_bucket_counts,
                &mut self.solanacdn_sum_ms,
                &mut self.solanacdn_count,
            ),
            RaceSource::Gossip => (
                &mut self.gossip_bucket_counts,
                &mut self.gossip_sum_ms,
                &mut self.gossip_count,
            ),
        };
        *sum_ms = sum_ms.saturating_add(lead_ms);
        *count = count.saturating_add(1);
        for (i, bound_ms) in RACE_LEAD_BUCKETS_MS.iter().enumerate() {
            if lead_ms <= *bound_ms {
                buckets[i] = buckets[i].saturating_add(1);
            }
        }
    }

    fn observe_delta(
        &mut self,
        delta_ms: i64,
        solanacdn_endpoint: Option<SocketAddr>,
        event_ms: u64,
    ) {
        self.delta.observe(delta_ms);

        let hour_utc = ((event_ms / 1000) / 3600) % 24;
        if let Some(hist) = self.delta_by_hour_utc.get_mut(hour_utc as usize) {
            hist.observe(delta_ms);
        }

        const MAX_POP_SEGMENTS: usize = 64;
        if let Some(endpoint) = solanacdn_endpoint {
            if self.delta_by_pop_endpoint.len() < MAX_POP_SEGMENTS
                || self.delta_by_pop_endpoint.contains_key(&endpoint)
            {
                self.delta_by_pop_endpoint
                    .entry(endpoint)
                    .or_insert_with(RaceDeltaHistogram::new)
                    .observe(delta_ms);
            }
        }
    }

    fn snapshot(&self) -> RaceHistogramSnapshot {
        let mut delta_by_pop_endpoint: Vec<(SocketAddr, RaceDeltaHistogramSnapshot)> = self
            .delta_by_pop_endpoint
            .iter()
            .map(|(ep, hist)| (*ep, hist.snapshot()))
            .collect();
        delta_by_pop_endpoint.sort_by_key(|(ep, _)| *ep);

        RaceHistogramSnapshot {
            lead_bucket_counts_by_winner: [
                (RaceSource::SolanaCdn, self.solanacdn_bucket_counts),
                (RaceSource::Gossip, self.gossip_bucket_counts),
            ],
            lead_sum_ms_by_winner: [
                (RaceSource::SolanaCdn, self.solanacdn_sum_ms),
                (RaceSource::Gossip, self.gossip_sum_ms),
            ],
            lead_count_by_winner: [
                (RaceSource::SolanaCdn, self.solanacdn_count),
                (RaceSource::Gossip, self.gossip_count),
            ],
            delta_bucket_counts: self.delta.bucket_counts,
            delta_sum_ms: self.delta.sum_ms,
            delta_count: self.delta.count,
            delta_by_pop_endpoint,
            delta_by_hour_utc: std::array::from_fn(|i| self.delta_by_hour_utc[i].snapshot()),
        }
    }
}

#[derive(Clone, Debug)]
struct RaceDeltaHistogram {
    bucket_counts: [u64; RACE_DELTA_BUCKETS_MS.len()],
    sum_ms: i64,
    count: u64,
}

impl RaceDeltaHistogram {
    fn new() -> Self {
        Self {
            bucket_counts: [0u64; RACE_DELTA_BUCKETS_MS.len()],
            sum_ms: 0,
            count: 0,
        }
    }

    fn observe(&mut self, delta_ms: i64) {
        self.sum_ms = self.sum_ms.saturating_add(delta_ms);
        self.count = self.count.saturating_add(1);
        for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
            if delta_ms <= *bound_ms {
                self.bucket_counts[i] = self.bucket_counts[i].saturating_add(1);
            }
        }
    }

    fn snapshot(&self) -> RaceDeltaHistogramSnapshot {
        RaceDeltaHistogramSnapshot {
            bucket_counts: self.bucket_counts,
            sum_ms: self.sum_ms,
            count: self.count,
        }
    }
}

#[derive(Clone, Debug)]
struct RaceTracker {
    inflight: HashMap<LedgerShredId, RaceEntry>,
    last_cleanup_ms: u64,
    pairs_total: u64,
    wins_solanacdn_total: u64,
    wins_gossip_total: u64,
    ties_total: u64,
    last_winner: Option<RaceSource>,
    last_lead_ms: Option<u64>,
    last_shred_slot: Option<u64>,
    histogram: RaceHistogram,
    samples: VecDeque<RaceSample>,
}

impl RaceTracker {
    fn new() -> Self {
        Self {
            inflight: HashMap::new(),
            last_cleanup_ms: 0,
            pairs_total: 0,
            wins_solanacdn_total: 0,
            wins_gossip_total: 0,
            ties_total: 0,
            last_winner: None,
            last_lead_ms: None,
            last_shred_slot: None,
            histogram: RaceHistogram::new(),
            samples: VecDeque::new(),
        }
    }

    fn push_sample(&mut self, sample: RaceSample) {
        const MAX_SAMPLES: usize = 4096;
        if self.samples.len() >= MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    fn peek_samples(&self, max: usize) -> Vec<RaceSample> {
        self.samples.iter().take(max).copied().collect()
    }

    fn consume_samples(&mut self, n: usize) {
        for _ in 0..n.min(self.samples.len()) {
            self.samples.pop_front();
        }
    }

    fn cleanup(&mut self, now_ms: u64, window_ms: u64) {
        if self.inflight.is_empty() {
            self.last_cleanup_ms = now_ms;
            return;
        }
        if now_ms.saturating_sub(self.last_cleanup_ms) < 1_000 {
            return;
        }
        self.last_cleanup_ms = now_ms;
        let expire_before = now_ms.saturating_sub(window_ms.max(250));
        self.inflight
            .retain(|_, entry| entry.first_seen_ms >= expire_before);
    }

    fn observe(
        &mut self,
        shred_id: LedgerShredId,
        source: RaceSource,
        now_ms: u64,
        window_ms: u64,
        pop_endpoint: Option<SocketAddr>,
        gossip_src_ip: Option<IpAddr>,
    ) {
        self.cleanup(now_ms, window_ms);

        const RACE_MAX_INFLIGHT: usize = 100_000;
        if !self.inflight.contains_key(&shred_id) && self.inflight.len() >= RACE_MAX_INFLIGHT {
            return;
        }

        let (solanacdn_ms, gossip_ms, solanacdn_endpoint, observed_gossip_src_ip) = {
            let entry = self.inflight.entry(shred_id).or_insert(RaceEntry {
                solanacdn_first_ms: None,
                solanacdn_endpoint: None,
                gossip_first_ms: None,
                gossip_src_ip: None,
                first_seen_ms: now_ms,
            });
            entry.first_seen_ms = entry.first_seen_ms.min(now_ms);
            match source {
                RaceSource::SolanaCdn => {
                    if entry.solanacdn_first_ms.is_none()
                        || entry.solanacdn_first_ms.is_some_and(|t| now_ms < t)
                    {
                        entry.solanacdn_first_ms = Some(now_ms);
                        if pop_endpoint.is_some() {
                            entry.solanacdn_endpoint = pop_endpoint;
                        }
                    } else if entry.solanacdn_endpoint.is_none() && pop_endpoint.is_some() {
                        entry.solanacdn_endpoint = pop_endpoint;
                    }
                }
                RaceSource::Gossip => {
                    if entry.gossip_first_ms.is_none()
                        || entry.gossip_first_ms.is_some_and(|t| now_ms < t)
                    {
                        entry.gossip_first_ms = Some(now_ms);
                        if gossip_src_ip.is_some() {
                            entry.gossip_src_ip = gossip_src_ip;
                        }
                    } else if entry.gossip_src_ip.is_none() && gossip_src_ip.is_some() {
                        entry.gossip_src_ip = gossip_src_ip;
                    }
                }
            }
            (
                entry.solanacdn_first_ms,
                entry.gossip_first_ms,
                entry.solanacdn_endpoint,
                entry.gossip_src_ip,
            )
        };

        let (Some(solanacdn_ms), Some(gossip_ms)) = (solanacdn_ms, gossip_ms) else {
            return;
        };

        self.pairs_total = self.pairs_total.saturating_add(1);
        self.inflight.remove(&shred_id);

        let event_ms = solanacdn_ms.min(gossip_ms);
        let delta_ms: i64 = (solanacdn_ms as i64).saturating_sub(gossip_ms as i64);
        if let Some(ip) = observed_gossip_src_ip {
            self.push_sample(RaceSample {
                gossip_src_ip: ip,
                delta_ms,
            });
        }
        let winner = if delta_ms < 0 {
            Some(RaceSource::SolanaCdn)
        } else if delta_ms > 0 {
            Some(RaceSource::Gossip)
        } else {
            self.ties_total = self.ties_total.saturating_add(1);
            self.last_winner = None;
            self.last_lead_ms = Some(0);
            self.last_shred_slot = Some(shred_id.slot());

            self.histogram
                .observe_delta(delta_ms, solanacdn_endpoint, event_ms);
            return;
        };

        let (winner, lead_ms) = match winner.expect("delta nonzero implies winner") {
            RaceSource::SolanaCdn => {
                self.wins_solanacdn_total = self.wins_solanacdn_total.saturating_add(1);
                (RaceSource::SolanaCdn, delta_ms.unsigned_abs())
            }
            RaceSource::Gossip => {
                self.wins_gossip_total = self.wins_gossip_total.saturating_add(1);
                (RaceSource::Gossip, delta_ms.unsigned_abs())
            }
        };
        self.last_winner = Some(winner);
        self.last_lead_ms = Some(lead_ms);
        self.last_shred_slot = Some(shred_id.slot());
        self.histogram.observe(winner, lead_ms);
        self.histogram
            .observe_delta(delta_ms, solanacdn_endpoint, event_ms);
    }

    fn snapshot(&self, cfg: &SolanaCdnConfig) -> RaceMetricsSnapshot {
        let histogram = cfg.race_enabled.then(|| self.histogram.snapshot());
        RaceMetricsSnapshot {
            enabled: cfg.race_enabled,
            sample_bits: cfg.race_sample_bits,
            window_ms: cfg.race_window_ms,
            inflight: self.inflight.len(),
            pairs_total: self.pairs_total,
            wins_solanacdn_total: self.wins_solanacdn_total,
            wins_gossip_total: self.wins_gossip_total,
            ties_total: self.ties_total,
            last_winner: self.last_winner,
            last_lead_ms: self.last_lead_ms,
            last_shred_slot: self.last_shred_slot.map(|v| v as u64),
            histogram,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaCdnStatus {
    pub connected: bool,
    pub publisher: Option<String>,
    pub connected_pops: Vec<String>,
    pub publisher_switches_total: u64,
    pub tx_deduped_packets_total: u64,
    pub rx_shred_bytes_total: u64,
    pub rx_shred_payloads_total: u64,
    pub dropped_shred_batches_oversized_total: u64,
    pub rx_shred_payloads_per_sec: f64,
    pub tunneled_vote_packets_total: u64,
    pub tunneled_vote_packets_per_sec: f64,
    pub rx_vote_packets_total: u64,
    pub dropped_vote_datagrams_total: u64,
    pub dropped_vote_datagrams_oversized_payload_total: u64,
    pub dropped_vote_datagrams_invalid_payload_total: u64,
    pub dropped_vote_datagrams_unexpected_dst_total: u64,
    pub dropped_quic_shreds_unexpected_msg_total: u64,
    pub dropped_quic_votes_unexpected_msg_total: u64,
    pub dropped_udp_shreds_unexpected_peer_total: u64,
    pub dropped_udp_shreds_unexpected_msg_total: u64,
    pub dropped_udp_votes_unexpected_peer_total: u64,
    pub dropped_udp_votes_unexpected_msg_total: u64,
    pub vote_tunnel_allowed_dsts_len: u64,
    pub last_shred_slot: Option<u64>,
    pub last_shred_timestamp_ms: Option<u64>,
    pub last_shred_age_ms: Option<u64>,
    pub last_accepted_shred_slot: Option<u64>,
    pub last_accepted_shred_timestamp_ms: Option<u64>,
    pub last_accepted_shred_age_ms: Option<u64>,
    pub tvu_shred_ingest_mode: TvuShredIngestMode,
    pub tvu_shred_stale: Option<bool>,
    pub tvu_shred_stale_for_ms: Option<u64>,
    pub race_enabled: bool,
    pub race_sample_bits: u8,
    pub race_window_ms: u64,
    pub race_inflight: u64,
    pub race_pairs_total: u64,
    pub race_wins_solanacdn_total: u64,
    pub race_wins_gossip_total: u64,
    pub race_ties_total: u64,
    pub race_last_winner: Option<String>,
    pub race_last_lead_ms: Option<u64>,
    pub race_last_shred_slot: Option<u64>,
}

#[derive(Debug)]
pub struct SolanaCdnHandle {
    cfg: SolanaCdnConfig,
    connected: AtomicBool,
    published_shred_batches: AtomicU64,
    pushed_shred_batches: AtomicU64,
    rx_shred_bytes: AtomicU64,
    rx_shred_payloads: AtomicU64,
    last_solanacdn_shred_rx_ms: AtomicU64,
    last_solanacdn_shred_slot: AtomicU64,
    last_solanacdn_shred_slot_valid: AtomicBool,
    last_solanacdn_shred_accepted_ms: AtomicU64,
    last_solanacdn_shred_accepted_slot: AtomicU64,
    last_solanacdn_shred_accepted_slot_valid: AtomicBool,
    tunneled_vote_packets: AtomicU64,
    rx_vote_packets: AtomicU64,
    rx_tx_packets: AtomicU64,
    tx_injected_packets: AtomicU64,
    tx_deduped_packets: AtomicU64,
    tx_inject_failed: AtomicU64,
    dropped_shred_payloads: AtomicU64,
    dropped_shred_batches_oversized: AtomicU64,
    dropped_vote_datagrams: AtomicU64,
    dropped_vote_datagrams_oversized_payload: AtomicU64,
    dropped_vote_datagrams_invalid_payload: AtomicU64,
    dropped_vote_datagrams_unexpected_dst: AtomicU64,
    dropped_quic_shreds_unexpected_msg: AtomicU64,
    dropped_quic_votes_unexpected_msg: AtomicU64,
    dropped_udp_shreds_unexpected_peer: AtomicU64,
    dropped_udp_shreds_unexpected_msg: AtomicU64,
    dropped_udp_votes_unexpected_peer: AtomicU64,
    dropped_udp_votes_unexpected_msg: AtomicU64,
    uplink_broadcast_lagged: AtomicU64,
    pop_endpoint_ips: DashSet<IpAddr>,
    pop_egress_ips: DashMap<IpAddr, u64>,
    pipe_pop_expected_pubkeys: DashMap<SocketAddr, PubkeyBytes>,
    connected_pops: DashSet<SocketAddr>,
    publisher_endpoint: ArcSwapOption<String>,
    publisher_switches_total: AtomicU64,
    heartbeat_schema_version: AtomicU32,
    publisher_uplink: ArcSwapOption<SessionUplink>,
    rate_state: std::sync::Mutex<RateState>,
    race_state: std::sync::Mutex<RaceTracker>,

    recent_tx_sigs: DashMap<[u8; 64], u64>,
    recent_vote_payloads: DashMap<u128, u64>,
    vote_tunnel_allowed_dsts: DashMap<SocketAddr, u64>,
}

impl SolanaCdnHandle {
    fn new(cfg: SolanaCdnConfig) -> Self {
        Self {
            cfg,
            connected: AtomicBool::new(false),
            published_shred_batches: AtomicU64::new(0),
            pushed_shred_batches: AtomicU64::new(0),
            rx_shred_bytes: AtomicU64::new(0),
            rx_shred_payloads: AtomicU64::new(0),
            last_solanacdn_shred_rx_ms: AtomicU64::new(0),
            last_solanacdn_shred_slot: AtomicU64::new(0),
            last_solanacdn_shred_slot_valid: AtomicBool::new(false),
            last_solanacdn_shred_accepted_ms: AtomicU64::new(0),
            last_solanacdn_shred_accepted_slot: AtomicU64::new(0),
            last_solanacdn_shred_accepted_slot_valid: AtomicBool::new(false),
            tunneled_vote_packets: AtomicU64::new(0),
            rx_vote_packets: AtomicU64::new(0),
            rx_tx_packets: AtomicU64::new(0),
            tx_injected_packets: AtomicU64::new(0),
            tx_deduped_packets: AtomicU64::new(0),
            tx_inject_failed: AtomicU64::new(0),
            dropped_shred_payloads: AtomicU64::new(0),
            dropped_shred_batches_oversized: AtomicU64::new(0),
            dropped_vote_datagrams: AtomicU64::new(0),
            dropped_vote_datagrams_oversized_payload: AtomicU64::new(0),
            dropped_vote_datagrams_invalid_payload: AtomicU64::new(0),
            dropped_vote_datagrams_unexpected_dst: AtomicU64::new(0),
            dropped_quic_shreds_unexpected_msg: AtomicU64::new(0),
            dropped_quic_votes_unexpected_msg: AtomicU64::new(0),
            dropped_udp_shreds_unexpected_peer: AtomicU64::new(0),
            dropped_udp_shreds_unexpected_msg: AtomicU64::new(0),
            dropped_udp_votes_unexpected_peer: AtomicU64::new(0),
            dropped_udp_votes_unexpected_msg: AtomicU64::new(0),
            uplink_broadcast_lagged: AtomicU64::new(0),
            pop_endpoint_ips: DashSet::new(),
            pop_egress_ips: DashMap::new(),
            pipe_pop_expected_pubkeys: DashMap::new(),
            connected_pops: DashSet::new(),
            publisher_endpoint: ArcSwapOption::const_empty(),
            publisher_switches_total: AtomicU64::new(0),
            heartbeat_schema_version: AtomicU32::new(0),
            publisher_uplink: ArcSwapOption::const_empty(),
            rate_state: std::sync::Mutex::new(RateState::default()),
            race_state: std::sync::Mutex::new(RaceTracker::new()),
            recent_tx_sigs: DashMap::new(),
            recent_vote_payloads: DashMap::new(),
            vote_tunnel_allowed_dsts: DashMap::new(),
        }
    }

    fn should_dedup_tx_sig(&self, sig: [u8; 64], now: u64) -> bool {
        self.prune_recent_tx_sigs_if_needed(now);
        if let Some(entry) = self.recent_tx_sigs.get(&sig) {
            let expired = *entry < now;
            drop(entry);
            if !expired {
                return true;
            }
            self.recent_tx_sigs.remove(&sig);
        }
        false
    }

    fn note_dedup_tx_sig(&self, sig: [u8; 64], now: u64) {
        self.prune_recent_tx_sigs_if_needed(now);
        self.recent_tx_sigs
            .insert(sig, now.saturating_add(TX_DEDUP_TTL_MS));
    }

    fn prune_recent_tx_sigs_if_needed(&self, now: u64) {
        if self.recent_tx_sigs.len() <= TX_SIG_DEDUP_MAX_ENTRIES {
            return;
        }
        let mut expired: Vec<[u8; 64]> = Vec::new();
        for entry in self.recent_tx_sigs.iter().take(4096) {
            if *entry.value() < now {
                expired.push(*entry.key());
            }
        }
        for key in expired {
            self.recent_tx_sigs.remove(&key);
        }
        if self.recent_tx_sigs.len() > TX_SIG_DEDUP_MAX_ENTRIES {
            self.recent_tx_sigs.clear();
        }
    }

    fn should_dedup_vote_payload(&self, dst: SocketAddr, payload: &[u8], now: u64) -> bool {
        let ttl_ms = self.cfg.vote_dedup_ttl_ms;
        let max_entries = self.cfg.vote_dedup_max_entries;
        if ttl_ms == 0 || max_entries == 0 {
            return false;
        }
        self.prune_recent_vote_payloads_if_needed(now, max_entries);
        let key = vote_dedup_key(&dst, payload);
        if let Some(entry) = self.recent_vote_payloads.get(&key) {
            let expired = *entry < now;
            drop(entry);
            if !expired {
                return true;
            }
            self.recent_vote_payloads.remove(&key);
        }
        self.recent_vote_payloads
            .insert(key, now.saturating_add(ttl_ms));
        false
    }

    fn prune_recent_vote_payloads_if_needed(&self, now: u64, max_entries: usize) {
        if self.recent_vote_payloads.len() <= max_entries {
            return;
        }
        let mut expired: Vec<u128> = Vec::new();
        for entry in self.recent_vote_payloads.iter().take(4096) {
            if *entry.value() < now {
                expired.push(*entry.key());
            }
        }
        for key in expired {
            self.recent_vote_payloads.remove(&key);
        }
        if self.recent_vote_payloads.len() > max_entries {
            self.recent_vote_payloads.clear();
        }
    }

    fn note_vote_tunnel_allowed_dst(&self, dst: SocketAddr, now: u64) {
        if !self.cfg.vote_tunnel {
            return;
        }
        if VOTE_TUNNEL_ALLOWED_DST_TTL_MS == 0 || VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES == 0 {
            return;
        }
        self.prune_vote_tunnel_allowed_dsts_if_needed(now);
        self.vote_tunnel_allowed_dsts.insert(
            dst,
            now.saturating_add(VOTE_TUNNEL_ALLOWED_DST_TTL_MS),
        );
    }

    fn prune_vote_tunnel_allowed_dsts_if_needed(&self, now: u64) {
        if self.vote_tunnel_allowed_dsts.len() <= VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES {
            return;
        }
        let mut expired: Vec<SocketAddr> = Vec::new();
        for entry in self.vote_tunnel_allowed_dsts.iter().take(1024) {
            if *entry.value() < now {
                expired.push(*entry.key());
            }
        }
        for key in expired {
            self.vote_tunnel_allowed_dsts.remove(&key);
        }
        if self.vote_tunnel_allowed_dsts.len() > VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES {
            self.vote_tunnel_allowed_dsts.clear();
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub fn publish_shreds_enabled(&self) -> bool {
        self.cfg.publish_shreds
    }

    pub fn publish_discarded_shreds_enabled(&self) -> bool {
        self.cfg.publish_discarded_shreds
    }

    pub fn subscribe_shreds_enabled(&self) -> bool {
        self.cfg.subscribe_shreds
    }

    /// Count an incoming shred delivered from a POP (direct injection path).
    ///
    /// When POPs are configured for `direct_shreds_from_pop`, they send raw shreds directly to
    /// the validator's TVU socket, bypassing the `PopToAgent::PushShredBatch` stream. This helper
    /// lets the validator pipeline increment "Pushed" counters so operators can confirm traffic
    /// is flowing even in fully-direct mode.
    pub fn note_pop_delivered_shred(&self, bytes: usize) {
        self.note_pop_delivered_shred_with_slot(bytes, None);
    }

    pub fn note_pop_delivered_shred_with_slot(&self, bytes: usize, slot: Option<u64>) {
        self.pushed_shred_batches.fetch_add(1, Ordering::Relaxed);
        self.note_solanacdn_shreds_rx(bytes, 1, slot);
    }

    pub fn inject_shreds_enabled(&self) -> bool {
        self.cfg.inject_shreds
    }

    pub fn direct_shreds_from_pop_enabled(&self) -> bool {
        self.cfg.direct_shreds_from_pop
    }

    pub fn vote_tunnel_enabled(&self) -> bool {
        self.cfg.vote_tunnel
    }

    pub fn repair_shreds_enabled(&self) -> bool {
        self.cfg.repair_shreds
    }

    pub fn tvu_shred_ingest_mode(&self) -> TvuShredIngestMode {
        self.cfg.tvu_shred_ingest_mode
    }

    pub fn race_enabled(&self) -> bool {
        self.cfg.race_enabled && self.is_connected()
    }

    pub fn note_race_observation(&self, shred_id: LedgerShredId, src_ip: IpAddr) {
        if !self.cfg.race_enabled {
            return;
        }
        if !self.is_connected() {
            return;
        }
        let sample_bits = self.cfg.race_sample_bits.min(31);
        if sample_bits != 0 {
            let mut hash = FNV1A_128_OFFSET_BASIS;
            hash = fnv1a_128_update(hash, &shred_id.slot().to_le_bytes());
            hash = fnv1a_128_update(hash, &u8::from(shred_id.shred_type()).to_le_bytes());
            hash = fnv1a_128_update(hash, &shred_id.index().to_le_bytes());
            let h64 = (hash as u64) ^ ((hash >> 64) as u64);
            let mask = (1u64 << sample_bits).saturating_sub(1);
            if (h64 & mask) != 0 {
                return;
            }
        }

        let source = if self.should_ignore_src_ip(src_ip) {
            RaceSource::SolanaCdn
        } else {
            RaceSource::Gossip
        };

        let pop_endpoint = (source == RaceSource::SolanaCdn)
            .then(|| self.race_pop_endpoint_hint(src_ip))
            .flatten();
        let gossip_src_ip = (source == RaceSource::Gossip).then_some(src_ip);
        let mut tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.observe(
            shred_id,
            source,
            now_ms(),
            self.cfg.race_window_ms,
            pop_endpoint,
            gossip_src_ip,
        );
    }

    pub fn note_race_observation_from_pop(
        &self,
        shred_id: LedgerShredId,
        pop_endpoint: SocketAddr,
    ) {
        if !self.cfg.race_enabled {
            return;
        }
        if !self.is_connected() {
            return;
        }
        let sample_bits = self.cfg.race_sample_bits.min(31);
        if sample_bits != 0 {
            let mut hash = FNV1A_128_OFFSET_BASIS;
            hash = fnv1a_128_update(hash, &shred_id.slot().to_le_bytes());
            hash = fnv1a_128_update(hash, &u8::from(shred_id.shred_type()).to_le_bytes());
            hash = fnv1a_128_update(hash, &shred_id.index().to_le_bytes());
            let h64 = (hash as u64) ^ ((hash >> 64) as u64);
            let mask = (1u64 << sample_bits).saturating_sub(1);
            if (h64 & mask) != 0 {
                return;
            }
        }

        let mut tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.observe(
            shred_id,
            RaceSource::SolanaCdn,
            now_ms(),
            self.cfg.race_window_ms,
            Some(pop_endpoint),
            None,
        );
    }

    fn race_pop_endpoint_hint(&self, src_ip: IpAddr) -> Option<SocketAddr> {
        if src_ip.is_loopback() {
            if let Some(publisher) = self.publisher_endpoint.load_full() {
                if let Ok(ep) = publisher.parse::<SocketAddr>() {
                    return Some(ep);
                }
            }
            return None;
        }

        // Direct injection uses the POP IP as the packet source and may not include the QUIC port,
        // so best-effort map it back to a connected endpoint.
        for ep in self.connected_pops.iter() {
            if ep.ip() == src_ip {
                return Some(*ep);
            }
        }
        None
    }

    /// Returns true if this shred should be ingested by the validator pipeline.
    /// In `solanacdn-only` mode, this gates turbine shreds to SolanaCDN sources while connected,
    /// and falls back to the normal P2P path when disconnected. In `solanacdn-preferred` mode,
    /// it falls back to P2P when SolanaCDN looks stalled.
    pub fn should_ingest_tvu_shred(&self, src_ip: IpAddr) -> bool {
        match self.cfg.tvu_shred_ingest_mode {
            TvuShredIngestMode::All => return true,
            TvuShredIngestMode::SolanaCdnOnly => {}
            TvuShredIngestMode::SolanaCdnPreferred => {}
        }
        if !self.is_connected() {
            return true;
        }
        if self.cfg.tvu_shred_ingest_mode == TvuShredIngestMode::SolanaCdnPreferred
            && !self.is_solanacdn_shred_accepted_fresh_at(now_ms())
        {
            return true;
        }
        self.should_ignore_src_ip(src_ip)
    }

    #[allow(dead_code)]
    fn is_solanacdn_shred_rx_fresh_at(&self, now_ms: u64) -> bool {
        let last = self.last_solanacdn_shred_rx_ms.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        now_ms.saturating_sub(last) <= self.cfg.tvu_shred_hybrid_stale_ms.max(250)
    }

    fn is_solanacdn_shred_accepted_fresh_at(&self, now_ms: u64) -> bool {
        let last = self
            .last_solanacdn_shred_accepted_ms
            .load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        now_ms.saturating_sub(last) <= self.cfg.tvu_shred_hybrid_stale_ms.max(250)
    }

    pub fn udp_mode(&self) -> DataPlaneMode {
        self.cfg.udp_mode
    }

    fn is_pop_egress_ip_fresh(&self, ip: IpAddr, now: u64) -> bool {
        if let Some(entry) = self.pop_egress_ips.get(&ip) {
            let expires_at = *entry;
            drop(entry);
            if expires_at >= now {
                return true;
            }
            self.pop_egress_ips.remove(&ip);
        }
        false
    }

    pub fn should_ignore_src_ip(&self, ip: IpAddr) -> bool {
        if ip.is_loopback() {
            return true;
        }
        let now = now_ms();
        let mut egress_ok = false;
        if let Some(entry) = self.pop_egress_ips.get(&ip) {
            let expires_at = *entry;
            drop(entry);
            if expires_at >= now {
                egress_ok = true;
            } else {
                self.pop_egress_ips.remove(&ip);
            }
        }
        if self.is_connected() {
            let mut has_connected = false;
            for ep in self.connected_pops.iter() {
                has_connected = true;
                if ep.ip() == ip {
                    return true;
                }
            }
            if has_connected {
                return egress_ok;
            }
            // Fallback when connected_pops is not yet populated (startup/tests).
            if self.pop_endpoint_ips.contains(&ip) {
                return true;
            }
            return egress_ok;
        }
        if self.pop_endpoint_ips.contains(&ip) {
            return true;
        }
        egress_ok
    }

    /// Replace the POP endpoint allowlist with the provided set (used for discovery refreshes).
    pub fn note_pop_endpoints(&self, endpoints: &[SocketAddr]) {
        self.pop_endpoint_ips.clear();
        for ep in endpoints {
            self.pop_endpoint_ips.insert(ep.ip());
        }
    }

    fn note_pipe_pop_expected_pubkeys(&self, expected: &HashMap<SocketAddr, PubkeyBytes>) {
        self.pipe_pop_expected_pubkeys.clear();
        for (ep, pk) in expected {
            self.pipe_pop_expected_pubkeys.insert(*ep, *pk);
        }
    }

    fn expected_pipe_pop_pubkey(&self, endpoint: SocketAddr) -> Option<PubkeyBytes> {
        self.pipe_pop_expected_pubkeys.get(&endpoint).map(|v| *v)
    }

    pub fn note_pop_endpoint(&self, endpoint: SocketAddr) {
        self.pop_endpoint_ips.insert(endpoint.ip());
    }

    pub fn note_pop_egress_ip(&self, ip: IpAddr) {
        if ip.is_loopback() {
            return;
        }

        let now = now_ms();
        let expires_at = now.saturating_add(POP_EGRESS_IP_TTL_MS);

        if let Some(mut entry) = self.pop_egress_ips.get_mut(&ip) {
            *entry = expires_at;
            return;
        }

        // Bound memory: best-effort prune of expired entries when full. If still full, skip adding
        // new keys (but always allow TTL refreshes for existing keys above).
        if self.pop_egress_ips.len() >= POP_EGRESS_IP_MAX_ENTRIES {
            let mut expired: Vec<IpAddr> = Vec::new();
            for entry in self.pop_egress_ips.iter().take(1024) {
                if *entry.value() < now {
                    expired.push(*entry.key());
                }
            }
            for key in expired {
                self.pop_egress_ips.remove(&key);
            }
        }

        if self.pop_egress_ips.len() >= POP_EGRESS_IP_MAX_ENTRIES {
            return;
        }

        self.pop_egress_ips.insert(ip, expires_at);
    }

    fn heartbeat_stats(&self) -> HeartbeatStats {
        HeartbeatStats {
            published_shred_batches: self.published_shred_batches.load(Ordering::Relaxed),
            pushed_shred_batches: self.pushed_shred_batches.load(Ordering::Relaxed),
            tunneled_vote_packets: self.tunneled_vote_packets.load(Ordering::Relaxed),
            rx_vote_packets: self.rx_vote_packets.load(Ordering::Relaxed),
        }
    }

    fn set_publisher_uplink(
        &self,
        publisher: Option<SocketAddr>,
        uplink: Option<Arc<SessionUplink>>,
    ) {
        self.publisher_uplink.store(uplink);
        self.connected.store(
            self.publisher_uplink.load_full().is_some(),
            Ordering::Relaxed,
        );

        let publisher_str = publisher.map(|p| p.to_string());
        let current = self.publisher_endpoint.load_full().map(|p| (*p).clone());
        if current != publisher_str {
            self.publisher_switches_total
                .fetch_add(1, Ordering::Relaxed);
            self.publisher_endpoint.store(publisher_str.map(Arc::new));
        }
    }

    fn note_solanacdn_shreds_rx(&self, bytes: usize, shreds: u64, slot: Option<u64>) {
        if bytes == 0 || shreds == 0 {
            return;
        }
        self.rx_shred_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.rx_shred_payloads.fetch_add(shreds, Ordering::Relaxed);
        self.last_solanacdn_shred_rx_ms
            .store(now_ms(), Ordering::Relaxed);

        if let Some(slot) = slot {
            self.last_solanacdn_shred_slot_valid
                .store(true, Ordering::Relaxed);
            let mut current = self.last_solanacdn_shred_slot.load(Ordering::Relaxed);
            while slot > current {
                match self.last_solanacdn_shred_slot.compare_exchange(
                    current,
                    slot,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(next) => current = next,
                }
            }
        }
    }

    pub(crate) fn note_solanacdn_accepted_shred_with_slot(&self, slot: Option<u64>) {
        self.last_solanacdn_shred_accepted_ms
            .store(now_ms(), Ordering::Relaxed);

        if let Some(slot) = slot {
            self.last_solanacdn_shred_accepted_slot_valid
                .store(true, Ordering::Relaxed);
            let mut current = self
                .last_solanacdn_shred_accepted_slot
                .load(Ordering::Relaxed);
            while slot > current {
                match self.last_solanacdn_shred_accepted_slot.compare_exchange(
                    current,
                    slot,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(next) => current = next,
                }
            }
        }
    }

    pub fn status_snapshot(&self) -> SolanaCdnStatus {
        let now = now_ms();
        let publisher = self.publisher_endpoint.load_full().map(|p| (*p).clone());
        let publisher_switches_total = self.publisher_switches_total.load(Ordering::Relaxed);
        let tx_deduped_packets_total = self.tx_deduped_packets.load(Ordering::Relaxed);

        let mut pops: Vec<String> = self.connected_pops.iter().map(|e| e.to_string()).collect();
        pops.sort();

        let rx_shred_bytes_total = self.rx_shred_bytes.load(Ordering::Relaxed);
        let rx_shred_payloads_total = self.rx_shred_payloads.load(Ordering::Relaxed);
        let dropped_shred_batches_oversized_total =
            self.dropped_shred_batches_oversized.load(Ordering::Relaxed);
        let tunneled_vote_packets_total = self.tunneled_vote_packets.load(Ordering::Relaxed);
        let rx_vote_packets_total = self.rx_vote_packets.load(Ordering::Relaxed);
        let dropped_vote_datagrams_total = self.dropped_vote_datagrams.load(Ordering::Relaxed);
        let dropped_vote_datagrams_oversized_payload_total = self
            .dropped_vote_datagrams_oversized_payload
            .load(Ordering::Relaxed);
        let dropped_vote_datagrams_invalid_payload_total = self
            .dropped_vote_datagrams_invalid_payload
            .load(Ordering::Relaxed);
        let dropped_vote_datagrams_unexpected_dst_total = self
            .dropped_vote_datagrams_unexpected_dst
            .load(Ordering::Relaxed);
        let dropped_quic_shreds_unexpected_msg_total = self
            .dropped_quic_shreds_unexpected_msg
            .load(Ordering::Relaxed);
        let dropped_quic_votes_unexpected_msg_total = self
            .dropped_quic_votes_unexpected_msg
            .load(Ordering::Relaxed);
        let dropped_udp_shreds_unexpected_peer_total = self
            .dropped_udp_shreds_unexpected_peer
            .load(Ordering::Relaxed);
        let dropped_udp_shreds_unexpected_msg_total = self
            .dropped_udp_shreds_unexpected_msg
            .load(Ordering::Relaxed);
        let dropped_udp_votes_unexpected_peer_total = self
            .dropped_udp_votes_unexpected_peer
            .load(Ordering::Relaxed);
        let dropped_udp_votes_unexpected_msg_total = self
            .dropped_udp_votes_unexpected_msg
            .load(Ordering::Relaxed);
        let vote_tunnel_allowed_dsts_len = self.vote_tunnel_allowed_dsts.len() as u64;

        let (rx_shred_payloads_per_sec, tunneled_vote_packets_per_sec) = {
            let mut state = match self.rate_state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(last) = state.last {
                let dt_ms = now.saturating_sub(last.at_ms);
                if dt_ms >= 250 {
                    let dt = (dt_ms as f64) / 1000.0;
                    state.rx_shred_payloads_per_sec =
                        (rx_shred_payloads_total.saturating_sub(last.rx_shred_payloads) as f64)
                            / dt;
                    state.tunneled_vote_packets_per_sec = (tunneled_vote_packets_total
                        .saturating_sub(last.tunneled_vote_packets)
                        as f64)
                        / dt;
                    state.last = Some(RateSample {
                        at_ms: now,
                        rx_shred_payloads: rx_shred_payloads_total,
                        tunneled_vote_packets: tunneled_vote_packets_total,
                    });
                }
            } else {
                state.last = Some(RateSample {
                    at_ms: now,
                    rx_shred_payloads: rx_shred_payloads_total,
                    tunneled_vote_packets: tunneled_vote_packets_total,
                });
                state.rx_shred_payloads_per_sec = 0.0;
                state.tunneled_vote_packets_per_sec = 0.0;
            }
            (
                state.rx_shred_payloads_per_sec,
                state.tunneled_vote_packets_per_sec,
            )
        };

        let last_shred_timestamp_ms = {
            let v = self.last_solanacdn_shred_rx_ms.load(Ordering::Relaxed);
            (v != 0).then_some(v)
        };
        let last_shred_age_ms = last_shred_timestamp_ms.map(|v| now.saturating_sub(v));

        let last_shred_slot = self
            .last_solanacdn_shred_slot_valid
            .load(Ordering::Relaxed)
            .then_some(self.last_solanacdn_shred_slot.load(Ordering::Relaxed));

        let last_accepted_shred_timestamp_ms = {
            let v = self
                .last_solanacdn_shred_accepted_ms
                .load(Ordering::Relaxed);
            (v != 0).then_some(v)
        };
        let last_accepted_shred_age_ms =
            last_accepted_shred_timestamp_ms.map(|v| now.saturating_sub(v));

        let last_accepted_shred_slot = self
            .last_solanacdn_shred_accepted_slot_valid
            .load(Ordering::Relaxed)
            .then_some(
                self.last_solanacdn_shred_accepted_slot
                    .load(Ordering::Relaxed),
            );

        let tvu_shred_stale_for_ms = last_accepted_shred_age_ms;
        let tvu_shred_stale = match self.cfg.tvu_shred_ingest_mode {
            TvuShredIngestMode::SolanaCdnPreferred => {
                Some(!self.is_solanacdn_shred_accepted_fresh_at(now))
            }
            _ => None,
        };

        let race = {
            let tracker = match self.race_state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            tracker.snapshot(&self.cfg)
        };

        SolanaCdnStatus {
            connected: self.is_connected(),
            publisher,
            connected_pops: pops,
            publisher_switches_total,
            tx_deduped_packets_total,
            rx_shred_bytes_total,
            rx_shred_payloads_total,
            dropped_shred_batches_oversized_total,
            rx_shred_payloads_per_sec,
            tunneled_vote_packets_total,
            tunneled_vote_packets_per_sec,
            rx_vote_packets_total,
            dropped_vote_datagrams_total,
            dropped_vote_datagrams_oversized_payload_total,
            dropped_vote_datagrams_invalid_payload_total,
            dropped_vote_datagrams_unexpected_dst_total,
            dropped_quic_shreds_unexpected_msg_total,
            dropped_quic_votes_unexpected_msg_total,
            dropped_udp_shreds_unexpected_peer_total,
            dropped_udp_shreds_unexpected_msg_total,
            dropped_udp_votes_unexpected_peer_total,
            dropped_udp_votes_unexpected_msg_total,
            vote_tunnel_allowed_dsts_len,
            last_shred_slot,
            last_shred_timestamp_ms,
            last_shred_age_ms,
            last_accepted_shred_slot,
            last_accepted_shred_timestamp_ms,
            last_accepted_shred_age_ms,
            tvu_shred_ingest_mode: self.cfg.tvu_shred_ingest_mode,
            tvu_shred_stale,
            tvu_shred_stale_for_ms,
            race_enabled: race.enabled,
            race_sample_bits: race.sample_bits,
            race_window_ms: race.window_ms,
            race_inflight: race.inflight as u64,
            race_pairs_total: race.pairs_total,
            race_wins_solanacdn_total: race.wins_solanacdn_total,
            race_wins_gossip_total: race.wins_gossip_total,
            race_ties_total: race.ties_total,
            race_last_winner: race.last_winner.map(|w| w.as_str().to_string()),
            race_last_lead_ms: race.last_lead_ms,
            race_last_shred_slot: race.last_shred_slot,
        }
    }

    pub fn try_publish_tvu_shred(&self, src_ip: IpAddr, payload: Bytes, discarded: bool) {
        if !self.cfg.publish_shreds {
            return;
        }
        if discarded && !self.cfg.publish_discarded_shreds {
            return;
        }
        if payload.is_empty() {
            return;
        }
        if self.should_ignore_src_ip(src_ip) {
            return;
        }
        self.try_publish_uplink_shred_payload(payload);
    }

    pub fn try_publish_local_tvu_shred(&self, payload: Bytes) {
        if !self.cfg.publish_shreds {
            return;
        }
        if payload.is_empty() {
            return;
        }
        self.try_publish_uplink_shred_payload(payload);
    }

    fn try_publish_uplink_shred_payload(&self, payload: Bytes) {
        let Some(uplink) = self.publisher_uplink.load_full() else {
            self.dropped_shred_payloads.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if uplink
            .tx
            .try_send(UplinkMsg::Shred(ShredPublish {
                kind: ShredKind::Tvu,
                payload,
            }))
            .is_err()
        {
            self.dropped_shred_payloads.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn try_publish_vote_datagram(&self, dst: SocketAddr, payload: Bytes) -> bool {
        if !self.cfg.vote_tunnel {
            return false;
        }
        if payload.is_empty() {
            return false;
        }
        self.note_vote_tunnel_allowed_dst(dst, now_ms());
        let Some(uplink) = self.publisher_uplink.load_full() else {
            return false;
        };
        match uplink
            .tx
            .try_send(UplinkMsg::Vote(VotePublish { dst, payload }))
        {
            Ok(()) => true,
            Err(_) => {
                self.dropped_vote_datagrams.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    fn note_connected_pop(&self, endpoint: SocketAddr) {
        self.connected_pops.insert(endpoint);
    }

    fn note_disconnected_pop(&self, endpoint: SocketAddr) {
        self.connected_pops.remove(&endpoint);
    }

    fn update_heartbeat_schema_version(&self, schema_version: u32) {
        self.heartbeat_schema_version
            .store(schema_version, Ordering::Relaxed);
    }

    fn ingest_runtime_snapshot(&self) -> serde_json::Value {
        let publisher = self.publisher_endpoint.load_full().map(|p| (*p).clone());
        let publisher_switches_total = self.publisher_switches_total.load(Ordering::Relaxed);

        let mut pops: Vec<String> = self.connected_pops.iter().map(|e| e.to_string()).collect();
        pops.sort();

        let tls_ca_cert_path = self
            .cfg
            .tls_ca_cert_path
            .as_ref()
            .map(|p| p.display().to_string());

        let last_shred_timestamp_ms = {
            let v = self.last_solanacdn_shred_rx_ms.load(Ordering::Relaxed);
            (v != 0).then_some(v)
        };
        let last_shred_slot = self
            .last_solanacdn_shred_slot_valid
            .load(Ordering::Relaxed)
            .then_some(self.last_solanacdn_shred_slot.load(Ordering::Relaxed));

        serde_json::json!({
            "publisher": publisher,
            "publisher_switches_total": publisher_switches_total,
            "pops": pops,
            "last_shred": {
                "slot": last_shred_slot,
                "received_at_ms": last_shred_timestamp_ms,
            },
            "tls": {
                "server_name": self.cfg.server_name.clone(),
                "ca_cert_path": tls_ca_cert_path,
                "insecure_skip_verify": self.cfg.tls_insecure_skip_verify,
            }
        })
    }

    fn ingest_counters_totals(&self) -> serde_json::Value {
        serde_json::json!({
            "published_shred_batches_total": self.published_shred_batches.load(Ordering::Relaxed) as i64,
            "pushed_shred_batches_total": self.pushed_shred_batches.load(Ordering::Relaxed) as i64,
            // Receiver-mode counters (Agave integrated): report shreds/bytes received from POPs.
            "rx_shred_batches_total": self.pushed_shred_batches.load(Ordering::Relaxed) as i64,
            "rx_shred_bytes_total": self.rx_shred_bytes.load(Ordering::Relaxed) as i64,
            "rx_shred_payloads_total": self.rx_shred_payloads.load(Ordering::Relaxed) as i64,
            "dropped_shred_batches_oversized_total": self.dropped_shred_batches_oversized.load(Ordering::Relaxed) as i64,
            "tunneled_vote_packets_total": self.tunneled_vote_packets.load(Ordering::Relaxed) as i64,
            "rx_vote_packets_total": self.rx_vote_packets.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_total": self.dropped_vote_datagrams.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_oversized_payload_total": self.dropped_vote_datagrams_oversized_payload.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_invalid_payload_total": self.dropped_vote_datagrams_invalid_payload.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_unexpected_dst_total": self.dropped_vote_datagrams_unexpected_dst.load(Ordering::Relaxed) as i64,
            "dropped_quic_shreds_unexpected_msg_total": self.dropped_quic_shreds_unexpected_msg.load(Ordering::Relaxed) as i64,
            "dropped_quic_votes_unexpected_msg_total": self.dropped_quic_votes_unexpected_msg.load(Ordering::Relaxed) as i64,
            "dropped_udp_shreds_unexpected_peer_total": self.dropped_udp_shreds_unexpected_peer.load(Ordering::Relaxed) as i64,
            "dropped_udp_shreds_unexpected_msg_total": self.dropped_udp_shreds_unexpected_msg.load(Ordering::Relaxed) as i64,
            "dropped_udp_votes_unexpected_peer_total": self.dropped_udp_votes_unexpected_peer.load(Ordering::Relaxed) as i64,
            "dropped_udp_votes_unexpected_msg_total": self.dropped_udp_votes_unexpected_msg.load(Ordering::Relaxed) as i64,
            "vote_tunnel_allowed_dsts_len": self.vote_tunnel_allowed_dsts.len() as i64,
            "rx_tx_packets_total": self.rx_tx_packets.load(Ordering::Relaxed) as i64,
            "tx_injected_packets_total": self.tx_injected_packets.load(Ordering::Relaxed) as i64,
            "tx_deduped_packets_total": self.tx_deduped_packets.load(Ordering::Relaxed) as i64,
            "tx_inject_failed_total": self.tx_inject_failed.load(Ordering::Relaxed) as i64,
            "uplink_dropped_shred_batches_total": self.dropped_shred_payloads.load(Ordering::Relaxed) as i64,
            "uplink_broadcast_lagged_total": self.uplink_broadcast_lagged.load(Ordering::Relaxed) as i64,
        })
    }

    fn pipe_ingest_race_snapshot_and_samples(
        &self,
        max_samples: usize,
    ) -> (RaceMetricsSnapshot, Vec<RaceSample>) {
        let tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        (
            tracker.snapshot(&self.cfg),
            tracker.peek_samples(max_samples.min(2048)),
        )
    }

    fn pipe_ingest_consume_race_samples(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.consume_samples(n);
    }
}

pub fn global() -> Option<Arc<SolanaCdnHandle>> {
    GLOBAL.load_full()
}

#[cfg(test)]
pub(crate) fn set_global_for_tests(handle: Option<Arc<SolanaCdnHandle>>) {
    GLOBAL.store(handle);
}

#[cfg(test)]
pub(crate) fn new_handle_for_tests(cfg: SolanaCdnConfig) -> Arc<SolanaCdnHandle> {
    Arc::new(SolanaCdnHandle::new(cfg))
}

pub fn init(
    mut cfg: SolanaCdnConfig,
    identity_keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
    vote_use_quic: bool,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
) {
    if vote_use_quic && cfg.vote_tunnel {
        warn!("solanacdn: vote tunneling requires UDP votes; disabling (vote_use_quic=true)");
        cfg.vote_tunnel = false;
    }

    if cfg.pop_endpoints.is_empty()
        && cfg.control_endpoint.is_none()
        && cfg
            .pipe_api_token
            .as_deref()
            .is_none_or(|s| s.trim().is_empty())
        && first_env(&["SOLANACDN_AGENT_API_TOKEN", "PIPE_API_KEY"]).is_none()
    {
        warn!(
            "solanacdn: no POP endpoints, control endpoint, or API token configured; skipping init"
        );
        return;
    }

    if !cfg.publish_shreds && !cfg.subscribe_shreds && !cfg.vote_tunnel {
        warn!("solanacdn: configured but all features are disabled; skipping init");
        return;
    }

    let handle = Arc::new(SolanaCdnHandle::new(cfg.clone()));
    GLOBAL.store(Some(handle.clone()));
    let _ = solana_turbine::solanacdn_hooks::set_leader_tvu_shred_publisher(Arc::new(
        AgaveLeaderTvuShredPublisher {
            handle: handle.clone(),
        },
    ));

    let thread_cfg = cfg.clone();
    std::thread::Builder::new()
        .name("solSolanaCdn".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_name("solSolanaCdnRt")
                .build()
                .expect("solanacdn runtime");
            runtime.block_on(async move {
                if let Err(e) = run(
                    thread_cfg,
                    identity_keypair,
                    exit,
                    handle,
                    inject_tpu,
                    inject_tvu,
                    inject_gossip,
                    inject_vote,
                )
                .await
                {
                    warn!("solanacdn: client exited with error: {e}");
                }
            });
        })
        .expect("spawn solanacdn thread");
}

struct AgaveLeaderTvuShredPublisher {
    handle: Arc<SolanaCdnHandle>,
}

impl solana_turbine::solanacdn_hooks::LeaderTvuShredPublisher for AgaveLeaderTvuShredPublisher {
    fn publish_tvu_shred(&self, payload: Bytes) {
        self.handle.try_publish_local_tvu_shred(payload);
    }
}

#[derive(Debug, Error)]
pub enum SolanaCdnError {
    #[error("crypto error: {0}")]
    Crypto(#[from] solanacdn_protocol::crypto::CryptoError),
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls error: {0}")]
    Tls(String),
    #[error("tls server name invalid: {0}")]
    InvalidServerName(String),
    #[error("quic connect error: {0}")]
    QuicConnect(String),
    #[error("auth failed: {0}")]
    AuthFailed(String),
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(0)
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(0)
}

fn env_trimmed(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn first_env(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = env_trimmed(key) {
            return Some(v);
        }
    }
    None
}

fn normalize_base_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_string()
}

fn read_env_file_value(path: &str, key: &str) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    for raw_line in contents.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim();
        }
        let (k, v) = line.split_once('=')?;
        if k.trim() != key {
            continue;
        }
        let mut v = v.trim().to_string();
        if (v.starts_with('"') && v.ends_with('"')) || (v.starts_with('\'') && v.ends_with('\'')) {
            v = v[1..v.len().saturating_sub(1)].to_string();
        }
        v = v.trim().to_string();
        if v.is_empty() {
            continue;
        }
        return Some(v);
    }
    None
}

fn json_error_message(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "empty response".to_string();
    }
    let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return trimmed.to_string();
    };
    val.get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| trimmed.to_string())
}

fn init_rustls() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // rustls 0.23 requires selecting a process-wide CryptoProvider when multiple are enabled.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_root_cert_store(path: &PathBuf) -> Result<RootCertStore, SolanaCdnError> {
    let mut certs = Vec::new();
    for item in CertificateDer::pem_file_iter(path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("failed to read CA certs: {e}"),
        )
    })? {
        let cert = item.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("failed to parse PEM cert: {e}"),
            )
        })?;
        certs.push(cert);
    }

    let mut roots = RootCertStore::empty();
    let (_valid, invalid) = roots.add_parsable_certificates(certs);
    if invalid > 0 {
        warn!(
            "solanacdn: CA bundle {} contained {} invalid certs",
            path.display(),
            invalid
        );
    }
    if roots.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("no valid CA certs in {}", path.display()),
        )
        .into());
    }
    Ok(roots)
}

fn load_default_root_cert_store() -> Result<RootCertStore, SolanaCdnError> {
    let mut roots = RootCertStore::empty();

    let native = rustls_native_certs::load_native_certs();
    if !native.errors.is_empty() {
        warn!(
            "solanacdn: native cert store load had {} errors (showing first): {}",
            native.errors.len(),
            native
                .errors
                .first()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );
    }

    let (valid, invalid) = roots.add_parsable_certificates(native.certs);
    if invalid > 0 {
        warn!(
            "solanacdn: native cert store contained {} invalid certs",
            invalid
        );
    }

    if roots.roots.is_empty() {
        if valid == 0 {
            warn!("solanacdn: no native root certs loaded; falling back to webpki-roots");
        }
        roots
            .roots
            .extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    if roots.roots.is_empty() {
        return Err(SolanaCdnError::Tls(
            "no trusted root certificates available for TLS verification".to_string(),
        ));
    }

    Ok(roots)
}

fn parse_hex_32(raw: &str) -> Option<[u8; 32]> {
    let raw = raw.trim();
    if raw.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    let bytes = raw.as_bytes();
    for i in 0..32 {
        let hi = bytes[2 * i];
        let lo = bytes[2 * i + 1];
        let hi = (hi as char).to_digit(16)? as u8;
        let lo = (lo as char).to_digit(16)? as u8;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    let digest = sha256_hasher::hash(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn pop_assign_score(seed: &str, key: &str) -> u64 {
    let digest = sha256_hasher::hashv(&[
        b"solanacdn_pop_assign_v1|",
        seed.as_bytes(),
        b"|",
        key.as_bytes(),
    ]);
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest.as_ref()[..8]);
    u64::from_be_bytes(head)
}

#[derive(Debug, Deserialize)]
struct PipeApiSolanaCdnTlsResponse {
    ok: bool,
    pop_ca_cert_url: String,
    #[serde(default)]
    pop_ca_cert_sha256: Option<String>,
    tls_server_name: String,
}

async fn maybe_bootstrap_pop_tls_from_pipe_api(cfg: &mut SolanaCdnConfig) {
    if cfg.tls_insecure_skip_verify || cfg.tls_ca_cert_path.is_some() {
        return;
    }

    let default_paths = ["/etc/solanacdn/tls/ca.crt", "/opt/solanacdn/tls/ca.crt"];
    for p in default_paths {
        let path = PathBuf::from(p);
        if path.exists() {
            cfg.tls_ca_cert_path = Some(path);
            return;
        }
    }

    if !cfg.pipe_api_tls_bootstrap {
        return;
    }

    let base_url = normalize_base_url(&cfg.pipe_api_base_url);
    if base_url.trim().is_empty() || !base_url.starts_with("https://") {
        return;
    }

    let client = match build_pipe_api_http_client(&PipeApiClientConfig {
        base_url: base_url.clone(),
        api_key: String::new(),
        timeout: Duration::from_millis(cfg.pipe_api_timeout_ms),
        tls_insecure_skip_verify: cfg.pipe_api_tls_insecure_skip_verify,
        tls_ca_cert_path: cfg.pipe_api_tls_ca_cert_path.clone(),
    }) {
        Ok(c) => c,
        Err(e) => {
            warn!("solanacdn: failed to init HTTP client for POP TLS bootstrap: {e}");
            return;
        }
    };

    let tls_url = format!("{}/solanacdn/tls", base_url);
    let resp = match client.get(&tls_url).send().await {
        Ok(r) => r,
        Err(e) => {
            debug!("solanacdn: POP TLS bootstrap request failed: {e}");
            return;
        }
    };
    if !resp.status().is_success() {
        debug!(
            "solanacdn: POP TLS bootstrap returned non-success status: {}",
            resp.status()
        );
        return;
    }
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            debug!("solanacdn: POP TLS bootstrap failed to read body: {e}");
            return;
        }
    };
    let parsed: PipeApiSolanaCdnTlsResponse = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            debug!("solanacdn: POP TLS bootstrap failed to parse JSON: {e}");
            return;
        }
    };
    if !parsed.ok {
        debug!("solanacdn: POP TLS bootstrap returned ok=false");
        return;
    }

    // If the operator didn't set an explicit SNI, prefer the control plane default.
    if cfg.server_name.trim().is_empty() {
        cfg.server_name = parsed.tls_server_name.clone();
    }

    let ca_url = parsed.pop_ca_cert_url.trim().to_string();
    if !ca_url.starts_with("https://") {
        debug!("solanacdn: POP TLS bootstrap CA URL is not https");
        return;
    }

    let ca_bytes = match client.get(&ca_url).send().await {
        Ok(r) => match r.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => {
                debug!("solanacdn: failed to read POP CA cert bytes: {e}");
                return;
            }
        },
        Err(e) => {
            debug!("solanacdn: failed to download POP CA cert: {e}");
            return;
        }
    };

    if ca_bytes.is_empty() {
        debug!("solanacdn: downloaded POP CA cert is empty");
        return;
    }

    const MAX_POP_CA_CERT_BYTES: usize = 1024 * 1024;
    if ca_bytes.len() > MAX_POP_CA_CERT_BYTES {
        warn!(
            "solanacdn: downloaded POP CA cert too large ({} bytes)",
            ca_bytes.len()
        );
        return;
    }

    let expected_sha_hex = parsed
        .pop_ca_cert_sha256
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(expected_sha_hex) = expected_sha_hex {
        if let Some(expected) = parse_hex_32(expected_sha_hex) {
            let got = sha256_bytes(&ca_bytes);
            if got != expected {
                warn!("solanacdn: downloaded POP CA cert sha256 mismatch; refusing to trust it");
                return;
            }
        } else {
            warn!("solanacdn: POP TLS bootstrap returned invalid pop_ca_cert_sha256; skipping sha verification");
        }
    }

    let prefix = expected_sha_hex
        .filter(|s| s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|s| format!("solanacdn-pop-ca-{}-", &s[..16]))
        .unwrap_or_else(|| "solanacdn-pop-ca-".to_string());

    let mut file = match tempfile::Builder::new()
        .prefix(&prefix)
        .suffix(".crt")
        .tempfile_in(std::env::temp_dir())
    {
        Ok(f) => f,
        Err(e) => {
            warn!("solanacdn: failed to create temp file for POP CA cert: {e}");
            return;
        }
    };
    if let Err(e) = file.as_file_mut().write_all(&ca_bytes) {
        warn!("solanacdn: failed to write POP CA cert to disk: {e}");
        return;
    }
    let _ = file.as_file_mut().sync_all();
    let path = match file.into_temp_path().keep() {
        Ok(p) => p,
        Err(e) => {
            warn!("solanacdn: failed to persist POP CA cert to disk: {e}");
            return;
        }
    };

    cfg.tls_ca_cert_path = Some(path.clone());
    info!(
        "solanacdn: bootstrapped POP TLS CA cert from Pipe control plane ({})",
        path.display()
    );
}

#[derive(Clone, Debug)]
struct PipeApiClientConfig {
    base_url: String,
    api_key: String,
    timeout: Duration,
    tls_insecure_skip_verify: bool,
    tls_ca_cert_path: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct PipeApiVerifyRequest {
    schema_version: u32,
    agent_instance_id: String,
    validator_pubkey: String,
    version: String,
    capture_mode: String,
    iface: String,
    direct_shreds_from_pop: bool,
}

#[derive(Debug, Deserialize)]
struct PipeApiVerifyResponse {
    ok: bool,
    agent_id: String,
    run_id: String,
    run_token: String,
    #[serde(default)]
    heartbeat_schema_version: Option<u32>,
    ingest: PipeApiIngestConfig,
    /// POP endpoints for the agent to connect to (provided by control plane).
    #[serde(default)]
    pop_endpoints: Vec<String>,
    /// POP endpoints with metadata (preferred; additive field).
    #[serde(default)]
    pop_endpoints_v2: Vec<PipeApiPopEndpointV2>,
}

#[derive(Clone, Debug, Deserialize)]
struct PipeApiPopEndpointV2 {
    id: String,
    quic_endpoint: String,
    #[serde(default)]
    node_pubkey: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct PipeApiIngestConfig {
    url: String,
    interval_secs: u64,
    max_body_bytes: u64,
    max_events: u64,
}

#[derive(Clone, Debug)]
struct PipeApiVerifyResult {
    agent_id: String,
    run_id: String,
    run_token: String,
    heartbeat_schema_version: u32,
    ingest: PipeApiIngestConfig,
    pop_endpoints: Vec<SocketAddr>,
    pop_expected_pubkeys: HashMap<SocketAddr, PubkeyBytes>,
}

#[derive(Debug, Deserialize)]
struct PipeApiSessionTokenResponse {
    ok: bool,
    session_token: String,
    expires_in: i64,
}

fn build_pipe_api_http_client(
    cfg: &PipeApiClientConfig,
) -> Result<reqwest::Client, SolanaCdnError> {
    let mut builder = reqwest::Client::builder()
        .timeout(cfg.timeout.max(Duration::from_millis(250)))
        .user_agent(format!(
            "agave-validator-solanacdn/{}",
            env!("CARGO_PKG_VERSION")
        ));

    if cfg.tls_insecure_skip_verify {
        builder = builder
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    } else if let Some(path) = cfg.tls_ca_cert_path.as_ref() {
        for item in CertificateDer::pem_file_iter(path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("failed to read CA certs: {e}"),
            )
        })? {
            let cert = item.map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("failed to parse PEM cert: {e}"),
                )
            })?;
            let cert = reqwest::Certificate::from_der(cert.as_ref())
                .map_err(|e| SolanaCdnError::Tls(format!("invalid CA cert: {e}")))?;
            builder = builder.add_root_certificate(cert);
        }
    }

    builder
        .build()
        .map_err(|e| SolanaCdnError::Tls(e.to_string()))
}

async fn pipe_api_verify(
    client: &reqwest::Client,
    cfg: &PipeApiClientConfig,
    req: &PipeApiVerifyRequest,
) -> Result<PipeApiVerifyResult, SolanaCdnError> {
    let url = format!("{}/v1/solanacdn-agent/verify", cfg.base_url);
    let resp = client
        .post(&url)
        .header("x-api-key", &cfg.api_key)
        .json(req)
        .send()
        .await
        .map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        let msg = json_error_message(&body);
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify failed: {} ({})",
            status.as_u16(),
            msg
        )));
    }

    let parsed: PipeApiVerifyResponse =
        serde_json::from_str(&body).map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    if !parsed.ok {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify returned unexpected response: {body}"
        )));
    }
    if parsed.run_token.trim().is_empty() {
        return Err(SolanaCdnError::AuthFailed(
            "Pipe API verify returned empty run_token".to_string(),
        ));
    }
    if parsed.agent_id.trim().is_empty() || parsed.run_id.trim().is_empty() {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify returned invalid ids: agent_id='{}' run_id='{}'",
            parsed.agent_id, parsed.run_id
        )));
    }
    if parsed.ingest.url.trim().is_empty() || parsed.ingest.interval_secs == 0 {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify returned invalid ingest config: url='{}' interval_secs={}",
            parsed.ingest.url, parsed.ingest.interval_secs
        )));
    }

    let mut pop_expected_pubkeys: HashMap<SocketAddr, PubkeyBytes> = HashMap::new();

    let mut pop_endpoints: Vec<SocketAddr> = Vec::new();
    if !parsed.pop_endpoints_v2.is_empty() {
        use std::str::FromStr;

        let mut candidates: HashMap<SocketAddr, (String, Option<PubkeyBytes>)> = HashMap::new();
        for pop in parsed.pop_endpoints_v2.iter() {
            let addr = match pop.quic_endpoint.parse::<SocketAddr>() {
                Ok(a) => a,
                Err(e) => {
                    warn!(
                        "solanacdn: ignoring invalid pop_endpoint from Pipe API verify v2 ({}): {e}",
                        pop.quic_endpoint
                    );
                    continue;
                }
            };
            let expected_pubkey = match pop
                .node_pubkey
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                Some(b58) => match Pubkey::from_str(b58) {
                    Ok(pk) => Some(PubkeyBytes(pk.to_bytes())),
                    Err(e) => {
                        warn!(
                            "solanacdn: ignoring invalid pop node_pubkey from Pipe API verify v2 ({b58}): {e}"
                        );
                        None
                    }
                },
                None => None,
            };

            match candidates.entry(addr) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert((pop.id.clone(), expected_pubkey));
                }
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    if o.get().1.is_none() && expected_pubkey.is_some() {
                        o.insert((pop.id.clone(), expected_pubkey));
                    }
                }
            }
        }

        let mut scored: Vec<(u64, SocketAddr, String, Option<PubkeyBytes>)> = candidates
            .into_iter()
            .map(|(addr, (id, pk))| (pop_assign_score(&req.validator_pubkey, &id), addr, id, pk))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.2.cmp(&b.2)).then_with(|| a.1.cmp(&b.1)));
        if scored.len() > PIPE_API_VERIFY_MAX_POP_ENDPOINTS {
            scored.truncate(PIPE_API_VERIFY_MAX_POP_ENDPOINTS);
        }

        for (_score, addr, _id, pk) in scored {
            pop_endpoints.push(addr);
            if let Some(pk) = pk {
                pop_expected_pubkeys.insert(addr, pk);
            }
        }
    }

    if pop_endpoints.is_empty() {
        let mut candidates: Vec<(String, SocketAddr)> = Vec::new();
        for s in parsed.pop_endpoints.iter() {
            match s.parse::<SocketAddr>() {
                Ok(addr) => candidates.push((s.trim().to_string(), addr)),
                Err(e) => {
                    warn!("solanacdn: ignoring invalid pop_endpoint from Pipe API verify ({s}): {e}");
                }
            }
        }

        candidates.sort_by(|(a_raw, a_addr), (b_raw, b_addr)| {
            let sa = pop_assign_score(&req.validator_pubkey, a_raw);
            let sb = pop_assign_score(&req.validator_pubkey, b_raw);
            sb.cmp(&sa).then_with(|| a_raw.cmp(b_raw)).then_with(|| a_addr.cmp(b_addr))
        });
        if candidates.len() > PIPE_API_VERIFY_MAX_POP_ENDPOINTS {
            candidates.truncate(PIPE_API_VERIFY_MAX_POP_ENDPOINTS);
        }
        pop_endpoints = candidates.into_iter().map(|(_raw, addr)| addr).collect();
    }
    pop_endpoints.sort();
    pop_endpoints.dedup();
    pop_expected_pubkeys.retain(|ep, _| pop_endpoints.binary_search(ep).is_ok());

    Ok(PipeApiVerifyResult {
        agent_id: parsed.agent_id,
        run_id: parsed.run_id,
        run_token: parsed.run_token,
        heartbeat_schema_version: parsed.heartbeat_schema_version.unwrap_or(0),
        ingest: parsed.ingest,
        pop_endpoints,
        pop_expected_pubkeys,
    })
}

async fn pipe_api_pop_session_token(
    client: &reqwest::Client,
    base_url: &str,
    run_token: &str,
) -> Result<PipeApiSessionTokenResponse, SolanaCdnError> {
    let url = format!("{base_url}/v1/solanacdn-agent/session-token");
    let resp = client
        .post(&url)
        .bearer_auth(run_token)
        .send()
        .await
        .map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        let msg = json_error_message(&body);
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API pop session token failed: {} ({})",
            status.as_u16(),
            msg
        )));
    }

    let parsed: PipeApiSessionTokenResponse =
        serde_json::from_str(&body).map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    if !parsed.ok {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API pop session token returned unexpected response: {body}"
        )));
    }
    if parsed.session_token.trim().is_empty() {
        return Err(SolanaCdnError::AuthFailed(
            "Pipe API pop session token returned empty session_token".to_string(),
        ));
    }

    Ok(parsed)
}

struct PipeApiRefresher {
    session_token_rx: watch::Receiver<Option<String>>,
    pop_endpoints_rx: watch::Receiver<Vec<SocketAddr>>,
    verify_rx: watch::Receiver<Option<PipeApiVerifyResult>>,
}

fn spawn_pipe_pop_session_token_refresher(
    cfg: PipeApiClientConfig,
    validator_pubkey_base58: String,
    direct_shreds_from_pop: bool,
    handle: Arc<SolanaCdnHandle>,
) -> PipeApiRefresher {
    let (token_tx, token_rx) = watch::channel::<Option<String>>(None);
    let (pops_tx, pops_rx) = watch::channel::<Vec<SocketAddr>>(Vec::new());
    let (verify_tx, verify_rx) = watch::channel::<Option<PipeApiVerifyResult>>(None);
    tokio::spawn(async move {
        let client = match build_pipe_api_http_client(&cfg) {
            Ok(c) => c,
            Err(e) => {
                warn!("solanacdn: failed to init Pipe API client: {e}");
                return;
            }
        };

        let agent_instance_id = format!("agave-integrated-{}", validator_pubkey_base58);
        let verify_req = PipeApiVerifyRequest {
            schema_version: 1,
            agent_instance_id,
            validator_pubkey: validator_pubkey_base58.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            capture_mode: "agave".to_string(),
            iface: first_env(&["SOLANACDN_AGENT_IFACE", "SOLANACDN_IFACE"])
                .unwrap_or_else(|| "integrated".to_string()),
            direct_shreds_from_pop,
        };

        let mut current_expires_at: Option<std::time::Instant> = None;
        let mut backoff = Duration::from_secs(1);

        loop {
            let verify = match pipe_api_verify(&client, &cfg, &verify_req).await {
                Ok(v) => {
                    backoff = Duration::from_secs(1);
                    v
                }
                Err(e) => {
                    warn!("solanacdn: Pipe API verify failed: {e}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
            };

            handle.note_pipe_pop_expected_pubkeys(&verify.pop_expected_pubkeys);
            verify_tx.send_replace(Some(verify.clone()));

            if verify.pop_endpoints.is_empty() {
                debug!("solanacdn: Pipe API verify returned no pop_endpoints");
            } else if *pops_tx.borrow() != verify.pop_endpoints {
                info!(
                    "solanacdn: discovered POP endpoints via Pipe API: {:?}",
                    verify.pop_endpoints
                );
                pops_tx.send_replace(verify.pop_endpoints.clone());
            }
            let run_token = verify.run_token;

            loop {
                match pipe_api_pop_session_token(&client, &cfg.base_url, &run_token).await {
                    Ok(parsed) => {
                        let expires_in_secs = (parsed.expires_in.max(1) as u64).clamp(5, 3600);
                        let refresh_in_secs = (expires_in_secs / 2).clamp(5, expires_in_secs);
                        current_expires_at =
                            Some(std::time::Instant::now() + Duration::from_secs(expires_in_secs));

                        token_tx.send_replace(Some(parsed.session_token));
                        tokio::time::sleep(Duration::from_secs(refresh_in_secs)).await;
                    }
                    Err(e) => {
                        warn!("solanacdn: Pipe API pop session token refresh failed: {e}");

                        if current_expires_at.is_some_and(|t| t <= std::time::Instant::now()) {
                            token_tx.send_replace(None);
                        }

                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        break;
                    }
                }
            }
        }
    });
    PipeApiRefresher {
        session_token_rx: token_rx,
        pop_endpoints_rx: pops_rx,
        verify_rx,
    }
}

async fn run_pipe_ingest_reporter(
    client: reqwest::Client,
    mut verify_rx: watch::Receiver<Option<PipeApiVerifyResult>>,
    handle: Arc<SolanaCdnHandle>,
    cfg: Arc<SolanaCdnConfig>,
    validator_pubkey_base58: String,
) {
    let mut interval_secs = loop {
        if let Some(v) = verify_rx.borrow().clone() {
            if v.heartbeat_schema_version != 0 {
                handle.update_heartbeat_schema_version(v.heartbeat_schema_version);
            }
            break v.ingest.interval_secs.max(10).min(3600);
        }
        if verify_rx.changed().await.is_err() {
            return;
        }
    };

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let Some(v) = verify_rx.borrow().clone() else {
                    continue;
                };
                if v.ingest.url.trim().is_empty() {
                    continue;
                }
                if v.ingest.max_events == 0 {
                    continue;
                }

                let sent_at = now_ts();
                let (body, consumed_race_samples) = build_pipe_ingest_body(
                    &v,
                    handle.as_ref(),
                    cfg.as_ref(),
                    &validator_pubkey_base58,
                    sent_at,
                );

                let resp = client
                    .post(&v.ingest.url)
                    .bearer_auth(v.run_token)
                    .json(&body)
                    .send()
                    .await;

                match resp {
                    Ok(r) if r.status().is_success() => {
                        handle.pipe_ingest_consume_race_samples(consumed_race_samples);
                    }
                    Ok(r) => {
                        let status = r.status();
                        let text = r.text().await.unwrap_or_default();
                        debug!("solanacdn: Pipe ingest failed: {} {}", status.as_u16(), text);
                    }
                    Err(e) => {
                        debug!("solanacdn: Pipe ingest failed: {e}");
                    }
                }
            }
            changed = verify_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let Some(v) = verify_rx.borrow().clone() else {
                    continue;
                };
                if v.heartbeat_schema_version != 0 {
                    handle.update_heartbeat_schema_version(v.heartbeat_schema_version);
                }
                let next = v.ingest.interval_secs.max(10).min(3600);
                if next != interval_secs {
                    interval_secs = next;
                    interval = tokio::time::interval(Duration::from_secs(interval_secs));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                }
            }
        }
    }
}

fn build_pipe_ingest_body(
    v: &PipeApiVerifyResult,
    handle: &SolanaCdnHandle,
    cfg: &SolanaCdnConfig,
    validator_pubkey_base58: &str,
    sent_at: i64,
) -> (serde_json::Value, usize) {
    let runtime = handle.ingest_runtime_snapshot();
    let counters = handle.ingest_counters_totals();
    let (race, race_samples) =
        handle.pipe_ingest_race_snapshot_and_samples(v.ingest.max_events as usize);

    let mut body = serde_json::json!({
        "sent_at": sent_at,
        "agent": {
            "agent_id": v.agent_id,
            "run_id": v.run_id,
            "validator_pubkey": validator_pubkey_base58,
            "version": env!("CARGO_PKG_VERSION"),
            "capture_mode": "agave",
            "iface": "",
            "direct_shreds_from_pop": cfg.direct_shreds_from_pop,
        },
        "counters": counters,
        "runtime": runtime,
    });

    if race.enabled || race.pairs_total > 0 {
        let mut race_json = serde_json::json!({
            "enabled": race.enabled,
            "sample_bits": race.sample_bits,
            "window_ms": race.window_ms,
            "pairs_total": race.pairs_total,
            "wins_solanacdn_total": race.wins_solanacdn_total,
            "wins_gossip_total": race.wins_gossip_total,
            "ties_total": race.ties_total,
            "inflight": race.inflight,
            "last_winner": race.last_winner.map(|w| w.as_str()),
            "last_lead_ms": race.last_lead_ms,
            "last_shred_slot": race.last_shred_slot,
        });
        if let Some(hist) = race.histogram {
            if let Some(obj) = race_json.as_object_mut() {
                obj.insert(
                    "histogram".to_string(),
                    serde_json::json!({
                        "delta_bucket_counts": hist.delta_bucket_counts,
                        "delta_sum_ms": hist.delta_sum_ms,
                        "delta_count": hist.delta_count,
                        "delta_by_pop_endpoint": hist.delta_by_pop_endpoint,
                        "delta_by_hour_utc": hist.delta_by_hour_utc,
                    }),
                );
            }
        }
        if let Some(obj) = body.as_object_mut() {
            obj.insert("race".to_string(), race_json);
        }
    }

    let max_body_bytes = v.ingest.max_body_bytes;
    if max_body_bytes > 0 && body_exceeds_max_bytes(&body, max_body_bytes) {
        // Drop optional fields rather than failing the ingest request.
        if let Some(obj) = body.as_object_mut() {
            obj.remove("runtime");
        }
    }
    if max_body_bytes > 0 && body_exceeds_max_bytes(&body, max_body_bytes) {
        // Keep totals, but drop histograms/segments first.
        if let Some(race_obj) = body.get_mut("race").and_then(|v| v.as_object_mut()) {
            race_obj.remove("histogram");
        }
    }
    if max_body_bytes > 0 && body_exceeds_max_bytes(&body, max_body_bytes) {
        if let Some(obj) = body.as_object_mut() {
            obj.remove("race");
        }
    }

    let mut consumed_race_samples: usize = 0;
    let max_samples = (v.ingest.max_events as usize).min(2048);
    if max_samples > 0 && !race_samples.is_empty() {
        let arr: Vec<serde_json::Value> = race_samples
            .into_iter()
            .take(max_samples)
            .map(|s| serde_json::json!({"delta_ms": s.delta_ms, "gossip_src_ip": s.gossip_src_ip.to_string()}))
            .collect();
        consumed_race_samples = arr.len();

        if let Some(obj) = body.as_object_mut() {
            obj.insert("race_samples".to_string(), serde_json::Value::Array(arr));
        }

        if max_body_bytes > 0 {
            // If the payload is too large, reduce race_samples aggressively to fit.
            let mut attempts: usize = 0;
            while body_exceeds_max_bytes(&body, max_body_bytes) {
                attempts = attempts.saturating_add(1);
                if attempts > 8 {
                    break;
                }
                if consumed_race_samples <= 1 {
                    consumed_race_samples = 0;
                    if let Some(obj) = body.as_object_mut() {
                        obj.remove("race_samples");
                    }
                    break;
                }
                consumed_race_samples = (consumed_race_samples / 2).max(1);
                if let Some(arr) = body.get_mut("race_samples").and_then(|v| v.as_array_mut()) {
                    arr.truncate(consumed_race_samples);
                }
            }

            if body_exceeds_max_bytes(&body, max_body_bytes) {
                consumed_race_samples = 0;
                if let Some(obj) = body.as_object_mut() {
                    obj.remove("race_samples");
                }
            }
        }
    }

    (body, consumed_race_samples)
}

fn body_exceeds_max_bytes(body: &serde_json::Value, max_body_bytes: u64) -> bool {
    let Ok(bytes) = serde_json::to_vec(body) else {
        return false;
    };
    (bytes.len() as u64) > max_body_bytes
}

#[derive(Debug)]
struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

fn make_quic_client_config(cfg: &SolanaCdnConfig) -> Result<quinn::ClientConfig, SolanaCdnError> {
    init_rustls();

    if cfg.tls_insecure_skip_verify {
        return Ok(make_quic_client_config_insecure());
    }

    let roots = match cfg.tls_ca_cert_path.as_ref() {
        Some(path) => load_root_cert_store(path)?,
        None => load_default_root_cert_store()?,
    };

    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.enable_early_data = false;
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("QUIC client crypto");
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

fn make_quic_client_config_insecure() -> quinn::ClientConfig {
    let mut tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    tls.enable_early_data = true;
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("QUIC client crypto");
    quinn::ClientConfig::new(Arc::new(crypto))
}

fn fnv1a_128_update(mut hash: u128, bytes: &[u8]) -> u128 {
    for b in bytes {
        hash ^= *b as u128;
        hash = hash.wrapping_mul(FNV1A_128_PRIME);
    }
    hash
}

fn shred_kind_tag(kind: ShredKind) -> u8 {
    match kind {
        ShredKind::Tvu => 0,
        ShredKind::Gossip => 1,
    }
}

fn compute_shred_batch_id(items: &[(ShredKind, Bytes)]) -> u128 {
    let mut hash = FNV1A_128_OFFSET_BASIS;
    hash = fnv1a_128_update(
        hash,
        &u32::try_from(items.len()).unwrap_or(u32::MAX).to_le_bytes(),
    );
    for (kind, payload) in items {
        hash = fnv1a_128_update(hash, &[shred_kind_tag(*kind)]);
        hash = fnv1a_128_update(
            hash,
            &u32::try_from(payload.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        hash = fnv1a_128_update(hash, payload.as_ref());
    }
    hash
}

fn vote_flow_id(dst: &SocketAddr) -> u64 {
    let mut h = DefaultHasher::new();
    dst.hash(&mut h);
    h.finish()
}

fn vote_dedup_key(dst: &SocketAddr, payload: &[u8]) -> u128 {
    let mut hash = FNV1A_128_OFFSET_BASIS;
    match dst {
        SocketAddr::V4(v4) => {
            hash = fnv1a_128_update(hash, &v4.ip().octets());
            hash = fnv1a_128_update(hash, &v4.port().to_le_bytes());
        }
        SocketAddr::V6(v6) => {
            hash = fnv1a_128_update(hash, &v6.ip().octets());
            hash = fnv1a_128_update(hash, &v6.port().to_le_bytes());
            hash = fnv1a_128_update(hash, &v6.scope_id().to_le_bytes());
        }
    }
    hash = fnv1a_128_update(
        hash,
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    hash = fnv1a_128_update(hash, payload);
    hash
}

async fn write_len_prefixed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), SolanaCdnError> {
    let len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame too large"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_len_prefixed<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, SolanaCdnError> {
    read_len_prefixed_with_limit(reader, DEFAULT_MAX_FRAME_BYTES).await
}

async fn read_len_prefixed_with_limit<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, SolanaCdnError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_frame_bytes {
        return Err(SolanaCdnError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame too large",
        )));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn write_agent_msg<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &AgentToPop,
) -> Result<(), SolanaCdnError> {
    let payload = encode_envelope(msg)?;
    write_len_prefixed(writer, &payload).await
}

async fn read_pop_msg<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<PopToAgent, SolanaCdnError> {
    let bytes = read_len_prefixed_with_limit(reader, max_frame_bytes).await?;
    Ok(decode_envelope(&bytes)?)
}

#[derive(Clone)]
struct QuicConnectConfig {
    client_config: quinn::ClientConfig,
    server_name: String,
}

#[derive(Clone)]
struct AuthContext {
    validator_pubkey: PubkeyBytes,
    signing_key: SigningKey,
    identity_keypair: Arc<Keypair>,
}

impl AuthContext {
    fn new(identity_keypair: Arc<Keypair>) -> Result<Self, SolanaCdnError> {
        let validator_pubkey = PubkeyBytes(identity_keypair.pubkey().to_bytes());
        let signing_key = signing_key_from_solana_keypair(identity_keypair.as_ref())?;
        Ok(Self {
            validator_pubkey,
            signing_key,
            identity_keypair,
        })
    }

    fn build_auth_request(&self) -> Result<AuthRequest, SolanaCdnError> {
        let payload = AuthRequestPayload {
            validator_pubkey: self.validator_pubkey,
            delegate_pubkey: None,
            delegation_cert: None,
            timestamp_ms: now_ms(),
            nonce: random_nonce_16(),
        };
        Ok(AuthRequest::sign(payload, &self.signing_key)?)
    }
}

fn signing_key_from_solana_keypair(identity: &Keypair) -> Result<SigningKey, SolanaCdnError> {
    let bytes = identity.secret_bytes();
    let sk = SigningKey::from_bytes(&bytes);
    let validator_pk = PubkeyBytes(identity.pubkey().to_bytes());
    let got_pk = PubkeyBytes(sk.verifying_key().to_bytes());
    if validator_pk != got_pk {
        return Err(SolanaCdnError::AuthFailed(
            "validator identity pubkey mismatch".to_string(),
        ));
    }
    Ok(sk)
}

#[derive(Clone)]
struct ShredBatchDeduper {
    inner: Arc<std::sync::Mutex<ShredBatchDeduperInner>>,
}

struct ShredBatchDeduperInner {
    max_entries: usize,
    order: VecDeque<u128>,
    seen: HashSet<u128>,
}

impl ShredBatchDeduper {
    fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(ShredBatchDeduperInner {
                max_entries,
                order: VecDeque::new(),
                seen: HashSet::new(),
            })),
        }
    }

    fn insert_if_new(&self, batch_id: u128) -> bool {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if inner.seen.contains(&batch_id) {
            return false;
        }
        inner.seen.insert(batch_id);
        inner.order.push_back(batch_id);
        while inner.order.len() > inner.max_entries {
            if let Some(old) = inner.order.pop_front() {
                inner.seen.remove(&old);
            }
        }
        true
    }
}

#[derive(Debug)]
enum SessionEvent {
    Connected {
        endpoint: SocketAddr,
        udp_enabled: bool,
    },
    Disconnected {
        endpoint: SocketAddr,
    },
    RttSample {
        endpoint: SocketAddr,
        rtt_ms: u64,
    },
}

#[derive(Clone, Copy, Debug)]
struct ConnectedPop {
    udp_enabled: bool,
    rtt_ewma_ms: u64,
    rtt_valid: bool,
}

struct ManagedSession {
    uplink: mpsc::Sender<UplinkMsg>,
    stop_tx: watch::Sender<bool>,
}

#[derive(Clone)]
struct ControlTlsClient {
    connector: tokio_rustls::TlsConnector,
    server_name: ServerName<'static>,
}

fn make_control_tls_client(
    cfg: &SolanaCdnConfig,
) -> Result<Option<ControlTlsClient>, SolanaCdnError> {
    if cfg.control_endpoint.is_none() {
        return Ok(None);
    }

    init_rustls();
    let server_name = ServerName::try_from(cfg.control_server_name.clone())
        .map_err(|e| SolanaCdnError::InvalidServerName(e.to_string()))?;

    let tls_config = if cfg.control_tls_insecure_skip_verify {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth()
    } else {
        let roots = match cfg.control_tls_ca_cert_path.as_ref() {
            Some(path) => load_root_cert_store(path)?,
            None => load_default_root_cert_store()?,
        };
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    Ok(Some(ControlTlsClient {
        connector: tokio_rustls::TlsConnector::from(Arc::new(tls_config)),
        server_name,
    }))
}

async fn fetch_pops_from_control(
    control_endpoint: SocketAddr,
    tls: Option<&ControlTlsClient>,
) -> Result<Vec<SocketAddr>, SolanaCdnError> {
    let stream = TcpStream::connect(control_endpoint).await?;
    if let Err(e) = stream.set_nodelay(true) {
        debug!("solanacdn: failed to set TCP_NODELAY for control {control_endpoint}: {e}");
    }

    match tls {
        None => {
            let (reader, writer) = tokio::io::split(stream);
            fetch_pops_from_control_over_io(reader, writer).await
        }
        Some(tls) => {
            let tls_stream = tls
                .connector
                .connect(tls.server_name.clone(), stream)
                .await
                .map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
            let (reader, writer) = tokio::io::split(tls_stream);
            fetch_pops_from_control_over_io(reader, writer).await
        }
    }
}

async fn fetch_pops_from_control_over_io<R, W>(
    mut reader: R,
    mut writer: W,
) -> Result<Vec<SocketAddr>, SolanaCdnError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let req = ControlRequest::ListPops;
    let payload = encode_envelope(&req)?;
    write_len_prefixed(&mut writer, &payload).await?;

    let bytes = read_len_prefixed(&mut reader).await?;
    let resp: ControlResponse = decode_envelope(&bytes)?;
    match resp {
        ControlResponse::PopList(list) => {
            Ok(list.pops.into_iter().map(|p| p.public_addr).collect())
        }
        ControlResponse::Error(err) => Err(SolanaCdnError::AuthFailed(format!(
            "control error: {}: {}",
            err.code, err.message
        ))),
        other => Err(SolanaCdnError::AuthFailed(format!(
            "unexpected control response: {other:?}"
        ))),
    }
}

const METRICS_HTTP_MAX_REQUEST_BYTES: usize = 8 * 1024;

fn prometheus_escape_label_value(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', r#"\""#)
}

fn histogram_quantile_seconds(
    q: f64,
    lower_bound_ms: i64,
    upper_bounds_ms: &[i64],
    cumulative_counts: &[u64],
    total_count: u64,
) -> Option<f64> {
    if !(0.0..=1.0).contains(&q) {
        return None;
    }
    if upper_bounds_ms.len() != cumulative_counts.len() {
        return None;
    }
    if total_count == 0 {
        return None;
    }

    let rank = (total_count as f64) * q;
    let mut prev_count: f64 = 0.0;
    let mut prev_bound_ms: f64 = lower_bound_ms as f64;
    for (i, bound_ms) in upper_bounds_ms.iter().enumerate() {
        let count = cumulative_counts[i] as f64;
        if count >= rank {
            let upper_bound_ms = *bound_ms as f64;
            let bucket_count = (count - prev_count).max(0.0);
            if bucket_count == 0.0 {
                return Some(upper_bound_ms / 1000.0);
            }
            let fraction = ((rank - prev_count) / bucket_count).clamp(0.0, 1.0);
            let estimate_ms = prev_bound_ms + (upper_bound_ms - prev_bound_ms) * fraction;
            return Some(estimate_ms / 1000.0);
        }
        prev_count = count;
        prev_bound_ms = *bound_ms as f64;
    }

    Some((*upper_bounds_ms.last()? as f64) / 1000.0)
}

fn format_prometheus_metrics(handle: &SolanaCdnHandle) -> String {
    let status = handle.status_snapshot();
    let race = {
        let tracker = match handle.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.snapshot(&handle.cfg)
    };
    let mut out = String::new();

    out.push_str(
        "# HELP solanacdn_connected Whether SolanaCDN has an active publisher session (0/1)\n",
    );
    out.push_str("# TYPE solanacdn_connected gauge\n");
    out.push_str(&format!(
        "solanacdn_connected {}\n",
        if status.connected { 1 } else { 0 }
    ));

    out.push_str(
        "# HELP solanacdn_publisher_switches_total Number of times the publisher POP changed\n",
    );
    out.push_str("# TYPE solanacdn_publisher_switches_total counter\n");
    out.push_str(&format!(
        "solanacdn_publisher_switches_total {}\n",
        status.publisher_switches_total
    ));

    out.push_str(
        "# HELP solanacdn_publisher_present Whether a publisher endpoint is selected (0/1)\n",
    );
    out.push_str("# TYPE solanacdn_publisher_present gauge\n");
    out.push_str(&format!(
        "solanacdn_publisher_present {}\n",
        if status.publisher.is_some() { 1 } else { 0 }
    ));
    if let Some(publisher) = status.publisher.as_deref() {
        let publisher = prometheus_escape_label_value(publisher);
        out.push_str("# HELP solanacdn_publisher_info Publisher POP identity as a label\n");
        out.push_str("# TYPE solanacdn_publisher_info gauge\n");
        out.push_str(&format!(
            "solanacdn_publisher_info{{publisher=\"{}\"}} 1\n",
            publisher
        ));
    }

    out.push_str("# HELP solanacdn_pop_connected POP connectivity by endpoint\n");
    out.push_str("# TYPE solanacdn_pop_connected gauge\n");
    for ep in &status.connected_pops {
        let ep = prometheus_escape_label_value(ep);
        out.push_str(&format!(
            "solanacdn_pop_connected{{endpoint=\"{}\"}} 1\n",
            ep
        ));
    }

    out.push_str("# HELP solanacdn_rx_shred_bytes_total Total shred bytes received from POPs\n");
    out.push_str("# TYPE solanacdn_rx_shred_bytes_total counter\n");
    out.push_str(&format!(
        "solanacdn_rx_shred_bytes_total {}\n",
        status.rx_shred_bytes_total
    ));

    out.push_str(
        "# HELP solanacdn_rx_shred_payloads_total Total shred payloads received from POPs\n",
    );
    out.push_str("# TYPE solanacdn_rx_shred_payloads_total counter\n");
    out.push_str(&format!(
        "solanacdn_rx_shred_payloads_total {}\n",
        status.rx_shred_payloads_total
    ));

    out.push_str("# HELP solanacdn_shred_batches_dropped_oversized_total PushShredBatch messages dropped due to oversized shred count\n");
    out.push_str("# TYPE solanacdn_shred_batches_dropped_oversized_total counter\n");
    out.push_str(&format!(
        "solanacdn_shred_batches_dropped_oversized_total {}\n",
        status.dropped_shred_batches_oversized_total
    ));

    out.push_str("# HELP solanacdn_rx_shred_payloads_per_sec Recent shred payload receive rate\n");
    out.push_str("# TYPE solanacdn_rx_shred_payloads_per_sec gauge\n");
    out.push_str(&format!(
        "solanacdn_rx_shred_payloads_per_sec {}\n",
        status.rx_shred_payloads_per_sec
    ));

    out.push_str(
        "# HELP solanacdn_tunneled_vote_packets_total Total vote packets tunneled to POPs\n",
    );
    out.push_str("# TYPE solanacdn_tunneled_vote_packets_total counter\n");
    out.push_str(&format!(
        "solanacdn_tunneled_vote_packets_total {}\n",
        status.tunneled_vote_packets_total
    ));

    out.push_str("# HELP solanacdn_tunneled_vote_packets_per_sec Recent vote tunnel rate\n");
    out.push_str("# TYPE solanacdn_tunneled_vote_packets_per_sec gauge\n");
    out.push_str(&format!(
        "solanacdn_tunneled_vote_packets_per_sec {}\n",
        status.tunneled_vote_packets_per_sec
    ));

    out.push_str("# HELP solanacdn_rx_vote_packets_total Total vote packets received from POPs\n");
    out.push_str("# TYPE solanacdn_rx_vote_packets_total counter\n");
    out.push_str(&format!(
        "solanacdn_rx_vote_packets_total {}\n",
        status.rx_vote_packets_total
    ));

    out.push_str("# HELP solanacdn_dropped_vote_datagrams_total Total vote datagrams dropped (deduped or failed injection)\n");
    out.push_str("# TYPE solanacdn_dropped_vote_datagrams_total counter\n");
    out.push_str(&format!(
        "solanacdn_dropped_vote_datagrams_total {}\n",
        status.dropped_vote_datagrams_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_dropped_oversized_payload_total Vote datagrams dropped due to oversized payloads\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_dropped_oversized_payload_total counter\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_dropped_oversized_payload_total {}\n",
        status.dropped_vote_datagrams_oversized_payload_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_dropped_invalid_payload_total Vote datagrams dropped due to invalid payloads (not a vote transaction)\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_dropped_invalid_payload_total counter\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_dropped_invalid_payload_total {}\n",
        status.dropped_vote_datagrams_invalid_payload_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_dropped_unexpected_dst_total Vote datagrams dropped due to unexpected destinations\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_dropped_unexpected_dst_total counter\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_dropped_unexpected_dst_total {}\n",
        status.dropped_vote_datagrams_unexpected_dst_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_allowed_dsts_len Number of currently allowed vote tunnel destinations\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_allowed_dsts_len gauge\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_allowed_dsts_len {}\n",
        status.vote_tunnel_allowed_dsts_len
    ));

    out.push_str("# HELP solanacdn_quic_shreds_dropped_unexpected_msg_total Shreds stream frames dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_quic_shreds_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_quic_shreds_dropped_unexpected_msg_total {}\n",
        status.dropped_quic_shreds_unexpected_msg_total
    ));

    out.push_str("# HELP solanacdn_quic_votes_dropped_unexpected_msg_total Votes stream frames dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_quic_votes_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_quic_votes_dropped_unexpected_msg_total {}\n",
        status.dropped_quic_votes_unexpected_msg_total
    ));

    out.push_str("# HELP solanacdn_udp_shreds_dropped_unexpected_peer_total UDP shred datagrams dropped due to unexpected peer IP\n");
    out.push_str("# TYPE solanacdn_udp_shreds_dropped_unexpected_peer_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_shreds_dropped_unexpected_peer_total {}\n",
        status.dropped_udp_shreds_unexpected_peer_total
    ));

    out.push_str("# HELP solanacdn_udp_shreds_dropped_unexpected_msg_total UDP shred datagrams dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_udp_shreds_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_shreds_dropped_unexpected_msg_total {}\n",
        status.dropped_udp_shreds_unexpected_msg_total
    ));

    out.push_str("# HELP solanacdn_udp_votes_dropped_unexpected_peer_total UDP vote datagrams dropped due to unexpected peer IP\n");
    out.push_str("# TYPE solanacdn_udp_votes_dropped_unexpected_peer_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_votes_dropped_unexpected_peer_total {}\n",
        status.dropped_udp_votes_unexpected_peer_total
    ));

    out.push_str("# HELP solanacdn_udp_votes_dropped_unexpected_msg_total UDP vote datagrams dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_udp_votes_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_votes_dropped_unexpected_msg_total {}\n",
        status.dropped_udp_votes_unexpected_msg_total
    ));

    if let Some(slot) = status.last_shred_slot {
        out.push_str("# HELP solanacdn_last_shred_slot Last Solana slot observed from POP-delivered shreds\n");
        out.push_str("# TYPE solanacdn_last_shred_slot gauge\n");
        out.push_str(&format!("solanacdn_last_shred_slot {}\n", slot));
    }
    if let Some(age_ms) = status.last_shred_age_ms {
        out.push_str("# HELP solanacdn_last_shred_age_seconds Age of last POP-delivered shred\n");
        out.push_str("# TYPE solanacdn_last_shred_age_seconds gauge\n");
        out.push_str(&format!(
            "solanacdn_last_shred_age_seconds {}\n",
            (age_ms as f64) / 1000.0
        ));
    }

    if let Some(slot) = status.last_accepted_shred_slot {
        out.push_str("# HELP solanacdn_last_accepted_shred_slot Last Solana slot observed from SolanaCDN shreds accepted into the validator pipeline\n");
        out.push_str("# TYPE solanacdn_last_accepted_shred_slot gauge\n");
        out.push_str(&format!("solanacdn_last_accepted_shred_slot {}\n", slot));
    }
    if let Some(age_ms) = status.last_accepted_shred_age_ms {
        out.push_str("# HELP solanacdn_last_accepted_shred_age_seconds Age of last SolanaCDN shred accepted into the validator pipeline\n");
        out.push_str("# TYPE solanacdn_last_accepted_shred_age_seconds gauge\n");
        out.push_str(&format!(
            "solanacdn_last_accepted_shred_age_seconds {}\n",
            (age_ms as f64) / 1000.0
        ));
    }

    out.push_str(
        "# HELP solanacdn_race_enabled Whether SolanaCDN race measurement is enabled (0/1)\n",
    );
    out.push_str("# TYPE solanacdn_race_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_race_enabled {}\n",
        if race.enabled { 1 } else { 0 }
    ));

    out.push_str(
        "# HELP solanacdn_race_sample_bits Deterministic sampling bits (1/(2^bits) shreds)\n",
    );
    out.push_str("# TYPE solanacdn_race_sample_bits gauge\n");
    out.push_str(&format!(
        "solanacdn_race_sample_bits {}\n",
        race.sample_bits
    ));

    out.push_str("# HELP solanacdn_race_window_seconds Race matching window seconds\n");
    out.push_str("# TYPE solanacdn_race_window_seconds gauge\n");
    out.push_str(&format!(
        "solanacdn_race_window_seconds {}\n",
        (race.window_ms as f64) / 1000.0
    ));

    out.push_str(
        "# HELP solanacdn_race_inflight Number of sampled shreds awaiting the other source\n",
    );
    out.push_str("# TYPE solanacdn_race_inflight gauge\n");
    out.push_str(&format!("solanacdn_race_inflight {}\n", race.inflight));

    out.push_str("# HELP solanacdn_race_pairs_total Number of shreds observed on both sources within the race window\n");
    out.push_str("# TYPE solanacdn_race_pairs_total counter\n");
    out.push_str(&format!(
        "solanacdn_race_pairs_total {}\n",
        race.pairs_total
    ));

    out.push_str(
        "# HELP solanacdn_race_wins_total Number of observed pairs where a source arrived first\n",
    );
    out.push_str("# TYPE solanacdn_race_wins_total counter\n");
    out.push_str(&format!(
        "solanacdn_race_wins_total{{winner=\"solanacdn\"}} {}\n",
        race.wins_solanacdn_total
    ));
    out.push_str(&format!(
        "solanacdn_race_wins_total{{winner=\"gossip\"}} {}\n",
        race.wins_gossip_total
    ));

    out.push_str("# HELP solanacdn_race_ties_total Number of observed pairs with identical first-seen timestamps\n");
    out.push_str("# TYPE solanacdn_race_ties_total counter\n");
    out.push_str(&format!("solanacdn_race_ties_total {}\n", race.ties_total));

    out.push_str(
        "# HELP solanacdn_race_lead_seconds Lead time where winner arrived before loser\n",
    );
    out.push_str("# TYPE solanacdn_race_lead_seconds histogram\n");
    if let Some(hist) = race.histogram {
        for (winner, buckets) in hist.lead_bucket_counts_by_winner {
            let winner_label = winner.as_str();
            for (i, bound_ms) in RACE_LEAD_BUCKETS_MS.iter().enumerate() {
                let le = (*bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"{:.3}\"}} {}\n",
                    winner_label, le, buckets[i]
                ));
            }

            let count = hist
                .lead_count_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            let sum_ms = hist
                .lead_sum_ms_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| *v)
                .unwrap_or(0);

            out.push_str(&format!(
                "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"+Inf\"}} {}\n",
                winner_label, count
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_sum{{winner=\"{}\"}} {}\n",
                winner_label,
                (sum_ms as f64) / 1000.0
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_count{{winner=\"{}\"}} {}\n",
                winner_label, count
            ));
        }

        out.push_str("# HELP solanacdn_race_lead_seconds_quantile Approximate lead-time quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_lead_seconds_quantile gauge\n");
        let lead_bounds_ms: [i64; RACE_LEAD_BUCKETS_MS.len()] =
            std::array::from_fn(|i| RACE_LEAD_BUCKETS_MS[i] as i64);
        for winner in [RaceSource::SolanaCdn, RaceSource::Gossip] {
            let winner_label = winner.as_str();
            let buckets = hist
                .lead_bucket_counts_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| v)
                .expect("winner buckets must exist");
            let count = hist
                .lead_count_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            for (q_label, q) in [("0.50", 0.50), ("0.95", 0.95), ("0.99", 0.99)] {
                let value = histogram_quantile_seconds(q, 0, &lead_bounds_ms, &buckets[..], count)
                    .unwrap_or(0.0);
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_quantile{{winner=\"{}\",quantile=\"{}\"}} {:.6}\n",
                    winner_label, q_label, value
                ));
            }
        }

        out.push_str("# HELP solanacdn_race_delta_seconds Signed delta between first-seen times (solanacdn_first - gossip_first); negative means SolanaCDN arrived first\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds histogram\n");
        for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
            let le = (*bound_ms as f64) / 1000.0;
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_bucket{{le=\"{:.3}\"}} {}\n",
                le, hist.delta_bucket_counts[i]
            ));
        }
        out.push_str(&format!(
            "solanacdn_race_delta_seconds_bucket{{le=\"+Inf\"}} {}\n",
            hist.delta_count
        ));
        out.push_str(&format!(
            "solanacdn_race_delta_seconds_sum {}\n",
            (hist.delta_sum_ms as f64) / 1000.0
        ));
        out.push_str(&format!(
            "solanacdn_race_delta_seconds_count {}\n",
            hist.delta_count
        ));

        out.push_str("# HELP solanacdn_race_delta_seconds_quantile Approximate signed-delta quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_quantile gauge\n");
        for (q_label, q) in [("0.50", 0.50), ("0.95", 0.95), ("0.99", 0.99)] {
            let value = histogram_quantile_seconds(
                q,
                RACE_DELTA_BUCKETS_MS[0],
                &RACE_DELTA_BUCKETS_MS,
                &hist.delta_bucket_counts[..],
                hist.delta_count,
            )
            .unwrap_or(0.0);
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_quantile{{quantile=\"{}\"}} {:.6}\n",
                q_label, value
            ));
        }

        out.push_str("# HELP solanacdn_race_delta_seconds_by_pop_endpoint Signed delta segmented by SolanaCDN POP endpoint\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_pop_endpoint histogram\n");
        for (ep, seg) in &hist.delta_by_pop_endpoint {
            let ep = prometheus_escape_label_value(&ep.to_string());
            for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
                let le = (*bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_delta_seconds_by_pop_endpoint_bucket{{pop_endpoint=\"{}\",le=\"{:.3}\"}} {}\n",
                    ep, le, seg.bucket_counts[i]
                ));
            }
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_pop_endpoint_bucket{{pop_endpoint=\"{}\",le=\"+Inf\"}} {}\n",
                ep, seg.count
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_pop_endpoint_sum{{pop_endpoint=\"{}\"}} {}\n",
                ep,
                (seg.sum_ms as f64) / 1000.0
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_pop_endpoint_count{{pop_endpoint=\"{}\"}} {}\n",
                ep, seg.count
            ));
        }

        out.push_str("# HELP solanacdn_race_delta_seconds_by_hour_utc Signed delta segmented by UTC hour-of-day (00-23)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_hour_utc histogram\n");
        for (hour, seg) in hist.delta_by_hour_utc.iter().enumerate() {
            let hour_label = format!("{hour:02}");
            for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
                let le = (*bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_delta_seconds_by_hour_utc_bucket{{hour_utc=\"{}\",le=\"{:.3}\"}} {}\n",
                    hour_label, le, seg.bucket_counts[i]
                ));
            }
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_hour_utc_bucket{{hour_utc=\"{}\",le=\"+Inf\"}} {}\n",
                hour_label, seg.count
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_hour_utc_sum{{hour_utc=\"{}\"}} {}\n",
                hour_label,
                (seg.sum_ms as f64) / 1000.0
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_hour_utc_count{{hour_utc=\"{}\"}} {}\n",
                hour_label, seg.count
            ));
        }
    } else {
        for winner_label in ["solanacdn", "gossip"] {
            for bound_ms in RACE_LEAD_BUCKETS_MS {
                let le = (bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"{:.3}\"}} 0\n",
                    winner_label, le
                ));
            }
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"+Inf\"}} 0\n",
                winner_label
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_sum{{winner=\"{}\"}} 0\n",
                winner_label
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_count{{winner=\"{}\"}} 0\n",
                winner_label
            ));
        }

        out.push_str("# HELP solanacdn_race_lead_seconds_quantile Approximate lead-time quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_lead_seconds_quantile gauge\n");
        for winner_label in ["solanacdn", "gossip"] {
            for q_label in ["0.50", "0.95", "0.99"] {
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_quantile{{winner=\"{}\",quantile=\"{}\"}} 0\n",
                    winner_label, q_label
                ));
            }
        }

        out.push_str("# HELP solanacdn_race_delta_seconds Signed delta between first-seen times (solanacdn_first - gossip_first); negative means SolanaCDN arrived first\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds histogram\n");
        for bound_ms in RACE_DELTA_BUCKETS_MS {
            let le = (bound_ms as f64) / 1000.0;
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_bucket{{le=\"{:.3}\"}} 0\n",
                le
            ));
        }
        out.push_str("solanacdn_race_delta_seconds_bucket{le=\"+Inf\"} 0\n");
        out.push_str("solanacdn_race_delta_seconds_sum 0\n");
        out.push_str("solanacdn_race_delta_seconds_count 0\n");

        out.push_str("# HELP solanacdn_race_delta_seconds_quantile Approximate signed-delta quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_quantile gauge\n");
        for q_label in ["0.50", "0.95", "0.99"] {
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_quantile{{quantile=\"{}\"}} 0\n",
                q_label
            ));
        }

        out.push_str("# HELP solanacdn_race_delta_seconds_by_pop_endpoint Signed delta segmented by SolanaCDN POP endpoint\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_pop_endpoint histogram\n");

        out.push_str("# HELP solanacdn_race_delta_seconds_by_hour_utc Signed delta segmented by UTC hour-of-day (00-23)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_hour_utc histogram\n");
    }

    out
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) {
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(headers.as_bytes()).await;
    let _ = stream.write_all(body).await;
}

async fn handle_metrics_conn(mut stream: TcpStream, handle: Arc<SolanaCdnHandle>) {
    let mut buf = [0u8; 1024];
    let mut req = Vec::new();
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                req.extend_from_slice(&buf[..n]);
                if req.len() > METRICS_HTTP_MAX_REQUEST_BYTES {
                    write_http_response(
                        &mut stream,
                        "413 Payload Too Large",
                        "text/plain; charset=utf-8",
                        b"request too large\n",
                    )
                    .await;
                    return;
                }
                if req.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let req_str = String::from_utf8_lossy(&req);
    let first = req_str.lines().next().unwrap_or_default();
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();

    if method != "GET" {
        write_http_response(
            &mut stream,
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            b"method not allowed\n",
        )
        .await;
        return;
    }

    match path {
        "/metrics" => {
            let body = format_prometheus_metrics(handle.as_ref());
            write_http_response(
                &mut stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                body.as_bytes(),
            )
            .await;
        }
        "/solanacdn/status" | "/status" => {
            let status = handle.status_snapshot();
            let body = serde_json::to_vec(&status).unwrap_or_else(|_| b"{}".to_vec());
            write_http_response(
                &mut stream,
                "200 OK",
                "application/json; charset=utf-8",
                &body,
            )
            .await;
        }
        _ => {
            write_http_response(
                &mut stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
            )
            .await;
        }
    }
}

async fn run_metrics_server(
    listen_addr: SocketAddr,
    handle: Arc<SolanaCdnHandle>,
    exit: Arc<AtomicBool>,
) -> Result<(), SolanaCdnError> {
    let listener = TcpListener::bind(listen_addr).await?;
    let bound = listener.local_addr()?;
    info!("solanacdn: metrics listening on http://{bound}/metrics");

    loop {
        if exit.load(Ordering::Relaxed) {
            return Ok(());
        }
        let accept = tokio::select! {
            res = listener.accept() => res,
            _ = tokio::time::sleep(Duration::from_millis(200)) => continue,
        };
        let (stream, _) = match accept {
            Ok(v) => v,
            Err(_) => continue,
        };
        tokio::spawn(handle_metrics_conn(stream, handle.clone()));
    }
}

async fn run(
    cfg: SolanaCdnConfig,
    identity_keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
    handle: Arc<SolanaCdnHandle>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
) -> Result<(), SolanaCdnError> {
    let mut cfg = cfg;

    if cfg.pipe_api_base_url.trim().is_empty() {
        if let Some(v) = first_env(&["SOLANACDN_AGENT_API_BASE", "PIPE_API_BASE"]) {
            cfg.pipe_api_base_url = v;
        }
    }
    if cfg.pipe_api_base_url.trim().is_empty()
        || cfg.pipe_api_base_url.trim() == "https://api.pipedev.network"
    {
        if let Some(v) = read_env_file_value("/etc/solanacdn/agent.env", "SOLANACDN_AGENT_API_BASE")
        {
            cfg.pipe_api_base_url = v;
        }
    }
    cfg.pipe_api_base_url = normalize_base_url(&cfg.pipe_api_base_url);

    if cfg
        .pipe_api_token
        .as_deref()
        .is_none_or(|s| s.trim().is_empty())
    {
        cfg.pipe_api_token = first_env(&["SOLANACDN_AGENT_API_TOKEN", "PIPE_API_KEY"]);
    }
    if cfg
        .pipe_api_token
        .as_deref()
        .is_none_or(|s| s.trim().is_empty())
    {
        cfg.pipe_api_token =
            read_env_file_value("/etc/solanacdn/agent.env", "SOLANACDN_AGENT_API_TOKEN");
    }

    // If the operator hasn't configured a CA bundle for POP TLS verification, attempt to
    // bootstrap it from the Pipe control plane. This keeps "no flags" setups working when POPs
    // use a private CA.
    maybe_bootstrap_pop_tls_from_pipe_api(&mut cfg).await;

    if let Some(listen_addr) = cfg.metrics_listen_addr {
        let handle = handle.clone();
        let exit = exit.clone();
        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(listen_addr, handle, exit).await {
                warn!("solanacdn: metrics server exited with error: {e}");
            }
        });
    }

    let validator_pubkey = PubkeyBytes(identity_keypair.pubkey().to_bytes());
    let validator_pubkey_base58 = validator_pubkey.to_base58();

    let pipe_api = cfg
        .pipe_api_token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|token| {
            spawn_pipe_pop_session_token_refresher(
                PipeApiClientConfig {
                    base_url: cfg.pipe_api_base_url.clone(),
                    api_key: token.to_string(),
                    timeout: Duration::from_millis(cfg.pipe_api_timeout_ms),
                    tls_insecure_skip_verify: cfg.pipe_api_tls_insecure_skip_verify,
                    tls_ca_cert_path: cfg.pipe_api_tls_ca_cert_path.clone(),
                },
                validator_pubkey_base58.clone(),
                cfg.direct_shreds_from_pop,
                handle.clone(),
            )
        });

    handle.note_pop_endpoints(cfg.pop_endpoints.as_slice());

    let cfg = Arc::new(cfg);
    let auth = Arc::new(AuthContext::new(identity_keypair.clone())?);
    let quic_connect = Arc::new(QuicConnectConfig {
        client_config: make_quic_client_config(&cfg)?,
        server_name: cfg.server_name.clone(),
    });
    let control_tls = make_control_tls_client(&cfg)?;

    let shred_deduper = ShredBatchDeduper::new(8192);

    let (publisher_tx, publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
    let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();

    let pipe_session_token_rx = pipe_api.as_ref().map(|p| p.session_token_rx.clone());
    let pipe_pop_endpoints_rx = pipe_api.as_ref().map(|p| p.pop_endpoints_rx.clone());
    let pipe_verify_rx = pipe_api.as_ref().map(|p| p.verify_rx.clone());

    if let Some(verify_rx) = pipe_verify_rx {
        let cfg = cfg.clone();
        let handle = handle.clone();
        let validator_pubkey_base58 = validator_pubkey_base58.clone();
        tokio::spawn(async move {
            let client = match build_pipe_api_http_client(&PipeApiClientConfig {
                base_url: cfg.pipe_api_base_url.clone(),
                api_key: String::new(),
                timeout: Duration::from_millis(cfg.pipe_api_timeout_ms),
                tls_insecure_skip_verify: cfg.pipe_api_tls_insecure_skip_verify,
                tls_ca_cert_path: cfg.pipe_api_tls_ca_cert_path.clone(),
            }) {
                Ok(c) => c,
                Err(e) => {
                    warn!("solanacdn: failed to init Pipe API ingest client: {e}");
                    return;
                }
            };
            run_pipe_ingest_reporter(client, verify_rx, handle, cfg, validator_pubkey_base58).await;
        });
    }

    manage_pop_sessions(
        cfg,
        auth,
        quic_connect,
        control_tls,
        handle,
        inject_tpu,
        inject_tvu,
        inject_gossip,
        inject_vote,
        shred_deduper,
        pipe_session_token_rx,
        pipe_pop_endpoints_rx,
        publisher_tx,
        publisher_rx,
        events_tx,
        events_rx,
        exit,
    )
    .await;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn manage_pop_sessions(
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    control_tls: Option<ControlTlsClient>,
    handle: Arc<SolanaCdnHandle>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    mut pipe_pop_endpoints_rx: Option<watch::Receiver<Vec<SocketAddr>>>,
    publisher_tx: watch::Sender<Option<SocketAddr>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    mut session_events_rx: mpsc::UnboundedReceiver<SessionEvent>,
    exit: Arc<AtomicBool>,
) {
    let static_endpoints: HashSet<SocketAddr> = cfg.pop_endpoints.iter().copied().collect();
    let preferred = cfg.pop_endpoints.first().copied();

    let mut control_discovered: HashSet<SocketAddr> = HashSet::new();
    let mut pipe_discovered: HashSet<SocketAddr> = pipe_pop_endpoints_rx
        .as_ref()
        .map(|rx| rx.borrow().iter().copied().collect())
        .unwrap_or_default();

    let mut desired: HashSet<SocketAddr> = static_endpoints
        .union(&control_discovered)
        .chain(pipe_discovered.iter())
        .copied()
        .collect();
    let mut sessions: HashMap<SocketAddr, ManagedSession> = HashMap::new();
    let mut connected: HashMap<SocketAddr, ConnectedPop> = HashMap::new();

    let mut exit_tick = tokio::time::interval(Duration::from_millis(200));
    exit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let control_refresh = Duration::from_millis(cfg.control_refresh_ms.max(250));
    let mut control_tick = tokio::time::interval(control_refresh);
    control_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut status_tick = tokio::time::interval(Duration::from_secs(30));
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Spawn initial sessions.
    for endpoint in desired.iter().copied() {
        spawn_session(
            endpoint,
            cfg.clone(),
            auth.clone(),
            quic_connect.clone(),
            handle.clone(),
            inject_tpu,
            inject_tvu,
            inject_gossip,
            inject_vote,
            shred_deduper.clone(),
            pipe_session_token_rx.clone(),
            publisher_rx.clone(),
            session_events_tx.clone(),
            &mut sessions,
        );
    }

    loop {
        tokio::select! {
            _ = exit_tick.tick() => {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
            }
            _ = control_tick.tick(), if cfg.control_endpoint.is_some() => {
                if let Some(control_endpoint) = cfg.control_endpoint {
                    match fetch_pops_from_control(control_endpoint, control_tls.as_ref()).await {
                        Ok(list) => {
                            let discovered: HashSet<SocketAddr> = list.into_iter().collect();
                            if !discovered.is_empty() && discovered != control_discovered {
                                control_discovered = discovered;
                                desired = static_endpoints
                                    .union(&control_discovered)
                                    .chain(pipe_discovered.iter())
                                    .copied()
                                    .collect();
                                handle.note_pop_endpoints(desired.iter().copied().collect::<Vec<_>>().as_slice());
                            }
                        }
                        Err(e) => {
                            debug!("solanacdn: control discovery failed for {control_endpoint}: {e}");
                        }
                    }
                }
            }
            _ = status_tick.tick() => {
                let publisher = *publisher_tx.borrow();
                let mut pops: Vec<SocketAddr> = connected.keys().copied().collect();
                pops.sort();
                let status = handle.status_snapshot();
                info!(
                    "solanacdn: status publisher={:?} connected_pops={} pops={:?} tvu_shred_ingest_mode={:?} tvu_shred_stale={:?} tvu_shred_stale_for_ms={:?} last_shred_slot={:?} last_shred_age_ms={:?} last_accepted_shred_slot={:?} last_accepted_shred_age_ms={:?} rx_shred_payloads_total={} rx_shred_payloads_per_sec={:.1} published_shred_batches_total={} pushed_shred_batches_total={} tunneled_vote_packets_total={} rx_vote_packets_total={} rx_tx_packets_total={}",
                    publisher,
                    pops.len(),
                    pops,
                    status.tvu_shred_ingest_mode,
                    status.tvu_shred_stale,
                    status.tvu_shred_stale_for_ms,
                    status.last_shred_slot,
                    status.last_shred_age_ms,
                    status.last_accepted_shred_slot,
                    status.last_accepted_shred_age_ms,
                    status.rx_shred_payloads_total,
                    status.rx_shred_payloads_per_sec,
                    handle.published_shred_batches.load(Ordering::Relaxed),
                    handle.pushed_shred_batches.load(Ordering::Relaxed),
                    status.tunneled_vote_packets_total,
                    handle.rx_vote_packets.load(Ordering::Relaxed),
                    handle.rx_tx_packets.load(Ordering::Relaxed),
                );
            }
            _ = async {
                if let Some(rx) = pipe_pop_endpoints_rx.as_mut() {
                    let _ = rx.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                if let Some(rx) = pipe_pop_endpoints_rx.as_ref() {
                    let next: HashSet<SocketAddr> = rx.borrow().iter().copied().collect();
                    if !next.is_empty() && next != pipe_discovered {
                        pipe_discovered = next;
                        desired = static_endpoints
                            .union(&control_discovered)
                            .chain(pipe_discovered.iter())
                            .copied()
                            .collect();
                        handle.note_pop_endpoints(desired.iter().copied().collect::<Vec<_>>().as_slice());
                    }
                }
            }
            ev = session_events_rx.recv() => {
                let Some(ev) = ev else { break; };
                match ev {
                    SessionEvent::Connected{endpoint, udp_enabled} => {
                        connected.insert(endpoint, ConnectedPop { udp_enabled, rtt_ewma_ms: 0, rtt_valid: false });
                        handle.note_connected_pop(endpoint);
                        info!("solanacdn: connected to POP {endpoint} (udp_enabled={udp_enabled})");
                    }
                    SessionEvent::Disconnected{endpoint} => {
                        connected.remove(&endpoint);
                        handle.note_disconnected_pop(endpoint);
                        info!("solanacdn: disconnected from POP {endpoint}");
                    }
                    SessionEvent::RttSample{endpoint, rtt_ms} => {
                        if let Some(info) = connected.get_mut(&endpoint) {
                            let sample = rtt_ms.max(1);
                            if !info.rtt_valid {
                                info.rtt_valid = true;
                                info.rtt_ewma_ms = sample;
                            } else {
                                info.rtt_ewma_ms = (info.rtt_ewma_ms.saturating_mul(7).saturating_add(sample)) / 8;
                            }
                        }
                    }
                }
            }
        }

        // Reconcile sessions for desired endpoints.
        let to_add: Vec<SocketAddr> = desired
            .iter()
            .copied()
            .filter(|e| !sessions.contains_key(e))
            .collect();
        for endpoint in to_add {
            spawn_session(
                endpoint,
                cfg.clone(),
                auth.clone(),
                quic_connect.clone(),
                handle.clone(),
                inject_tpu,
                inject_tvu,
                inject_gossip,
                inject_vote,
                shred_deduper.clone(),
                pipe_session_token_rx.clone(),
                publisher_rx.clone(),
                session_events_tx.clone(),
                &mut sessions,
            );
        }

        let to_remove: Vec<SocketAddr> = sessions
            .keys()
            .copied()
            .filter(|e| !desired.contains(e))
            .collect();
        for endpoint in to_remove {
            if let Some(sess) = sessions.remove(&endpoint) {
                let _ = sess.stop_tx.send(true);
                connected.remove(&endpoint);
            }
        }

        // Select publisher.
        let prefer_udp = cfg.udp_mode != DataPlaneMode::Off;
        let new_publisher = select_publisher(preferred, &desired, &connected, prefer_udp);
        if *publisher_tx.borrow() != new_publisher {
            info!("solanacdn: selected publisher {:?}", new_publisher);
            let _ = publisher_tx.send(new_publisher);
        }

        // Update publisher uplink on the global handle.
        let uplink = new_publisher.and_then(|ep| {
            sessions.get(&ep).map(|s| {
                Arc::new(SessionUplink {
                    tx: s.uplink.clone(),
                })
            })
        });
        handle.set_publisher_uplink(new_publisher, uplink);
    }

    // Stop all sessions.
    for sess in sessions.into_values() {
        let _ = sess.stop_tx.send(true);
    }
    handle.set_publisher_uplink(None, None);
}

fn select_publisher(
    preferred: Option<SocketAddr>,
    desired: &HashSet<SocketAddr>,
    connected: &HashMap<SocketAddr, ConnectedPop>,
    prefer_udp: bool,
) -> Option<SocketAddr> {
    if connected.is_empty() {
        return None;
    }
    if let Some(pref) = preferred {
        if desired.contains(&pref) && connected.contains_key(&pref) {
            return Some(pref);
        }
    }

    let mut candidates: Vec<(SocketAddr, ConnectedPop)> = desired
        .iter()
        .copied()
        .filter_map(|e| connected.get(&e).copied().map(|info| (e, info)))
        .collect();
    if candidates.is_empty() {
        return None;
    }
    if prefer_udp {
        let has_udp = candidates.iter().any(|(_e, info)| info.udp_enabled);
        if has_udp {
            candidates.retain(|(_e, info)| info.udp_enabled);
        }
    }
    candidates.sort_by_key(|(e, info)| {
        if info.rtt_valid {
            (0u8, info.rtt_ewma_ms, *e)
        } else {
            (1u8, u64::MAX, *e)
        }
    });
    candidates.first().map(|(e, _)| *e)
}

#[allow(clippy::too_many_arguments)]
fn spawn_session(
    endpoint: SocketAddr,
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    handle: Arc<SolanaCdnHandle>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    sessions: &mut HashMap<SocketAddr, ManagedSession>,
) {
    let capacity = cfg.shreds_queue_len.max(cfg.votes_queue_len).max(1024);
    let (uplink_tx, uplink_rx) = mpsc::channel::<UplinkMsg>(capacity);
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(run_pop_session_forever(
        endpoint,
        cfg,
        auth,
        quic_connect,
        handle,
        uplink_rx,
        inject_tpu,
        inject_tvu,
        inject_gossip,
        inject_vote,
        shred_deduper,
        pipe_session_token_rx,
        publisher_rx,
        session_events_tx,
        stop_rx,
    ));
    sessions.insert(
        endpoint,
        ManagedSession {
            uplink: uplink_tx,
            stop_tx,
        },
    );
}

#[allow(clippy::too_many_arguments)]
async fn run_pop_session_forever(
    endpoint: SocketAddr,
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    handle: Arc<SolanaCdnHandle>,
    mut uplink_rx: mpsc::Receiver<UplinkMsg>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_millis(200);
    loop {
        if *stop_rx.borrow() {
            return;
        }
        match run_pop_session(
            endpoint,
            cfg.clone(),
            auth.clone(),
            quic_connect.clone(),
            handle.clone(),
            &mut uplink_rx,
            inject_tpu,
            inject_tvu,
            inject_gossip,
            inject_vote,
            shred_deduper.clone(),
            pipe_session_token_rx.clone(),
            publisher_rx.clone(),
            session_events_tx.clone(),
            stop_rx.clone(),
        )
        .await
        {
            Ok(()) => {}
            Err(e) => debug!("solanacdn: session {endpoint} ended with error: {e}"),
        }

        tokio::select! {
            _ = stop_rx.changed() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(5));
    }
}

async fn wait_for_pipe_session_token(
    token_rx: &mut watch::Receiver<Option<String>>,
    mut stop_rx: watch::Receiver<bool>,
) -> Option<String> {
    loop {
        if *stop_rx.borrow() {
            return None;
        }
        if let Some(token) = token_rx.borrow().clone() {
            let trimmed = token.trim().to_string();
            if !trimmed.is_empty() {
                return Some(trimmed);
            }
        }
        tokio::select! {
            _ = stop_rx.changed() => {}
            res = token_rx.changed() => {
                if res.is_err() {
                    return None;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_pop_session(
    endpoint: SocketAddr,
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    handle: Arc<SolanaCdnHandle>,
    uplink_rx: &mut mpsc::Receiver<UplinkMsg>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    mut pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    mut stop_rx: watch::Receiver<bool>,
) -> Result<(), SolanaCdnError> {
    let pipe_session_token = if let Some(rx) = pipe_session_token_rx.as_mut() {
        wait_for_pipe_session_token(rx, stop_rx.clone()).await
    } else {
        None
    };
    if *stop_rx.borrow() {
        return Ok(());
    }

    let mut quic = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))?;
    quic.set_default_client_config(quic_connect.client_config.clone());
    let conn = quic
        .connect(endpoint, quic_connect.server_name.as_str())
        .map_err(|e| SolanaCdnError::QuicConnect(e.to_string()))?
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(e.to_string()))?;

    let (mut ctrl_send, mut ctrl_recv) = conn
        .open_bi()
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(format!("open_bi(control): {e}")))?;

    // Auth on control stream
    let auth_req = auth.build_auth_request()?;
    match pipe_session_token {
        Some(session_token) => {
            write_agent_msg(
                &mut ctrl_send,
                &AgentToPop::AuthWithSessionToken(AuthWithSessionToken {
                    auth: auth_req,
                    session_token,
                }),
            )
            .await?;
        }
        None => {
            write_agent_msg(&mut ctrl_send, &AgentToPop::Auth(auth_req)).await?;
        }
    }
    let auth_ok = match read_pop_msg(&mut ctrl_recv, CTRL_MAX_FRAME_BYTES).await? {
        PopToAgent::AuthOk(ok) => ok,
        PopToAgent::AuthError(err) => {
            if err.message.contains("missing pipe session token") {
                warn!(
                    "solanacdn: POP requires a Pipe session token. Set --solanacdn-api-token (or env SOLANACDN_AGENT_API_TOKEN/PIPE_API_KEY). If running with --solanacdn-only/--solanacdn-hybrid, the validator will not receive POP shreds until a token is configured."
                );
            }
            return Err(SolanaCdnError::AuthFailed(format!(
                "{}: {}",
                err.code, err.message
            )));
        }
        other => {
            return Err(SolanaCdnError::AuthFailed(format!(
                "unexpected auth response: {other:?}"
            )))
        }
    };
    let pop_pubkey = auth_ok.pop_pubkey;

    if let Some(expected) = handle.expected_pipe_pop_pubkey(endpoint) {
        if expected != pop_pubkey {
            let expected_b58 = expected.to_base58();
            let actual_b58 = pop_pubkey.to_base58();
            match cfg.pop_pubkey_pinning {
                PopPubkeyPinningMode::Off => {}
                PopPubkeyPinningMode::Warn => {
                    warn!("solanacdn: POP pubkey mismatch for {endpoint}: expected={expected_b58} actual={actual_b58} (continuing)");
                }
                PopPubkeyPinningMode::Enforce => {
                    return Err(SolanaCdnError::AuthFailed(format!(
                        "POP pubkey mismatch for {endpoint}: expected={expected_b58} actual={actual_b58}"
                    )));
                }
            }
        }
    }

    let udp_advertised = auth_ok.udp_shreds_port != 0 && auth_ok.udp_votes_port != 0;
    let udp_enabled = match cfg.udp_mode {
        DataPlaneMode::Off => false,
        DataPlaneMode::Auto => udp_advertised,
        DataPlaneMode::Always => {
            if !udp_advertised {
                return Err(SolanaCdnError::AuthFailed(
                    "udp_mode=always but POP did not advertise UDP ports".to_string(),
                ));
            }
            true
        }
    };

    handle.note_pop_endpoint(endpoint);

    let _ = session_events_tx.send(SessionEvent::Connected {
        endpoint,
        udp_enabled,
    });

    let udp_token = auth_ok.udp_token;
    let pop_shreds_addr = SocketAddr::new(endpoint.ip(), auth_ok.udp_shreds_port);
    let pop_votes_addr = SocketAddr::new(endpoint.ip(), auth_ok.udp_votes_port);

    let udp_shreds = if udp_enabled {
        Some(Arc::new(UdpSocket::bind("0.0.0.0:0").await?))
    } else {
        None
    };
    let udp_votes = if udp_enabled {
        Some(Arc::new(UdpSocket::bind("0.0.0.0:0").await?))
    } else {
        None
    };

    if let (Some(shreds), Some(votes)) = (udp_shreds.as_ref(), udp_votes.as_ref()) {
        write_agent_msg(
            &mut ctrl_send,
            &AgentToPop::RegisterUdpPorts {
                shreds_port: shreds.local_addr()?.port(),
                votes_port: votes.local_addr()?.port(),
            },
        )
        .await?;
    }

    let (mut shreds_send, mut shreds_recv) = conn
        .open_bi()
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(format!("open_bi(shreds): {e}")))?;
    write_agent_msg(
        &mut shreds_send,
        &AgentToPop::StreamHello(StreamKind::Shreds),
    )
    .await?;
    let (mut votes_send, mut votes_recv) = conn
        .open_bi()
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(format!("open_bi(votes): {e}")))?;
    write_agent_msg(&mut votes_send, &AgentToPop::StreamHello(StreamKind::Votes)).await?;

    let udp_inject_tpu = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    udp_inject_tpu.connect(inject_tpu).await?;
    let udp_inject_tvu = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    udp_inject_tvu.connect(inject_tvu).await?;
    let udp_inject_gossip = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    udp_inject_gossip.connect(inject_gossip).await?;
    let udp_inject_votes = Arc::new(VoteInjectSockets::bind().await?);

    // Register validator ports + direct injection preference.
    let is_publisher = *publisher_rx.borrow() == Some(endpoint);
    let direct_shreds = udp_enabled && cfg.direct_shreds_from_pop && is_publisher;
    write_agent_msg(
        &mut ctrl_send,
        &AgentToPop::RegisterValidatorPorts {
            tvu_port: inject_tvu.port(),
            gossip_port: inject_gossip.port(),
            direct_shreds,
        },
    )
    .await?;

    if cfg.subscribe_shreds && is_publisher {
        write_agent_msg(&mut ctrl_send, &AgentToPop::SubscribeShreds).await?;
    }

    let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(256);
    let ctrl_writer_task = tokio::spawn(async move {
        while let Some(msg) = ctrl_out_rx.recv().await {
            if write_agent_msg(&mut ctrl_send, &msg).await.is_err() {
                return;
            }
        }
    });

    // Pipe session token refresher: push AuthRefresh updates on the control stream.
    let auth_refresh_task = if let Some(mut token_rx) = pipe_session_token_rx.take() {
        let ctrl_out_tx = ctrl_out_tx.clone();
        Some(tokio::spawn(async move {
            let mut last_sent: Option<String> = token_rx.borrow().clone();
            loop {
                if token_rx.changed().await.is_err() {
                    return;
                }
                let Some(token) = token_rx.borrow().clone() else {
                    continue;
                };
                let token = token.trim().to_string();
                if token.is_empty() {
                    continue;
                }
                if Some(token.clone()) == last_sent {
                    continue;
                }
                last_sent = Some(token.clone());
                if ctrl_out_tx
                    .send(AgentToPop::AuthRefresh(AuthRefresh {
                        session_token: token,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }))
    } else {
        None
    };

    let (shreds_out_tx, mut shreds_out_rx) = mpsc::channel::<AgentToPop>(4096);
    let shreds_writer_task = tokio::spawn(async move {
        while let Some(msg) = shreds_out_rx.recv().await {
            if write_agent_msg(&mut shreds_send, &msg).await.is_err() {
                return;
            }
        }
    });

    let (votes_out_tx, mut votes_out_rx) = mpsc::channel::<AgentToPop>(1024);
    let votes_writer_task = tokio::spawn(async move {
        while let Some(msg) = votes_out_rx.recv().await {
            if write_agent_msg(&mut votes_send, &msg).await.is_err() {
                return;
            }
        }
    });

    let last_hb_sent_ms = Arc::new(AtomicU64::new(0));

    // Heartbeat loop (control stream).
    let hb_task = {
        let ctrl_out_tx = ctrl_out_tx.clone();
        let hb_handle = handle.clone();
        let hb_last_sent_ms = last_hb_sent_ms.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let now = now_ms();
                hb_last_sent_ms.store(now, Ordering::Relaxed);
                let hb = Heartbeat {
                    now_ms: now,
                    stats: hb_handle.heartbeat_stats(),
                };
                if ctrl_out_tx.send(AgentToPop::Heartbeat(hb)).await.is_err() {
                    return;
                }
            }
        })
    };

    // Publisher watcher: subscribe/unsubscribe + direct_shreds toggle.
    let publisher_task = {
        let mut publisher_rx = publisher_rx.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            let mut last_is_publisher = is_publisher;
            loop {
                let now_is_publisher = *publisher_rx.borrow() == Some(endpoint);
                if now_is_publisher != last_is_publisher {
                    last_is_publisher = now_is_publisher;
                    if cfg.subscribe_shreds {
                        let msg = if now_is_publisher {
                            AgentToPop::SubscribeShreds
                        } else {
                            AgentToPop::UnsubscribeShreds
                        };
                        let _ = ctrl_out_tx.send(msg).await;
                    }
                    if cfg.direct_shreds_from_pop && udp_enabled {
                        let _ = ctrl_out_tx
                            .send(AgentToPop::RegisterValidatorPorts {
                                tvu_port: inject_tvu.port(),
                                gossip_port: inject_gossip.port(),
                                direct_shreds: now_is_publisher,
                            })
                            .await;
                    }
                }
                if publisher_rx.changed().await.is_err() {
                    return;
                }
            }
        })
    };

    // Control stream reader.
    let ctrl_reader_task = {
        let conn = conn.clone();
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg = match read_pop_msg(&mut ctrl_recv, CTRL_MAX_FRAME_BYTES).await {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("solanacdn: control stream read error from {endpoint}: {e}");
                        conn.close(
                            quinn::VarInt::from_u32(SCDN_PROTOCOL_VIOLATION_CLOSE_CODE),
                            b"control stream read error",
                        );
                        return;
                    }
                };
                handle_pop_msg(
                    endpoint,
                    pop_pubkey,
                    &cfg,
                    auth.as_ref(),
                    &handle,
                    &ctrl_out_tx,
                    &mut publisher_rx,
                    &shred_deduper,
                    &udp_inject_tpu,
                    &udp_inject_tvu,
                    &udp_inject_gossip,
                    inject_vote,
                    &udp_inject_votes,
                    &session_events_tx,
                    &last_hb_sent_ms,
                    msg,
                )
                .await;
            }
        })
    };

    // Shreds stream reader.
    let shreds_reader_task = {
        let conn = conn.clone();
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        tokio::spawn(async move {
            loop {
                // Avoid processing shreds when not the selected publisher (reduces attack surface
                // and CPU for non-publisher sessions).
                while *publisher_rx.borrow() != Some(endpoint) {
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = read_pop_msg(&mut shreds_recv, SHREDS_MAX_FRAME_BYTES) => {
                        let msg = match res {
                            Ok(v) => v,
                            Err(e) => {
                                debug!("solanacdn: shreds stream read error from {endpoint}: {e}");
                                conn.close(
                                    quinn::VarInt::from_u32(SCDN_PROTOCOL_VIOLATION_CLOSE_CODE),
                                    b"shreds stream read error",
                                );
                                return;
                            }
                        };
                        if !matches!(msg, PopToAgent::PushShredBatch(_)) {
                            handle
                                .dropped_quic_shreds_unexpected_msg
                                .fetch_add(1, Ordering::Relaxed);
                            warn!("solanacdn: protocol violation from {endpoint}: unexpected msg type on shreds stream; closing session");
                            conn.close(
                                quinn::VarInt::from_u32(SCDN_PROTOCOL_VIOLATION_CLOSE_CODE),
                                b"unexpected msg on shreds stream",
                            );
                            return;
                        }
                        handle_pop_msg(
                            endpoint,
                            pop_pubkey,
                            &cfg,
                            auth.as_ref(),
                            &handle,
                            &ctrl_out_tx,
                            &mut publisher_rx,
                            &shred_deduper,
                            &udp_inject_tpu,
                            &udp_inject_tvu,
                            &udp_inject_gossip,
                            inject_vote,
                            &udp_inject_votes,
                            &session_events_tx,
                            &last_hb_sent_ms,
                            msg,
                        )
                        .await;
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        })
    };

    // Votes stream reader.
    let votes_reader_task = {
        let conn = conn.clone();
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        tokio::spawn(async move {
            loop {
                // Avoid processing vote downlink when not the selected publisher.
                while *publisher_rx.borrow() != Some(endpoint) {
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = read_pop_msg(&mut votes_recv, VOTES_MAX_FRAME_BYTES) => {
                        let msg = match res {
                            Ok(v) => v,
                            Err(e) => {
                                debug!("solanacdn: votes stream read error from {endpoint}: {e}");
                                conn.close(
                                    quinn::VarInt::from_u32(SCDN_PROTOCOL_VIOLATION_CLOSE_CODE),
                                    b"votes stream read error",
                                );
                                return;
                            }
                        };
                        if !matches!(msg, PopToAgent::PushVoteDatagram(_)) {
                            handle
                                .dropped_quic_votes_unexpected_msg
                                .fetch_add(1, Ordering::Relaxed);
                            warn!("solanacdn: protocol violation from {endpoint}: unexpected msg type on votes stream; closing session");
                            conn.close(
                                quinn::VarInt::from_u32(SCDN_PROTOCOL_VIOLATION_CLOSE_CODE),
                                b"unexpected msg on votes stream",
                            );
                            return;
                        }
                        handle_pop_msg(
                            endpoint,
                            pop_pubkey,
                            &cfg,
                            auth.as_ref(),
                            &handle,
                            &ctrl_out_tx,
                            &mut publisher_rx,
                            &shred_deduper,
                            &udp_inject_tpu,
                            &udp_inject_tvu,
                            &udp_inject_gossip,
                            inject_vote,
                            &udp_inject_votes,
                            &session_events_tx,
                            &last_hb_sent_ms,
                            msg,
                        )
                        .await;
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        })
    };

    // UDP shreds downlink (PushShredBatch + PushShredFecChunk + DirectShredsProbe).
    let udp_shreds_task = if let Some(sock) = udp_shreds.clone() {
        let conn = conn.clone();
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        Some(tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            let mut fec: HashMap<u64, (solanacdn_protocol::fec::RaptorqDecoder, u64)> =
                HashMap::new();
            let mut last_cleanup_ms = now_ms();
            const FEC_MAX_OBJECTS: usize = 1024;
            const FEC_EXPIRE_MS: u64 = 5_000;
            loop {
                // Avoid spending CPU on shreds UDP downlink when not the selected publisher.
                while *publisher_rx.borrow() != Some(endpoint) {
                    fec.clear();
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = sock.recv_from(&mut buf) => {
                        let (len, peer) = match res {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        let bytes = &buf[..len];
                        if !bytes.starts_with(&udp_token) {
                            continue;
                        }
                        let msg_bytes = match bytes.get(solanacdn_protocol::udp::UDP_TOKEN_LEN..) {
                            Some(v) => v,
                            None => continue,
                        };
                        let msg: PopToAgent = match solanacdn_protocol::frame::decode_envelope(msg_bytes) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let PopToAgent::DirectShredsProbe { .. } = msg {
                            // Only learn/update egress IPs when direct POP→validator injection is enabled
                            // for the current publisher session. This avoids letting non-publisher sessions
                            // expand the allowlist.
                            if cfg.direct_shreds_from_pop && *publisher_rx.borrow() == Some(endpoint) {
                                handle.note_pop_egress_ip(peer.ip());
                            }
                            continue;
                        }

                        let now = now_ms();
                        let peer_ip = peer.ip();
                        if peer_ip != endpoint.ip() && !handle.is_pop_egress_ip_fresh(peer_ip, now) {
                            handle
                                .dropped_udp_shreds_unexpected_peer
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        match msg {
                            PopToAgent::PushShredFecChunk(chunk) => {
                                if !cfg.inject_shreds {
                                    continue;
                                }
                                if chunk.packet.len() > 2048 {
                                    continue;
                                }

                                if !fec.contains_key(&chunk.object_id) && fec.len() >= FEC_MAX_OBJECTS {
                                    continue;
                                }
                                let entry = match fec.entry(chunk.object_id) {
                                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                                        entry.get_mut().1 = now;
                                        entry.into_mut()
                                    }
                                    std::collections::hash_map::Entry::Vacant(entry) => {
                                        let dec = match solanacdn_protocol::fec::RaptorqDecoder::new(chunk.oti) {
                                            Ok(v) => v,
                                            Err(_) => continue,
                                        };
                                        entry.insert((dec, now))
                                    }
                                };
                                if let Some(bytes) = entry.0.push_packet(&chunk.packet) {
                                    fec.remove(&chunk.object_id);
                                    if bytes.len() > SHREDS_MAX_FRAME_BYTES {
                                        continue;
                                    }
                                    let decoded: PopToAgent = match solanacdn_protocol::frame::decode_envelope(&bytes) {
                                        Ok(v) => v,
                                        Err(_) => continue,
                                    };
                                    if !matches!(decoded, PopToAgent::PushShredBatch(_)) {
                                        handle
                                            .dropped_udp_shreds_unexpected_msg
                                            .fetch_add(1, Ordering::Relaxed);
                                        warn!("solanacdn: protocol violation from {endpoint}: unexpected msg type after FEC decode; closing session");
                                        conn.close(
                                            quinn::VarInt::from_u32(
                                                SCDN_PROTOCOL_VIOLATION_CLOSE_CODE,
                                            ),
                                            b"unexpected msg on udp shreds port (fec)",
                                        );
                                        return;
                                    }
                                    handle_pop_msg(
                                        endpoint,
                                        pop_pubkey,
                                        &cfg,
                                        auth.as_ref(),
                                        &handle,
                                        &ctrl_out_tx,
                                        &mut publisher_rx,
                                        &shred_deduper,
                                        &udp_inject_tpu,
                                        &udp_inject_tvu,
                                        &udp_inject_gossip,
                                        inject_vote,
                                        &udp_inject_votes,
                                        &session_events_tx,
                                        &last_hb_sent_ms,
                                        decoded,
                                    )
                                    .await;
                                }
                            }
                            other @ PopToAgent::PushShredBatch(_) => {
                                handle_pop_msg(
                                    endpoint,
                                    pop_pubkey,
                                    &cfg,
                                    auth.as_ref(),
                                    &handle,
                                    &ctrl_out_tx,
                                    &mut publisher_rx,
                                    &shred_deduper,
                                    &udp_inject_tpu,
                                    &udp_inject_tvu,
                                    &udp_inject_gossip,
                                    inject_vote,
                                    &udp_inject_votes,
                                    &session_events_tx,
                                    &last_hb_sent_ms,
                                    other,
                                )
                                .await;
                            }
                            _ => {
                                handle
                                    .dropped_udp_shreds_unexpected_msg
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!("solanacdn: protocol violation from {endpoint}: unexpected msg type on udp shreds port; closing session");
                                conn.close(
                                    quinn::VarInt::from_u32(SCDN_PROTOCOL_VIOLATION_CLOSE_CODE),
                                    b"unexpected msg on udp shreds port",
                                );
                                return;
                            }
                        }

                        if now.saturating_sub(last_cleanup_ms) > 1_000 {
                            last_cleanup_ms = now;
                            let expire_before = now.saturating_sub(FEC_EXPIRE_MS);
                            fec.retain(|_, (_, last)| *last >= expire_before);
                        }
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        fec.clear();
                    }
                }
            }
        }))
    } else {
        None
    };

    // UDP votes downlink.
    let udp_votes_task = if let Some(sock) = udp_votes.clone() {
        let conn = conn.clone();
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        Some(tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                // Avoid spending CPU on votes UDP downlink when not the selected publisher.
                while *publisher_rx.borrow() != Some(endpoint) {
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = sock.recv_from(&mut buf) => {
                        let (len, peer) = match res {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        let bytes = &buf[..len];
                        if !bytes.starts_with(&udp_token) {
                            continue;
                        }
                        let msg_bytes = match bytes.get(solanacdn_protocol::udp::UDP_TOKEN_LEN..) {
                            Some(v) => v,
                            None => continue,
                        };
                        let msg: PopToAgent = match solanacdn_protocol::frame::decode_envelope(msg_bytes) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let now = now_ms();
                        let peer_ip = peer.ip();
                        if peer_ip != endpoint.ip() && !handle.is_pop_egress_ip_fresh(peer_ip, now) {
                            handle
                                .dropped_udp_votes_unexpected_peer
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if !matches!(msg, PopToAgent::PushVoteDatagram(_)) {
                            handle
                                .dropped_udp_votes_unexpected_msg
                                .fetch_add(1, Ordering::Relaxed);
                            warn!("solanacdn: protocol violation from {endpoint}: unexpected msg type on udp votes port; closing session");
                            conn.close(
                                quinn::VarInt::from_u32(SCDN_PROTOCOL_VIOLATION_CLOSE_CODE),
                                b"unexpected msg on udp votes port",
                            );
                            return;
                        }
                        handle_pop_msg(
                            endpoint,
                            pop_pubkey,
                            &cfg,
                            auth.as_ref(),
                            &handle,
                            &ctrl_out_tx,
                            &mut publisher_rx,
                            &shred_deduper,
                            &udp_inject_tpu,
                            &udp_inject_tvu,
                            &udp_inject_gossip,
                            inject_vote,
                            &udp_inject_votes,
                            &session_events_tx,
                            &last_hb_sent_ms,
                            msg,
                        )
                        .await;
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    // Main loop: exit when connection closes or stop requested.
    let conn_closed = conn.closed();
    tokio::pin!(conn_closed);
    loop {
        tokio::select! {
            _ = &mut conn_closed => break,
            _ = stop_rx.changed() => break,
            msg = uplink_rx.recv() => {
                let Some(msg) = msg else { break; };
                if *publisher_rx.borrow() != Some(endpoint) {
                    continue;
                }
                match msg {
                    UplinkMsg::Shred(shred) => {
                        if !cfg.publish_shreds || shred.payload.is_empty() {
                            continue;
                        }
                        let batch = make_single_shred_batch(shred.kind, shred.payload);
                        let msg = AgentToPop::PublishShredBatch(batch);

                        if cfg.udp_mode != DataPlaneMode::Off {
                            if let Some(sock) = udp_shreds.as_ref() {
                                if let Ok(bytes) =
                                    solanacdn_protocol::udp::encode_udp_datagram(udp_token, &msg)
                                {
                                    if sock.send_to(&bytes, pop_shreds_addr).await.is_ok() {
                                        handle
                                            .published_shred_batches
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                }
                            }
                        }

                        let _ = shreds_out_tx.send(msg).await;
                        handle.published_shred_batches.fetch_add(1, Ordering::Relaxed);
                    }
                    UplinkMsg::Vote(vote) => {
                        if !cfg.vote_tunnel || vote.payload.is_empty() {
                            continue;
                        }
                        handle.note_vote_tunnel_allowed_dst(vote.dst, now_ms());
                        let dg = VoteDatagram {
                            flow_id: vote_flow_id(&vote.dst),
                            src: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                            dst: vote.dst,
                            payload: vote.payload.to_vec(),
                        };
                        let msg = AgentToPop::PublishVoteDatagram(dg);

                        if cfg.udp_mode != DataPlaneMode::Off {
                            if let Some(sock) = udp_votes.as_ref() {
                                if let Ok(bytes) =
                                    solanacdn_protocol::udp::encode_udp_datagram(udp_token, &msg)
                                {
                                    if sock.send_to(&bytes, pop_votes_addr).await.is_ok() {
                                        handle
                                            .tunneled_vote_packets
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                }
                            }
                        }

                        let _ = votes_out_tx.send(msg).await;
                        handle.tunneled_vote_packets.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    let _ = session_events_tx.send(SessionEvent::Disconnected { endpoint });

    ctrl_writer_task.abort();
    shreds_writer_task.abort();
    votes_writer_task.abort();
    hb_task.abort();
    if let Some(t) = auth_refresh_task {
        t.abort();
    }
    publisher_task.abort();
    ctrl_reader_task.abort();
    shreds_reader_task.abort();
    votes_reader_task.abort();
    if let Some(t) = udp_shreds_task {
        t.abort();
    }
    if let Some(t) = udp_votes_task {
        t.abort();
    }

    Ok(())
}

async fn handle_pop_msg(
    endpoint: SocketAddr,
    pop_pubkey: PubkeyBytes,
    cfg: &SolanaCdnConfig,
    auth: &AuthContext,
    handle: &SolanaCdnHandle,
    ctrl_out_tx: &mpsc::Sender<AgentToPop>,
    publisher_rx: &mut watch::Receiver<Option<SocketAddr>>,
    shred_deduper: &ShredBatchDeduper,
    udp_inject_tpu: &UdpSocket,
    udp_inject_tvu: &UdpSocket,
    udp_inject_gossip: &UdpSocket,
    inject_vote: SocketAddr,
    udp_inject_votes: &VoteInjectSockets,
    session_events_tx: &mpsc::UnboundedSender<SessionEvent>,
    last_hb_sent_ms: &AtomicU64,
    msg: PopToAgent,
) {
    match msg {
        PopToAgent::HeartbeatAck(_ack) => {
            let sent_at = last_hb_sent_ms.load(Ordering::Relaxed);
            if sent_at != 0 {
                let rtt_ms = now_ms().saturating_sub(sent_at);
                let _ = session_events_tx.send(SessionEvent::RttSample { endpoint, rtt_ms });
            }
        }
        PopToAgent::PushShredBatch(batch) => {
            if !cfg.inject_shreds {
                return;
            }
            if *publisher_rx.borrow() != Some(endpoint) {
                return;
            }
            if batch.shreds.len() > PUSH_SHRED_BATCH_MAX_SHREDS {
                handle
                    .dropped_shred_batches_oversized
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if !shred_deduper.insert_if_new(batch.batch_id) {
                return;
            }
            handle.pushed_shred_batches.fetch_add(1, Ordering::Relaxed);
            let mut rx_bytes: usize = 0;
            let mut rx_shreds: u64 = 0;
            let mut max_slot: Option<u64> = None;
            for shred in &batch.shreds {
                if shred.payload.is_empty() {
                    continue;
                }
                if shred.payload.len() > PACKET_DATA_SIZE {
                    continue;
                }
                if handle.race_enabled() {
                    if let Some(shred_id) =
                        solana_ledger::shred::layout::get_shred_id(shred.payload.as_slice())
                    {
                        handle.note_race_observation_from_pop(shred_id, endpoint);
                    }
                }
                rx_shreds = rx_shreds.saturating_add(1);
                rx_bytes = rx_bytes.saturating_add(shred.payload.len());
                max_slot = Some(max_slot.unwrap_or(0).max(shred.id.slot));
                let dst = match shred.kind {
                    ShredKind::Tvu => udp_inject_tvu,
                    ShredKind::Gossip => udp_inject_gossip,
                };
                let _ = dst.send(&shred.payload).await;
            }
            handle.note_solanacdn_shreds_rx(rx_bytes, rx_shreds, max_slot);
        }
        PopToAgent::PushVoteDatagram(dg) => {
            if !cfg.vote_tunnel {
                return;
            }
            if *publisher_rx.borrow() != Some(endpoint) {
                return;
            }
            handle.rx_vote_packets.fetch_add(1, Ordering::Relaxed);
            if dg.payload.is_empty() {
                return;
            }
            let now = now_ms();
            if dg.payload.len() > PACKET_DATA_SIZE {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                handle
                    .dropped_vote_datagrams_oversized_payload
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if dg.dst.port() != inject_vote.port() {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                handle
                    .dropped_vote_datagrams_unexpected_dst
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            let vote_program_id = solana_vote_program::id().to_bytes();
            if !wire_tx_has_program_id(dg.payload.as_slice(), &vote_program_id) {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                handle
                    .dropped_vote_datagrams_invalid_payload
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if handle.should_dedup_vote_payload(inject_vote, &dg.payload, now) {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if udp_inject_votes
                .send_to(&dg.payload, inject_vote)
                .await
                .is_err()
            {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        PopToAgent::RelayTransaction(tx) => {
            handle.rx_tx_packets.fetch_add(1, Ordering::Relaxed);
            if tx.payload.is_empty() || tx.payload.len() > PACKET_DATA_SIZE {
                handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // Transactions may be routed via multiple POPs (home-POP forwarding, multi-POP, etc),
            // so do not gate this on the current shred/vote publisher selection.
            let now = now_ms();
            let Some(sig) = try_first_signature_bytes_from_wire_tx(tx.payload.as_slice()) else {
                handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
                return;
            };
            if handle.should_dedup_tx_sig(sig, now) {
                handle.tx_deduped_packets.fetch_add(1, Ordering::Relaxed);
                return;
            }
            handle.note_dedup_tx_sig(sig, now);
            if udp_inject_tpu.send(&tx.payload).await.is_ok() {
                handle.tx_injected_packets.fetch_add(1, Ordering::Relaxed);
            } else {
                handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        PopToAgent::AuthError(err) => {
            debug!(
                "solanacdn: server auth error from {endpoint}: {}: {}",
                err.code, err.message
            );
        }
        _ => {}
    }
}

fn make_single_shred_batch(kind: ShredKind, payload: Bytes) -> ShredBatch {
    let created_at_ms = now_ms();
    let items = &[(kind, payload.clone())];
    let batch_id = compute_shred_batch_id(items);
    let shreds = vec![Shred {
        id: ShredId { slot: 0, index: 0 },
        kind,
        payload: payload.to_vec(),
    }];
    ShredBatch {
        batch_id,
        created_at_ms,
        shreds,
    }
}
