//! Maintains a connection to the Block Engine.
//!
//! The Block Engine is responsible for the following:
//! - Acts as a system that sends high profit bundles and transactions to a validator.
//! - Sends transactions and bundles to the validator.

use {
    crate::{
        backoff::BackoffStrategy,
        packet_bundle::PacketBundle,
        proto_packet_to_packet,
        proxy::{
            auth::{token_manager::auth_tokens_update_loop, AuthInterceptor},
            ProxyError,
        },
        sigverify::SigverifyTracerPacketStats,
    },
    crossbeam_channel::Sender,
    jito_protos::proto::{
        auth::Token,
        block_engine::{
            self, block_engine_validator_client::BlockEngineValidatorClient,
            BlockBuilderFeeInfoRequest,
        },
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_perf::packet::PacketBatch,
    solana_sdk::{pubkey::Pubkey, saturating_add_assign},
    std::{
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    tokio::time::{interval, sleep},
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
        Status, Streaming,
    },
    uuid::Uuid,
};

#[derive(Default)]
struct BlockEngineStageStats {
    num_bundles: u64,
    num_bundle_packets: u64,
    num_packets: u64,
    num_empty_packets: u64,
}

impl BlockEngineStageStats {
    pub(crate) fn report(&self) {
        datapoint_info!(
            "block_engine_stage-stats",
            ("num_bundles", self.num_bundles, i64),
            ("num_bundle_packets", self.num_bundle_packets, i64),
            ("num_packets", self.num_packets, i64),
            ("num_empty_packets", self.num_empty_packets, i64)
        );
    }
}

pub struct BlockBuilderFeeInfo {
    pub block_builder: Pubkey,
    pub block_builder_commission: u64,
}

#[derive(Clone, Debug)]
pub struct BlockEngineConfig {
    /// Address to the external auth-service responsible for generating access tokens.
    pub auth_service_endpoint: Endpoint,

    /// Primary backend endpoint.
    pub backend_endpoint: Endpoint,

    /// If set then it will be assumed the backend verified packets so signature verification will be bypassed in the validator.
    pub trust_packets: bool,
}

pub struct BlockEngineStage {
    t_hdls: Vec<JoinHandle<()>>,
}

impl BlockEngineStage {
    pub fn new(
        block_engine_config: BlockEngineConfig,
        // Channel that bundles get piped through.
        bundle_tx: Sender<Vec<PacketBundle>>,
        // The keypair stored here is used to sign auth challenges.
        cluster_info: Arc<ClusterInfo>,
        // Channel that non-trusted packets get piped through.
        packet_tx: Sender<PacketBatch>,
        // Channel that trusted packets get piped through.
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) -> Self {
        let BlockEngineConfig {
            auth_service_endpoint,
            backend_endpoint,
            trust_packets,
        } = block_engine_config;

        let access_token = Arc::new(Mutex::new(Token::default()));
        let block_builder_fee_info = block_builder_fee_info.clone();

        let thread = Builder::new()
            .name("block-engine-stage".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.spawn(auth_tokens_update_loop(
                    auth_service_endpoint,
                    access_token.clone(),
                    cluster_info.clone(),
                    exit.clone(),
                ));
                rt.block_on(Self::start(
                    access_token,
                    backend_endpoint,
                    bundle_tx,
                    packet_tx,
                    trust_packets,
                    verified_packet_tx,
                    exit,
                    block_builder_fee_info,
                ));
            })
            .unwrap();

        Self {
            t_hdls: vec![thread],
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.t_hdls {
            t.join()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn start(
        access_token: Arc<Mutex<Token>>,
        block_engine_endpoint: Endpoint,
        bundle_tx: Sender<Vec<PacketBundle>>,
        packet_tx: Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
    ) {
        const WAIT_FOR_FIRST_AUTH: Duration = Duration::from_secs(5);

        let mut num_wait_for_auth: usize = 0;
        let mut num_stream_errors: usize = 0;
        let mut num_connect_errors: usize = 0;

        while access_token.lock().unwrap().value.is_empty() {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            num_wait_for_auth += 1;
            datapoint_info!(
                "block_engine_stage-wait_for_auth",
                ("wait_count", num_wait_for_auth, i64)
            );
            sleep(WAIT_FOR_FIRST_AUTH).await;
        }

        let mut backoff = BackoffStrategy::new();
        while !exit.load(Ordering::Relaxed) {
            match block_engine_endpoint.connect().await {
                Ok(channel) => {
                    match Self::start_consuming_block_engine_bundles_and_packets(
                        &mut backoff,
                        &bundle_tx,
                        BlockEngineValidatorClient::with_interceptor(
                            channel,
                            AuthInterceptor::new(access_token.clone()),
                        ),
                        &packet_tx,
                        trust_packets,
                        &verified_packet_tx,
                        &exit,
                        &block_builder_fee_info,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            num_stream_errors += 1;
                            datapoint_error!(
                                "block_engine_stage-stream_error",
                                ("count", num_stream_errors, i64),
                                ("error", e.to_string(), String),
                            );
                        }
                    }
                }
                Err(e) => {
                    num_connect_errors += 1;
                    datapoint_error!(
                        "block_engine_stage-connect_error",
                        ("count", num_connect_errors, i64),
                        ("error", e.to_string(), String),
                    );
                }
            }

            sleep(Duration::from_millis(backoff.next_wait())).await;
        }
    }

    async fn start_consuming_block_engine_bundles_and_packets(
        backoff: &mut BackoffStrategy,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) -> crate::proxy::Result<()> {
        let subscribe_packets_stream = client
            .subscribe_packets(block_engine::SubscribePacketsRequest {})
            .await?
            .into_inner();
        let subscribe_bundles_stream = client
            .subscribe_bundles(block_engine::SubscribeBundlesRequest {})
            .await?
            .into_inner();

        let block_builder_info = client
            .get_block_builder_fee_info(BlockBuilderFeeInfoRequest {})
            .await?
            .into_inner();
        {
            let mut bb_fee = block_builder_fee_info.lock().unwrap();
            bb_fee.block_builder_commission = block_builder_info.commission;
            bb_fee.block_builder =
                Pubkey::from_str(&block_builder_info.pubkey).unwrap_or(bb_fee.block_builder);
        }

        backoff.reset();

        Self::consume_bundle_and_packet_stream(
            client,
            (subscribe_bundles_stream, subscribe_packets_stream),
            bundle_tx,
            packet_tx,
            trust_packets,
            verified_packet_tx,
            exit,
            block_builder_fee_info,
        )
        .await
    }

    async fn consume_bundle_and_packet_stream(
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        (mut bundle_stream, mut packet_stream): (
            Streaming<block_engine::SubscribeBundlesResponse>,
            Streaming<block_engine::SubscribePacketsResponse>,
        ),
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) -> crate::proxy::Result<()> {
        const METRICS_TICK: Duration = Duration::from_secs(1);
        const MAINTENANCE_TICK: Duration = Duration::from_secs(10 * 60);

        let mut block_engine_stats = BlockEngineStageStats::default();
        let mut metrics_tick = interval(METRICS_TICK);
        let mut maintenance_tick = interval(MAINTENANCE_TICK);

        info!("connected to packet and bundle stream");

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_block_engine_packets(resp, packet_tx, verified_packet_tx, trust_packets, &mut block_engine_stats)?;
                }
                maybe_bundles = bundle_stream.message() => {
                    Self::handle_block_engine_maybe_bundles(maybe_bundles, bundle_tx, &mut block_engine_stats)?;
                }
                _ = metrics_tick.tick() => {
                    block_engine_stats.report();
                    block_engine_stats = BlockEngineStageStats::default();
                }
                _ = maintenance_tick.tick() => {
                    let block_builder_info = client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest{}).await?.into_inner();
                    let mut bb_fee = block_builder_fee_info.lock().unwrap();
                    bb_fee.block_builder_commission = block_builder_info.commission;
                    bb_fee.block_builder = Pubkey::from_str(&block_builder_info.pubkey).unwrap_or(bb_fee.block_builder);
                }
            }
        }

        Ok(())
    }

    fn handle_block_engine_maybe_bundles(
        maybe_bundles_response: Result<Option<block_engine::SubscribeBundlesResponse>, Status>,
        bundle_sender: &Sender<Vec<PacketBundle>>,
        block_engine_stats: &mut BlockEngineStageStats,
    ) -> crate::proxy::Result<()> {
        let bundles_response = maybe_bundles_response?.ok_or(ProxyError::GrpcStreamDisconnected)?;
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .filter_map(|bundle| {
                Some(PacketBundle {
                    batch: PacketBatch::new(
                        bundle
                            .bundle?
                            .packets
                            .into_iter()
                            .map(proto_packet_to_packet)
                            .collect(),
                    ),
                    uuid: Uuid::from_str(&bundle.uuid).ok()?,
                })
            })
            .collect();

        saturating_add_assign!(block_engine_stats.num_bundles, bundles.len() as u64);
        saturating_add_assign!(
            block_engine_stats.num_bundle_packets,
            bundles.iter().map(|bundle| bundle.batch.len() as u64).sum()
        );

        // NOTE: bundles are sanitized in bundle_sanitizer module
        bundle_sender
            .send(bundles)
            .map_err(|_| ProxyError::PacketForwardError)
    }

    fn handle_block_engine_packets(
        resp: block_engine::SubscribePacketsResponse,
        packet_tx: &Sender<PacketBatch>,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        trust_packets: bool,
        block_engine_stats: &mut BlockEngineStageStats,
    ) -> crate::proxy::Result<()> {
        if let Some(batch) = resp.batch {
            if batch.packets.is_empty() {
                saturating_add_assign!(block_engine_stats.num_empty_packets, 1);
                return Ok(());
            }

            let packet_batch = PacketBatch::new(
                batch
                    .packets
                    .into_iter()
                    .map(proto_packet_to_packet)
                    .collect(),
            );

            saturating_add_assign!(block_engine_stats.num_packets, packet_batch.len() as u64);

            if trust_packets {
                verified_packet_tx
                    .send((vec![packet_batch], None))
                    .map_err(|_| ProxyError::PacketForwardError)?;
            } else {
                packet_tx
                    .send(packet_batch)
                    .map_err(|_| ProxyError::PacketForwardError)?;
            }
        } else {
            saturating_add_assign!(block_engine_stats.num_empty_packets, 1);
        }

        Ok(())
    }
}
