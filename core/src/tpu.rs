//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

use {
    crate::{
        banking_stage::BankingStage,
        broadcast_stage::{BroadcastStage, BroadcastStageType, RetransmitSlotsReceiver},
        bundle_account_locker::BundleAccountLocker,
        bundle_stage::BundleStage,
        cluster_info_vote_listener::{
            ClusterInfoVoteListener, GossipDuplicateConfirmedSlotsSender,
            GossipVerifiedVoteHashSender, VerifiedVoteSender, VoteTracker,
        },
        fetch_stage::FetchStage,
        find_packet_sender_stake_stage::FindPacketSenderStakeStage,
        relayer_stage::RelayerStage,
        sigverify::TransactionSigVerifier,
        sigverify_stage::SigVerifyStage,
        staked_nodes_updater_service::StakedNodesUpdaterService,
        tip_manager::TipManager,
    },
    crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError},
    solana_client::connection_cache::ConnectionCache,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{blockstore::Blockstore, blockstore_processor::TransactionStatusSender},
    solana_poh::poh_recorder::{PohRecorder, WorkingBankEntry},
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSender,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        bank_forks::BankForks,
        cost_model::CostModel,
        vote_sender_types::{ReplayVoteReceiver, ReplayVoteSender},
    },
    solana_sdk::{pubkey::Pubkey, signature::Keypair},
    solana_streamer::quic::{
        spawn_server, StreamStats, MAX_STAKED_CONNECTIONS, MAX_UNSTAKED_CONNECTIONS,
    },
    std::{
        collections::HashMap,
        net::{SocketAddr, UdpSocket},
        sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
        thread,
        time::Duration,
    },
};

pub const DEFAULT_TPU_COALESCE_MS: u64 = 5;

/// Timeout interval when joining threads during TPU close
const TPU_THREADS_JOIN_TIMEOUT_SECONDS: u64 = 10;

// allow multiple connections for NAT and any open/close overlap
pub const MAX_QUIC_CONNECTIONS_PER_IP: usize = 8;

const NUM_BUNDLES_PRE_LOCK: u64 = 4;

pub struct TpuSockets {
    pub transactions: Vec<UdpSocket>,
    pub transaction_forwards: Vec<UdpSocket>,
    pub vote: Vec<UdpSocket>,
    pub broadcast: Vec<UdpSocket>,
    pub transactions_quic: UdpSocket,
    pub transactions_forwards_quic: UdpSocket,
}

pub struct Tpu {
    fetch_stage: FetchStage,
    sigverify_stage: SigVerifyStage,
    vote_sigverify_stage: SigVerifyStage,
    mev_stage: RelayerStage,
    banking_stage: BankingStage,
    cluster_info_vote_listener: ClusterInfoVoteListener,
    broadcast_stage: BroadcastStage,
    tpu_quic_t: Option<thread::JoinHandle<()>>,
    tpu_forwards_quic_t: Option<thread::JoinHandle<()>>,
    find_packet_sender_stake_stage: FindPacketSenderStakeStage,
    vote_find_packet_sender_stake_stage: FindPacketSenderStakeStage,
    staked_nodes_updater_service: StakedNodesUpdaterService,
    bundle_stage: BundleStage,
}

impl Tpu {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        entry_receiver: Receiver<WorkingBankEntry>,
        retransmit_slots_receiver: RetransmitSlotsReceiver,
        sockets: TpuSockets,
        subscriptions: &Arc<RpcSubscriptions>,
        transaction_status_sender: Option<TransactionStatusSender>,
        blockstore: &Arc<Blockstore>,
        broadcast_type: &BroadcastStageType,
        exit: &Arc<AtomicBool>,
        shred_version: u16,
        vote_tracker: Arc<VoteTracker>,
        bank_forks: Arc<RwLock<BankForks>>,
        verified_vote_sender: VerifiedVoteSender,
        gossip_verified_vote_hash_sender: GossipVerifiedVoteHashSender,
        replay_vote_receiver: ReplayVoteReceiver,
        replay_vote_sender: ReplayVoteSender,
        bank_notification_sender: Option<BankNotificationSender>,
        tpu_coalesce_ms: u64,
        cluster_confirmed_slot_sender: GossipDuplicateConfirmedSlotsSender,
        cost_model: &Arc<RwLock<CostModel>>,
        connection_cache: &Arc<ConnectionCache>,
        keypair: &Keypair,
        enable_quic_servers: bool,
        relayer_address: String,
        block_engine_address: String,
        tip_program_pubkey: Pubkey,
        shred_receiver_address: Option<SocketAddr>,
    ) -> Self {
        let TpuSockets {
            transactions: transactions_sockets,
            transaction_forwards: tpu_forwards_sockets,
            vote: tpu_vote_sockets,
            broadcast: broadcast_sockets,
            transactions_quic: transactions_quic_sockets,
            transactions_forwards_quic: transactions_forwards_quic_sockets,
        } = sockets;

        let (packet_intercept_sender, packet_intercept_receiver) = unbounded();
        let (packet_sender, packet_receiver) = unbounded();
        let (vote_packet_sender, vote_packet_receiver) = unbounded();
        let (forwarded_packet_sender, forwarded_packet_receiver) = unbounded();
        let fetch_stage = FetchStage::new_with_sender(
            transactions_sockets,
            tpu_forwards_sockets,
            tpu_vote_sockets,
            exit,
            &packet_intercept_sender,
            &vote_packet_sender,
            &forwarded_packet_sender,
            forwarded_packet_receiver,
            poh_recorder,
            tpu_coalesce_ms,
            Some(bank_forks.read().unwrap().get_vote_only_mode_signal()),
        );

        let staked_nodes = Arc::new(RwLock::new(HashMap::new()));
        let staked_nodes_updater_service = StakedNodesUpdaterService::new(
            exit.clone(),
            cluster_info.clone(),
            bank_forks.clone(),
            staked_nodes.clone(),
        );

        let (find_packet_sender_stake_sender, find_packet_sender_stake_receiver) = unbounded();

        let find_packet_sender_stake_stage = FindPacketSenderStakeStage::new(
            packet_receiver,
            find_packet_sender_stake_sender,
            staked_nodes.clone(),
            "tpu-find-packet-sender-stake",
        );

        let (vote_find_packet_sender_stake_sender, vote_find_packet_sender_stake_receiver) =
            unbounded();

        let vote_find_packet_sender_stake_stage = FindPacketSenderStakeStage::new(
            vote_packet_receiver,
            vote_find_packet_sender_stake_sender,
            staked_nodes.clone(),
            "tpu-vote-find-packet-sender-stake",
        );

        let (verified_sender, verified_receiver) = unbounded();

        let stats = Arc::new(StreamStats::default());
        let tpu_quic_t = enable_quic_servers.then(|| {
            spawn_server(
                transactions_quic_sockets,
                keypair,
                cluster_info.my_contact_info().tpu.ip(),
                packet_intercept_sender,
                exit.clone(),
                MAX_QUIC_CONNECTIONS_PER_IP,
                staked_nodes.clone(),
                MAX_STAKED_CONNECTIONS,
                MAX_UNSTAKED_CONNECTIONS,
                stats.clone(),
            )
            .unwrap()
        });

        let tpu_forwards_quic_t = enable_quic_servers.then(|| {
            spawn_server(
                transactions_forwards_quic_sockets,
                keypair,
                cluster_info.my_contact_info().tpu_forwards.ip(),
                forwarded_packet_sender,
                exit.clone(),
                MAX_QUIC_CONNECTIONS_PER_IP,
                staked_nodes,
                MAX_STAKED_CONNECTIONS.saturating_add(MAX_UNSTAKED_CONNECTIONS),
                0, // Prevent unstaked nodes from forwarding transactions
                stats,
            )
            .unwrap()
        });

        let sigverify_stage = {
            let verifier = TransactionSigVerifier::new(verified_sender.clone());
            SigVerifyStage::new(find_packet_sender_stake_receiver, verifier, "tpu-verifier")
        };

        let (verified_tpu_vote_packets_sender, verified_tpu_vote_packets_receiver) = unbounded();

        let vote_sigverify_stage = {
            let verifier =
                TransactionSigVerifier::new_reject_non_vote(verified_tpu_vote_packets_sender);
            SigVerifyStage::new(
                vote_find_packet_sender_stake_receiver,
                verifier,
                "tpu-vote-verifier",
            )
        };

        let (bundle_sender, bundle_receiver) = unbounded();

        let mev_stage = RelayerStage::new(
            cluster_info,
            relayer_address,
            block_engine_address,
            verified_sender,
            bundle_sender,
            packet_intercept_receiver,
            packet_sender,
            exit.clone(),
        );

        let (verified_gossip_vote_packets_sender, verified_gossip_vote_packets_receiver) =
            unbounded();
        let cluster_info_vote_listener = ClusterInfoVoteListener::new(
            exit.clone(),
            cluster_info.clone(),
            verified_gossip_vote_packets_sender,
            poh_recorder.clone(),
            vote_tracker,
            bank_forks.clone(),
            subscriptions.clone(),
            verified_vote_sender,
            gossip_verified_vote_hash_sender,
            replay_vote_receiver,
            blockstore.clone(),
            bank_notification_sender,
            cluster_confirmed_slot_sender,
        );

        let tip_manager = TipManager::new(tip_program_pubkey);

        let bundle_account_locker =
            Arc::new(Mutex::new(BundleAccountLocker::new(NUM_BUNDLES_PRE_LOCK)));

        // tip accounts can't be used in BankingStage. This makes handling race conditions
        // for tip-related things in BundleStage easier.
        // TODO (LB): once there's a unified scheduler, we should allow tips in BankingStage
        //  and treat them w/ a priority similar to ComputeBudget::SetComputeUnitPrice
        let mut tip_accounts = tip_manager.get_tip_accounts();
        tip_accounts.insert(tip_manager.config_pubkey());
        tip_accounts.insert(tip_manager.program_id());

        let banking_stage = BankingStage::new(
            cluster_info,
            poh_recorder,
            verified_receiver,
            verified_tpu_vote_packets_receiver,
            verified_gossip_vote_packets_receiver,
            transaction_status_sender.clone(),
            replay_vote_sender.clone(),
            cost_model.clone(),
            connection_cache.clone(),
            tip_accounts,
            bundle_account_locker.clone(),
        );

        let bundle_stage = BundleStage::new(
            cluster_info,
            poh_recorder,
            transaction_status_sender,
            replay_vote_sender,
            cost_model.clone(),
            bundle_receiver,
            exit.clone(),
            tip_manager,
            bundle_account_locker,
        );

        let broadcast_stage = broadcast_type.new_broadcast_stage(
            broadcast_sockets,
            cluster_info.clone(),
            entry_receiver,
            retransmit_slots_receiver,
            exit.clone(),
            blockstore.clone(),
            bank_forks,
            shred_version,
            shred_receiver_address,
        );

        Self {
            fetch_stage,
            sigverify_stage,
            vote_sigverify_stage,
            mev_stage,
            banking_stage,
            cluster_info_vote_listener,
            broadcast_stage,
            tpu_quic_t,
            tpu_forwards_quic_t,
            find_packet_sender_stake_stage,
            vote_find_packet_sender_stake_stage,
            staked_nodes_updater_service,
            bundle_stage,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        // spawn a new thread to wait for tpu close
        let (sender, receiver) = bounded(0);
        let _ = thread::spawn(move || {
            let _ = self.do_join();
            sender.send(()).unwrap();
        });

        // exit can deadlock. put an upper-bound on how long we wait for it
        let timeout = Duration::from_secs(TPU_THREADS_JOIN_TIMEOUT_SECONDS);
        if let Err(RecvTimeoutError::Timeout) = receiver.recv_timeout(timeout) {
            error!("timeout for closing tpu");
        }
        Ok(())
    }

    fn do_join(self) -> thread::Result<()> {
        let results = vec![
            self.fetch_stage.join(),
            self.sigverify_stage.join(),
            self.vote_sigverify_stage.join(),
            self.cluster_info_vote_listener.join(),
            self.banking_stage.join(),
            self.find_packet_sender_stake_stage.join(),
            self.vote_find_packet_sender_stake_stage.join(),
            self.staked_nodes_updater_service.join(),
            self.mev_stage.join(),
            self.bundle_stage.join(),
        ];
        if let Some(tpu_quic_t) = self.tpu_quic_t {
            tpu_quic_t.join()?;
        }
        if let Some(tpu_forwards_quic_t) = self.tpu_forwards_quic_t {
            tpu_forwards_quic_t.join()?;
        }
        let broadcast_result = self.broadcast_stage.join();
        for result in results {
            result?;
        }
        let _ = broadcast_result?;
        Ok(())
    }
}
