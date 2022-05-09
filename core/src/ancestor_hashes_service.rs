use {
    crate::{
        cluster_slots::ClusterSlots,
        duplicate_repair_status::{DeadSlotAncestorRequestStatus, DuplicateAncestorDecision},
        outstanding_requests::OutstandingRequests,
        packet_threshold::DynamicPacketToProcessThreshold,
        repair_response::{self},
        repair_service::{DuplicateSlotsResetSender, RepairInfo, RepairStatsGroup},
        replay_stage::DUPLICATE_THRESHOLD,
        result::{Error, Result},
        serve_repair::{AncestorHashesRepairType, ServeRepair},
    },
    crossbeam_channel::{unbounded, Receiver, Sender},
    dashmap::{mapref::entry::Entry::Occupied, DashMap},
    solana_ledger::{blockstore::Blockstore, shred::SIZE_OF_NONCE},
    solana_perf::{
        packet::{limited_deserialize, Packet, PacketBatch},
        recycler::Recycler,
    },
    solana_runtime::bank::Bank,
    solana_sdk::{
        clock::{Slot, SLOT_MS},
        pubkey::Pubkey,
        timing::timestamp,
    },
    solana_streamer::streamer::{self, PacketBatchReceiver, StreamerReceiveStats},
    std::{
        collections::HashSet,
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

#[derive(Debug, PartialEq)]
pub enum AncestorHashesReplayUpdate {
    Dead(Slot),
    DeadDuplicateConfirmed(Slot),
}

impl AncestorHashesReplayUpdate {
    fn slot(&self) -> Slot {
        match self {
            AncestorHashesReplayUpdate::Dead(slot) => *slot,
            AncestorHashesReplayUpdate::DeadDuplicateConfirmed(slot) => *slot,
        }
    }
}

pub const MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND: usize = 2;

pub type AncestorHashesReplayUpdateSender = Sender<AncestorHashesReplayUpdate>;
pub type AncestorHashesReplayUpdateReceiver = Receiver<AncestorHashesReplayUpdate>;

type RetryableSlotsSender = Sender<Slot>;
type RetryableSlotsReceiver = Receiver<Slot>;
type OutstandingAncestorHashesRepairs = OutstandingRequests<AncestorHashesRepairType>;

#[derive(Default)]
pub struct AncestorHashesResponsesStats {
    pub total_packets: usize,
    pub dropped_packets: usize,
    pub invalid_packets: usize,
    pub processed: usize,
}

impl AncestorHashesResponsesStats {
    fn report(&mut self) {
        inc_new_counter_info!(
            "ancestor_hashes_responses-total_packets",
            self.total_packets
        );
        inc_new_counter_info!("ancestor_hashes_responses-processed", self.processed);
        inc_new_counter_info!(
            "ancestor_hashes_responses-dropped_packets",
            self.dropped_packets
        );
        inc_new_counter_info!(
            "ancestor_hashes_responses-invalid_packets",
            self.invalid_packets
        );
        *self = AncestorHashesResponsesStats::default();
    }
}

pub struct AncestorRepairRequestsStats {
    pub ancestor_requests: RepairStatsGroup,
    last_report: Instant,
}

impl Default for AncestorRepairRequestsStats {
    fn default() -> Self {
        AncestorRepairRequestsStats {
            ancestor_requests: RepairStatsGroup::default(),
            last_report: Instant::now(),
        }
    }
}

impl AncestorRepairRequestsStats {
    fn report(&mut self) {
        let slot_to_count: Vec<_> = self
            .ancestor_requests
            .slot_pubkeys
            .iter()
            .map(|(slot, slot_repairs)| {
                (
                    slot,
                    slot_repairs
                        .pubkey_repairs()
                        .iter()
                        .map(|(_key, count)| count)
                        .sum::<u64>(),
                )
            })
            .collect();

        let repair_total = self.ancestor_requests.count;
        if self.last_report.elapsed().as_secs() > 2 && repair_total > 0 {
            info!("ancestor_repair_requests_stats: {:?}", slot_to_count);
            datapoint_info!(
                "ancestor-repair",
                ("ancestor-repair-count", self.ancestor_requests.count, i64)
            );

            *self = AncestorRepairRequestsStats::default();
        }
    }
}

pub struct AncestorHashesService {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl AncestorHashesService {
    pub fn new(
        exit: Arc<AtomicBool>,
        blockstore: Arc<Blockstore>,
        ancestor_hashes_request_socket: Arc<UdpSocket>,
        repair_info: RepairInfo,
        ancestor_hashes_replay_update_receiver: AncestorHashesReplayUpdateReceiver,
    ) -> Self {
        let outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>> =
            Arc::new(RwLock::new(OutstandingAncestorHashesRepairs::default()));
        let (response_sender, response_receiver) = unbounded();
        let t_receiver = streamer::receiver(
            ancestor_hashes_request_socket.clone(),
            exit.clone(),
            response_sender,
            Recycler::default(),
            Arc::new(StreamerReceiveStats::new(
                "ancestor_hashes_response_receiver",
            )),
            1,
            false,
        );

        let ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>> =
            Arc::new(DashMap::new());
        let (retryable_slots_sender, retryable_slots_receiver) = unbounded();

        // Listen for responses to our ancestor requests
        let t_ancestor_hashes_responses = Self::run_responses_listener(
            ancestor_hashes_request_statuses.clone(),
            response_receiver,
            blockstore,
            outstanding_requests.clone(),
            exit.clone(),
            repair_info.duplicate_slots_reset_sender.clone(),
            retryable_slots_sender,
        );

        // Generate ancestor requests for dead slots that are repairable
        let t_ancestor_requests = Self::run_manage_ancestor_requests(
            ancestor_hashes_request_statuses,
            ancestor_hashes_request_socket,
            repair_info,
            outstanding_requests,
            exit,
            ancestor_hashes_replay_update_receiver,
            retryable_slots_receiver,
        );
        let thread_hdls = vec![t_receiver, t_ancestor_hashes_responses, t_ancestor_requests];
        Self { thread_hdls }
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }

    /// Listen for responses to our ancestors hashes repair requests
    fn run_responses_listener(
        ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>>,
        response_receiver: PacketBatchReceiver,
        blockstore: Arc<Blockstore>,
        outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>>,
        exit: Arc<AtomicBool>,
        duplicate_slots_reset_sender: DuplicateSlotsResetSender,
        retryable_slots_sender: RetryableSlotsSender,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("solana-ancestor-hashes-responses-service".to_string())
            .spawn(move || {
                let mut last_stats_report = Instant::now();
                let mut stats = AncestorHashesResponsesStats::default();
                let mut packet_threshold = DynamicPacketToProcessThreshold::default();
                loop {
                    let result = Self::process_new_packets_from_channel(
                        &ancestor_hashes_request_statuses,
                        &response_receiver,
                        &blockstore,
                        &outstanding_requests,
                        &mut stats,
                        &mut packet_threshold,
                        &duplicate_slots_reset_sender,
                        &retryable_slots_sender,
                    );
                    match result {
                        Err(Error::RecvTimeout(_)) | Ok(_) => {}
                        Err(err) => info!("ancestors hashes responses listener error: {:?}", err),
                    };
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    if last_stats_report.elapsed().as_secs() > 2 {
                        stats.report();
                        last_stats_report = Instant::now();
                    }
                }
            })
            .unwrap()
    }

    /// Process messages from the network
    fn process_new_packets_from_channel(
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        response_receiver: &PacketBatchReceiver,
        blockstore: &Blockstore,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
        stats: &mut AncestorHashesResponsesStats,
        packet_threshold: &mut DynamicPacketToProcessThreshold,
        duplicate_slots_reset_sender: &DuplicateSlotsResetSender,
        retryable_slots_sender: &RetryableSlotsSender,
    ) -> Result<()> {
        let timeout = Duration::new(1, 0);
        let mut packet_batches = vec![response_receiver.recv_timeout(timeout)?];
        let mut total_packets = packet_batches[0].packets.len();

        let mut dropped_packets = 0;
        while let Ok(batch) = response_receiver.try_recv() {
            total_packets += batch.packets.len();
            if packet_threshold.should_drop(total_packets) {
                dropped_packets += batch.packets.len();
            } else {
                packet_batches.push(batch);
            }
        }

        stats.dropped_packets += dropped_packets;
        stats.total_packets += total_packets;

        let timer = Instant::now();
        for packet_batch in packet_batches {
            Self::process_packet_batch(
                ancestor_hashes_request_statuses,
                packet_batch,
                stats,
                outstanding_requests,
                blockstore,
                duplicate_slots_reset_sender,
                retryable_slots_sender,
            );
        }
        packet_threshold.update(total_packets, timer.elapsed());
        Ok(())
    }

    fn process_packet_batch(
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        packet_batch: PacketBatch,
        stats: &mut AncestorHashesResponsesStats,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
        blockstore: &Blockstore,
        duplicate_slots_reset_sender: &DuplicateSlotsResetSender,
        retryable_slots_sender: &RetryableSlotsSender,
    ) {
        packet_batch.packets.iter().for_each(|packet| {
            let decision = Self::verify_and_process_ancestor_response(
                packet,
                ancestor_hashes_request_statuses,
                stats,
                outstanding_requests,
                blockstore,
            );
            if let Some((slot, decision)) = decision {
                Self::handle_ancestor_request_decision(
                    slot,
                    decision,
                    duplicate_slots_reset_sender,
                    retryable_slots_sender,
                );
            }
        });
    }

    /// Returns `Some((request_slot, decision))`, where `decision` is an actionable
    /// result after processing sufficient responses for the subject of the query,
    /// `request_slot`
    fn verify_and_process_ancestor_response(
        packet: &Packet,
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        stats: &mut AncestorHashesResponsesStats,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
        blockstore: &Blockstore,
    ) -> Option<(Slot, DuplicateAncestorDecision)> {
        let from_addr = packet.meta.addr();
        limited_deserialize(&packet.data[..packet.meta.size.saturating_sub(SIZE_OF_NONCE)])
            .ok()
            .and_then(|ancestor_hashes_response| {
                // Verify the response
                let request_slot = repair_response::nonce(&packet.data[..packet.meta.size])
                    .and_then(|nonce| {
                        outstanding_requests.write().unwrap().register_response(
                            nonce,
                            &ancestor_hashes_response,
                            timestamp(),
                            // If the response is valid, return the slot the request
                            // was for
                            |ancestor_hashes_request| ancestor_hashes_request.0,
                        )
                    });

                if request_slot.is_none() {
                    stats.invalid_packets += 1;
                    return None;
                }

                // If was a valid response, there must be a valid `request_slot`
                let request_slot = request_slot.unwrap();
                stats.processed += 1;

                if let Occupied(mut ancestor_hashes_status_ref) =
                    ancestor_hashes_request_statuses.entry(request_slot)
                {
                    let decision = ancestor_hashes_status_ref.get_mut().add_response(
                        &from_addr,
                        ancestor_hashes_response.into_slot_hashes(),
                        blockstore,
                    );
                    if decision.is_some() {
                        // Once a request is completed, remove it from the map so that new
                        // requests for the same slot can be made again if necessary. It's
                        // important to hold the `write` lock here via
                        // `ancestor_hashes_status_ref` so that we don't race with deletion +
                        // insertion from the `t_ancestor_requests` thread, which may
                        // 1) Remove expired statuses from `ancestor_hashes_request_statuses`
                        // 2) Insert another new one via `manage_ancestor_requests()`.
                        // In which case we wouldn't want to delete the newly inserted entry here.
                        ancestor_hashes_status_ref.remove();
                    }
                    decision.map(|decision| (request_slot, decision))
                } else {
                    None
                }
            })
    }

    fn handle_ancestor_request_decision(
        slot: Slot,
        decision: DuplicateAncestorDecision,
        duplicate_slots_reset_sender: &DuplicateSlotsResetSender,
        retryable_slots_sender: &RetryableSlotsSender,
    ) {
        if decision.is_retryable() {
            let _ = retryable_slots_sender.send(slot);
        }
        let potential_slots_to_dump = {
            // TODO: In the case of DuplicateAncestorDecision::ContinueSearch
            // This means all the ancestors were mismatched, which
            // means the earliest mismatched ancestor has yet to be found.
            //
            // In the best case scenario, this means after ReplayStage dumps
            // the earliest known ancestor `A` here, and then repairs `A`,
            // because we may still have the incorrect version of some ancestor
            // of `A`, we will mark `A` as dead and then continue the search
            // protocol through another round of ancestor repairs.
            //
            // However this process is a bit slow, so in an ideal world, the
            // protocol could be extended to keep searching by making
            // another ancestor repair request from the earliest returned
            // ancestor from this search.
            decision
                .repair_status()
                .map(|status| status.correct_ancestors_to_repair.clone())
        };

        // Now signal ReplayStage about the new updated slots. It's important to do this
        // AFTER we've removed the ancestor_hashes_status_ref in case replay
        // then sends us another dead slot signal based on the updates we are
        // about to send.
        if let Some(potential_slots_to_dump) = potential_slots_to_dump {
            // Signal ReplayStage to dump the fork that is descended from
            // `earliest_mismatched_slot_to_dump`.
            if !potential_slots_to_dump.is_empty() {
                let _ = duplicate_slots_reset_sender.send(potential_slots_to_dump);
            }
        }
    }

    fn process_replay_updates(
        ancestor_hashes_replay_update_receiver: &AncestorHashesReplayUpdateReceiver,
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        dead_slot_pool: &mut HashSet<Slot>,
        repairable_dead_slot_pool: &mut HashSet<Slot>,
        root_slot: Slot,
    ) {
        for update in ancestor_hashes_replay_update_receiver.try_iter() {
            let slot = update.slot();
            if slot <= root_slot || ancestor_hashes_request_statuses.contains_key(&slot) {
                return;
            }
            match update {
                AncestorHashesReplayUpdate::Dead(dead_slot) => {
                    if repairable_dead_slot_pool.contains(&dead_slot) {
                        return;
                    } else {
                        dead_slot_pool.insert(dead_slot);
                    }
                }
                AncestorHashesReplayUpdate::DeadDuplicateConfirmed(dead_slot) => {
                    dead_slot_pool.remove(&dead_slot);
                    repairable_dead_slot_pool.insert(dead_slot);
                }
            }
        }
    }

    fn run_manage_ancestor_requests(
        ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>>,
        ancestor_hashes_request_socket: Arc<UdpSocket>,
        repair_info: RepairInfo,
        outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>>,
        exit: Arc<AtomicBool>,
        ancestor_hashes_replay_update_receiver: AncestorHashesReplayUpdateReceiver,
        retryable_slots_receiver: RetryableSlotsReceiver,
    ) -> JoinHandle<()> {
        let serve_repair = ServeRepair::new(repair_info.cluster_info.clone());
        let mut repair_stats = AncestorRepairRequestsStats::default();

        let mut dead_slot_pool = HashSet::new();
        let mut repairable_dead_slot_pool = HashSet::new();

        // Sliding window that limits the number of slots repaired via AncestorRepair
        // to MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND/second
        let mut request_throttle = vec![];
        Builder::new()
            .name("solana-manage-ancestor-requests".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    return;
                }

                Self::manage_ancestor_requests(
                    &ancestor_hashes_request_statuses,
                    &ancestor_hashes_request_socket,
                    &repair_info,
                    &outstanding_requests,
                    &ancestor_hashes_replay_update_receiver,
                    &retryable_slots_receiver,
                    &serve_repair,
                    &mut repair_stats,
                    &mut dead_slot_pool,
                    &mut repairable_dead_slot_pool,
                    &mut request_throttle,
                );

                sleep(Duration::from_millis(SLOT_MS));
            })
            .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn manage_ancestor_requests(
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        ancestor_hashes_request_socket: &UdpSocket,
        repair_info: &RepairInfo,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
        ancestor_hashes_replay_update_receiver: &AncestorHashesReplayUpdateReceiver,
        retryable_slots_receiver: &RetryableSlotsReceiver,
        serve_repair: &ServeRepair,
        repair_stats: &mut AncestorRepairRequestsStats,
        dead_slot_pool: &mut HashSet<Slot>,
        repairable_dead_slot_pool: &mut HashSet<Slot>,
        request_throttle: &mut Vec<u64>,
    ) {
        let root_bank = repair_info.bank_forks.read().unwrap().root_bank();
        for slot in retryable_slots_receiver.try_iter() {
            datapoint_info!("ancestor-repair-retry", ("slot", slot, i64));
            repairable_dead_slot_pool.insert(slot);
        }

        Self::process_replay_updates(
            ancestor_hashes_replay_update_receiver,
            ancestor_hashes_request_statuses,
            dead_slot_pool,
            repairable_dead_slot_pool,
            root_bank.slot(),
        );

        Self::find_epoch_slots_frozen_dead_slots(
            &repair_info.cluster_slots,
            dead_slot_pool,
            repairable_dead_slot_pool,
            &root_bank,
        );

        dead_slot_pool.retain(|slot| *slot > root_bank.slot());

        repairable_dead_slot_pool.retain(|slot| *slot > root_bank.slot());

        ancestor_hashes_request_statuses.retain(|slot, status| {
            if *slot <= root_bank.slot() {
                false
            } else if status.is_expired() {
                // Add the slot back to the repairable pool to retry
                repairable_dead_slot_pool.insert(*slot);
                false
            } else {
                true
            }
        });

        // Keep around the last second of requests in the throttler.
        request_throttle.retain(|request_time| *request_time > (timestamp() - 1000));

        let number_of_allowed_requests =
            MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND.saturating_sub(request_throttle.len());

        // Find dead slots for which it's worthwhile to ask the network for their
        // ancestors
        for _ in 0..number_of_allowed_requests {
            let slot = repairable_dead_slot_pool.iter().next().cloned();
            if let Some(slot) = slot {
                warn!(
                    "Cluster froze slot: {}, but we marked it as dead.
                    Initiating protocol to sample cluster for dead slot ancestors.",
                    slot
                );

                if Self::initiate_ancestor_hashes_requests_for_duplicate_slot(
                    ancestor_hashes_request_statuses,
                    ancestor_hashes_request_socket,
                    &repair_info.cluster_slots,
                    serve_repair,
                    &repair_info.repair_validators,
                    slot,
                    repair_stats,
                    outstanding_requests,
                ) {
                    request_throttle.push(timestamp());
                    repairable_dead_slot_pool.take(&slot).unwrap();
                }
            } else {
                break;
            }
        }

        repair_stats.report();
    }

    /// Find if any dead slots in `dead_slot_pool` have been frozen by sufficient
    /// number of nodes in the cluster to justify adding to the `repairable_dead_slot_pool`.
    fn find_epoch_slots_frozen_dead_slots(
        cluster_slots: &ClusterSlots,
        dead_slot_pool: &mut HashSet<Slot>,
        repairable_dead_slot_pool: &mut HashSet<Slot>,
        root_bank: &Bank,
    ) {
        dead_slot_pool.retain(|dead_slot| {
            let epoch = root_bank.get_epoch_and_slot_index(*dead_slot).0;
            if let Some(epoch_stakes) = root_bank.epoch_stakes(epoch) {
                let status = cluster_slots.lookup(*dead_slot);
                if let Some(completed_dead_slot_pubkeys) = status {
                    let total_stake = epoch_stakes.total_stake();
                    let node_id_to_vote_accounts = epoch_stakes.node_id_to_vote_accounts();
                    let total_completed_slot_stake: u64 = completed_dead_slot_pubkeys
                        .read()
                        .unwrap()
                        .iter()
                        .map(|(node_key, _)| {
                            node_id_to_vote_accounts
                                .get(node_key)
                                .map(|v| v.total_stake)
                                .unwrap_or(0)
                        })
                        .sum();
                    // If sufficient number of validators froze this slot, then there's a chance
                    // this dead slot was duplicate confirmed and will make it into in the main fork.
                    // This means it's worth asking the cluster to get the correct version.
                    if total_completed_slot_stake as f64 / total_stake as f64 > DUPLICATE_THRESHOLD
                    {
                        repairable_dead_slot_pool.insert(*dead_slot);
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                warn!(
                    "Dead slot {} is too far ahead of root bank {}",
                    dead_slot,
                    root_bank.slot()
                );
                false
            }
        })
    }

    /// Returns true if a request was successfully made and the status
    /// added to `ancestor_hashes_request_statuses`
    fn initiate_ancestor_hashes_requests_for_duplicate_slot(
        ancestor_hashes_request_statuses: &DashMap<Slot, DeadSlotAncestorRequestStatus>,
        ancestor_hashes_request_socket: &UdpSocket,
        cluster_slots: &ClusterSlots,
        serve_repair: &ServeRepair,
        repair_validators: &Option<HashSet<Pubkey>>,
        duplicate_slot: Slot,
        repair_stats: &mut AncestorRepairRequestsStats,
        outstanding_requests: &RwLock<OutstandingAncestorHashesRepairs>,
    ) -> bool {
        let sampled_validators = serve_repair.repair_request_ancestor_hashes_sample_peers(
            duplicate_slot,
            cluster_slots,
            repair_validators,
        );

        if let Ok(sampled_validators) = sampled_validators {
            for (pubkey, socket_addr) in sampled_validators.iter() {
                repair_stats
                    .ancestor_requests
                    .update(pubkey, duplicate_slot, 0);
                let nonce = outstanding_requests
                    .write()
                    .unwrap()
                    .add_request(AncestorHashesRepairType(duplicate_slot), timestamp());
                let request_bytes =
                    serve_repair.ancestor_repair_request_bytes(duplicate_slot, nonce);
                if let Ok(request_bytes) = request_bytes {
                    let _ = ancestor_hashes_request_socket.send_to(&request_bytes, socket_addr);
                }
            }

            let ancestor_request_status = DeadSlotAncestorRequestStatus::new(
                sampled_validators
                    .into_iter()
                    .map(|(_pk, socket_addr)| socket_addr),
                duplicate_slot,
            );
            assert!(!ancestor_hashes_request_statuses.contains_key(&duplicate_slot));
            ancestor_hashes_request_statuses.insert(duplicate_slot, ancestor_request_status);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::{
            cluster_slot_state_verifier::DuplicateSlotsToRepair,
            repair_service::DuplicateSlotsResetReceiver,
            replay_stage::{
                tests::{replay_blockstore_components, ReplayBlockstoreComponents},
                ReplayStage,
            },
            serve_repair::MAX_ANCESTOR_RESPONSES,
            vote_simulator::VoteSimulator,
        },
        solana_gossip::{
            cluster_info::{ClusterInfo, Node},
            contact_info::ContactInfo,
        },
        solana_ledger::{blockstore::make_many_slot_entries, get_tmp_ledger_path},
        solana_runtime::{accounts_background_service::AbsRequestSender, bank_forks::BankForks},
        solana_sdk::{hash::Hash, signature::Keypair},
        solana_streamer::socket::SocketAddrSpace,
        std::collections::HashMap,
        trees::tr,
    };

    #[test]
    pub fn test_ancestor_hashes_service_process_replay_updates() {
        let (ancestor_hashes_replay_update_sender, ancestor_hashes_replay_update_receiver) =
            unbounded();
        let ancestor_hashes_request_statuses = DashMap::new();
        let mut dead_slot_pool = HashSet::new();
        let mut repairable_dead_slot_pool = HashSet::new();
        let slot = 10;
        let mut root_slot = 0;

        // 1) Getting a dead signal should only add the slot to the dead pool
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::Dead(slot))
            .unwrap();
        AncestorHashesService::process_replay_updates(
            &ancestor_hashes_replay_update_receiver,
            &ancestor_hashes_request_statuses,
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            root_slot,
        );
        assert!(dead_slot_pool.contains(&slot));
        assert!(repairable_dead_slot_pool.is_empty());

        // 2) Getting a duplicate confirmed dead slot should move the slot
        // from the dead pool to the repairable pool
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(slot))
            .unwrap();
        AncestorHashesService::process_replay_updates(
            &ancestor_hashes_replay_update_receiver,
            &ancestor_hashes_request_statuses,
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            root_slot,
        );
        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.contains(&slot));

        // 3) Getting another dead signal should not add it back to the dead pool
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::Dead(slot))
            .unwrap();
        AncestorHashesService::process_replay_updates(
            &ancestor_hashes_replay_update_receiver,
            &ancestor_hashes_request_statuses,
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            root_slot,
        );
        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.contains(&slot));

        // 4) If an outstanding request for a slot already exists, should
        // ignore any signals from replay stage
        ancestor_hashes_request_statuses.insert(slot, DeadSlotAncestorRequestStatus::default());
        dead_slot_pool.clear();
        repairable_dead_slot_pool.clear();
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::Dead(slot))
            .unwrap();
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(slot))
            .unwrap();
        AncestorHashesService::process_replay_updates(
            &ancestor_hashes_replay_update_receiver,
            &ancestor_hashes_request_statuses,
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            root_slot,
        );
        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.is_empty());

        // 5) If we get any signals for slots <= root_slot, they should be ignored
        root_slot = 15;
        ancestor_hashes_request_statuses.clear();
        dead_slot_pool.clear();
        repairable_dead_slot_pool.clear();
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::Dead(root_slot - 1))
            .unwrap();
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(
                root_slot - 2,
            ))
            .unwrap();
        AncestorHashesService::process_replay_updates(
            &ancestor_hashes_replay_update_receiver,
            &ancestor_hashes_request_statuses,
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            root_slot,
        );
        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.is_empty());
    }

    #[test]
    fn test_ancestor_hashes_service_find_epoch_slots_frozen_dead_slots() {
        let vote_simulator = VoteSimulator::new(3);
        let cluster_slots = ClusterSlots::default();
        let mut dead_slot_pool = HashSet::new();
        let mut repairable_dead_slot_pool = HashSet::new();
        let root_bank = vote_simulator.bank_forks.read().unwrap().root_bank();
        let dead_slot = 10;
        dead_slot_pool.insert(dead_slot);

        // ClusterSlots doesn't have an entry for this slot yet, shouldn't move the slot
        // from the dead slot pool.
        AncestorHashesService::find_epoch_slots_frozen_dead_slots(
            &cluster_slots,
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &root_bank,
        );
        assert_eq!(dead_slot_pool.len(), 1);
        assert!(dead_slot_pool.contains(&dead_slot));
        assert!(repairable_dead_slot_pool.is_empty());

        let max_epoch = root_bank.epoch_stakes_map().keys().max().unwrap();
        let slot_outside_known_epochs = root_bank
            .epoch_schedule()
            .get_last_slot_in_epoch(*max_epoch)
            + 1;
        dead_slot_pool.insert(slot_outside_known_epochs);

        // Should remove `slot_outside_known_epochs`
        AncestorHashesService::find_epoch_slots_frozen_dead_slots(
            &cluster_slots,
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &root_bank,
        );
        assert_eq!(dead_slot_pool.len(), 1);
        assert!(dead_slot_pool.contains(&dead_slot));
        assert!(repairable_dead_slot_pool.is_empty());

        // Slot hasn't reached the threshold
        for (i, key) in (0..2).zip(vote_simulator.node_pubkeys.iter()) {
            cluster_slots.insert_node_id(dead_slot, *key);
            AncestorHashesService::find_epoch_slots_frozen_dead_slots(
                &cluster_slots,
                &mut dead_slot_pool,
                &mut repairable_dead_slot_pool,
                &root_bank,
            );
            if i == 0 {
                assert_eq!(dead_slot_pool.len(), 1);
                assert!(dead_slot_pool.contains(&dead_slot));
                assert!(repairable_dead_slot_pool.is_empty());
            } else {
                assert!(dead_slot_pool.is_empty());
                assert_eq!(repairable_dead_slot_pool.len(), 1);
                assert!(repairable_dead_slot_pool.contains(&dead_slot));
            }
        }
    }

    struct ResponderThreads {
        t_request_receiver: JoinHandle<()>,
        t_listen: JoinHandle<()>,
        exit: Arc<AtomicBool>,
        responder_info: ContactInfo,
        response_receiver: PacketBatchReceiver,
        correct_bank_hashes: HashMap<Slot, Hash>,
    }

    impl ResponderThreads {
        fn shutdown(self) {
            self.exit.store(true, Ordering::Relaxed);
            self.t_request_receiver.join().unwrap();
            self.t_listen.join().unwrap();
        }

        fn new(slot_to_query: Slot) -> Self {
            assert!(slot_to_query >= MAX_ANCESTOR_RESPONSES as Slot);
            let responder_node = Node::new_localhost();
            let cluster_info = ClusterInfo::new(
                responder_node.info.clone(),
                Arc::new(Keypair::new()),
                SocketAddrSpace::Unspecified,
            );
            let responder_serve_repair =
                Arc::new(RwLock::new(ServeRepair::new(Arc::new(cluster_info))));

            // Set up thread to give us responses
            let ledger_path = get_tmp_ledger_path!();
            let exit = Arc::new(AtomicBool::new(false));
            let (requests_sender, requests_receiver) = unbounded();
            let (response_sender, response_receiver) = unbounded();

            // Set up blockstore for responses
            let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());
            // Create slots [slot, slot + MAX_ANCESTOR_RESPONSES) with 5 shreds apiece
            let (shreds, _) =
                make_many_slot_entries(slot_to_query, MAX_ANCESTOR_RESPONSES as u64, 5);
            blockstore
                .insert_shreds(shreds, None, false)
                .expect("Expect successful ledger write");
            let mut correct_bank_hashes = HashMap::new();
            for duplicate_confirmed_slot in
                slot_to_query - MAX_ANCESTOR_RESPONSES as Slot + 1..=slot_to_query
            {
                let hash = Hash::new_unique();
                correct_bank_hashes.insert(duplicate_confirmed_slot, hash);
                blockstore.insert_bank_hash(duplicate_confirmed_slot, hash, true);
            }

            // Set up response threads
            let t_request_receiver = streamer::receiver(
                Arc::new(responder_node.sockets.serve_repair),
                exit.clone(),
                requests_sender,
                Recycler::default(),
                Arc::new(StreamerReceiveStats::new(
                    "ancestor_hashes_response_receiver",
                )),
                1,
                false,
            );
            let t_listen = ServeRepair::listen(
                responder_serve_repair,
                Some(blockstore),
                requests_receiver,
                response_sender,
                &exit,
            );

            Self {
                t_request_receiver,
                t_listen,
                exit,
                responder_info: responder_node.info,
                response_receiver,
                correct_bank_hashes,
            }
        }
    }

    struct ManageAncestorHashesState {
        ancestor_hashes_request_statuses: Arc<DashMap<Slot, DeadSlotAncestorRequestStatus>>,
        ancestor_hashes_request_socket: Arc<UdpSocket>,
        requester_serve_repair: ServeRepair,
        repair_info: RepairInfo,
        outstanding_requests: Arc<RwLock<OutstandingAncestorHashesRepairs>>,
        dead_slot_pool: HashSet<Slot>,
        repairable_dead_slot_pool: HashSet<Slot>,
        request_throttle: Vec<u64>,
        repair_stats: AncestorRepairRequestsStats,
        _duplicate_slots_reset_receiver: DuplicateSlotsResetReceiver,
        retryable_slots_sender: RetryableSlotsSender,
        retryable_slots_receiver: RetryableSlotsReceiver,
        ancestor_hashes_replay_update_sender: AncestorHashesReplayUpdateSender,
        ancestor_hashes_replay_update_receiver: AncestorHashesReplayUpdateReceiver,
    }

    impl ManageAncestorHashesState {
        fn new(bank_forks: Arc<RwLock<BankForks>>) -> Self {
            let ancestor_hashes_request_statuses = Arc::new(DashMap::new());
            let ancestor_hashes_request_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").unwrap());
            let epoch_schedule = *bank_forks.read().unwrap().root_bank().epoch_schedule();
            let requester_cluster_info = Arc::new(ClusterInfo::new(
                Node::new_localhost().info,
                Arc::new(Keypair::new()),
                SocketAddrSpace::Unspecified,
            ));
            let requester_serve_repair = ServeRepair::new(requester_cluster_info.clone());
            let (duplicate_slots_reset_sender, _duplicate_slots_reset_receiver) = unbounded();
            let repair_info = RepairInfo {
                bank_forks,
                cluster_info: requester_cluster_info,
                cluster_slots: Arc::new(ClusterSlots::default()),
                epoch_schedule,
                duplicate_slots_reset_sender,
                repair_validators: None,
            };

            let (ancestor_hashes_replay_update_sender, ancestor_hashes_replay_update_receiver) =
                unbounded();
            let (retryable_slots_sender, retryable_slots_receiver) = unbounded();
            Self {
                ancestor_hashes_request_statuses,
                ancestor_hashes_request_socket,
                requester_serve_repair,
                repair_info,
                outstanding_requests: Arc::new(RwLock::new(
                    OutstandingAncestorHashesRepairs::default(),
                )),
                dead_slot_pool: HashSet::new(),
                repairable_dead_slot_pool: HashSet::new(),
                request_throttle: vec![],
                repair_stats: AncestorRepairRequestsStats::default(),
                _duplicate_slots_reset_receiver,
                ancestor_hashes_replay_update_sender,
                ancestor_hashes_replay_update_receiver,
                retryable_slots_sender,
                retryable_slots_receiver,
            }
        }
    }

    fn setup_dead_slot(
        dead_slot: Slot,
        correct_bank_hashes: &HashMap<Slot, Hash>,
    ) -> ReplayBlockstoreComponents {
        assert!(dead_slot >= MAX_ANCESTOR_RESPONSES as Slot);
        let mut forks = tr(0);

        // Create a bank_forks that includes everything but the dead slot
        for slot in 1..dead_slot {
            forks.push_front(tr(slot));
        }
        let mut replay_blockstore_components = replay_blockstore_components(Some(forks), 1, None);
        let ReplayBlockstoreComponents {
            ref blockstore,
            ref mut vote_simulator,
            ..
        } = replay_blockstore_components;

        // Create dead slot in bank_forks
        let is_frozen = false;
        vote_simulator.fill_bank_forks(
            tr(dead_slot - 1) / tr(dead_slot),
            &HashMap::new(),
            is_frozen,
        );

        // Create slots [slot, slot + num_ancestors) with 5 shreds apiece
        let (shreds, _) = make_many_slot_entries(dead_slot, dead_slot, 5);
        blockstore
            .insert_shreds(shreds, None, false)
            .expect("Expect successful ledger write");
        for duplicate_confirmed_slot in 0..dead_slot {
            let bank_hash = correct_bank_hashes
                .get(&duplicate_confirmed_slot)
                .cloned()
                .unwrap_or_else(Hash::new_unique);
            blockstore.insert_bank_hash(duplicate_confirmed_slot, bank_hash, true);
        }
        blockstore.set_dead_slot(dead_slot).unwrap();
        replay_blockstore_components
    }

    #[test]
    fn test_ancestor_hashes_service_initiate_ancestor_hashes_requests_for_duplicate_slot() {
        let dead_slot = MAX_ANCESTOR_RESPONSES as Slot;
        let responder_threads = ResponderThreads::new(dead_slot);

        let ResponderThreads {
            ref responder_info,
            ref response_receiver,
            ref correct_bank_hashes,
            ..
        } = responder_threads;

        let ReplayBlockstoreComponents {
            blockstore: requester_blockstore,
            vote_simulator,
            ..
        } = setup_dead_slot(dead_slot, correct_bank_hashes);

        let ManageAncestorHashesState {
            ancestor_hashes_request_statuses,
            ancestor_hashes_request_socket,
            repair_info,
            outstanding_requests,
            requester_serve_repair,
            mut repair_stats,
            ..
        } = ManageAncestorHashesState::new(vote_simulator.bank_forks);

        let RepairInfo {
            cluster_info: requester_cluster_info,
            cluster_slots,
            repair_validators,
            ..
        } = repair_info;

        AncestorHashesService::initiate_ancestor_hashes_requests_for_duplicate_slot(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &cluster_slots,
            &requester_serve_repair,
            &repair_validators,
            dead_slot,
            &mut repair_stats,
            &outstanding_requests,
        );
        assert!(ancestor_hashes_request_statuses.is_empty());

        // Add the responder to the eligible list for requests
        let responder_id = responder_info.id;
        cluster_slots.insert_node_id(dead_slot, responder_id);
        requester_cluster_info.insert_info(responder_info.clone());
        // Now the request should actually be made
        AncestorHashesService::initiate_ancestor_hashes_requests_for_duplicate_slot(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &cluster_slots,
            &requester_serve_repair,
            &repair_validators,
            dead_slot,
            &mut repair_stats,
            &outstanding_requests,
        );

        assert_eq!(ancestor_hashes_request_statuses.len(), 1);
        assert!(ancestor_hashes_request_statuses.contains_key(&dead_slot));

        // Should have received valid response
        let mut response_packet = response_receiver
            .recv_timeout(Duration::from_millis(10_000))
            .unwrap();
        let packet = &mut response_packet.packets[0];
        packet.meta.set_addr(&responder_info.serve_repair);
        let decision = AncestorHashesService::verify_and_process_ancestor_response(
            packet,
            &ancestor_hashes_request_statuses,
            &mut AncestorHashesResponsesStats::default(),
            &outstanding_requests,
            &requester_blockstore,
        )
        .unwrap();

        assert_matches!(
            decision,
            (
                _dead_slot,
                DuplicateAncestorDecision::EarliestAncestorNotFrozen(_)
            )
        );
        assert_eq!(
            decision
                .1
                .repair_status()
                .unwrap()
                .correct_ancestors_to_repair,
            vec![(dead_slot, *correct_bank_hashes.get(&dead_slot).unwrap())]
        );

        // Should have removed the ancestor status on successful
        // completion
        assert!(ancestor_hashes_request_statuses.is_empty());
        responder_threads.shutdown();
    }

    #[test]
    fn test_ancestor_hashes_service_manage_ancestor_requests() {
        let vote_simulator = VoteSimulator::new(3);
        let ManageAncestorHashesState {
            ancestor_hashes_request_statuses,
            ancestor_hashes_request_socket,
            requester_serve_repair,
            repair_info,
            outstanding_requests,
            mut dead_slot_pool,
            mut repairable_dead_slot_pool,
            mut request_throttle,
            ancestor_hashes_replay_update_sender,
            ancestor_hashes_replay_update_receiver,
            retryable_slots_receiver,
            ..
        } = ManageAncestorHashesState::new(vote_simulator.bank_forks);
        let responder_node = Node::new_localhost();
        let RepairInfo {
            ref bank_forks,
            ref cluster_info,
            ..
        } = repair_info;
        cluster_info.insert_info(responder_node.info);
        bank_forks.read().unwrap().root_bank().epoch_schedule();
        // 1) No signals from ReplayStage, no requests should be made
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );

        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.is_empty());
        assert!(ancestor_hashes_request_statuses.is_empty());

        // 2) Simulate signals from ReplayStage, should make a request
        // for `dead_duplicate_confirmed_slot`
        let dead_slot = 10;
        let dead_duplicate_confirmed_slot = 14;
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::Dead(dead_slot))
            .unwrap();
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(
                dead_duplicate_confirmed_slot,
            ))
            .unwrap();
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(
                dead_duplicate_confirmed_slot,
            ))
            .unwrap();
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );

        assert_eq!(dead_slot_pool.len(), 1);
        assert!(dead_slot_pool.contains(&dead_slot));
        assert!(repairable_dead_slot_pool.is_empty());
        assert_eq!(ancestor_hashes_request_statuses.len(), 1);
        assert!(ancestor_hashes_request_statuses.contains_key(&dead_duplicate_confirmed_slot));

        // 3) Simulate an outstanding request timing out
        ancestor_hashes_request_statuses
            .get_mut(&dead_duplicate_confirmed_slot)
            .unwrap()
            .value_mut()
            .make_expired();

        // If the request timed out, we should remove the slot from `ancestor_hashes_request_statuses`,
        // and add it to `repairable_dead_slot_pool`. Because the request_throttle is at its limit,
        // we should not immediately retry the timed request.
        request_throttle.resize(MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND, std::u64::MAX);
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );

        assert_eq!(dead_slot_pool.len(), 1);
        assert!(dead_slot_pool.contains(&dead_slot));
        assert_eq!(repairable_dead_slot_pool.len(), 1);
        assert!(repairable_dead_slot_pool.contains(&dead_duplicate_confirmed_slot));
        assert!(ancestor_hashes_request_statuses.is_empty());

        // 4) If the throttle only has expired timestamps from more than a second ago,
        // then on the next iteration, we should clear the entries in the throttle
        // and retry a request for the timed out request
        request_throttle.clear();
        request_throttle.resize(
            MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND,
            timestamp() - 1001,
        );
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );
        assert_eq!(dead_slot_pool.len(), 1);
        assert!(dead_slot_pool.contains(&dead_slot));
        assert!(repairable_dead_slot_pool.is_empty());
        assert_eq!(ancestor_hashes_request_statuses.len(), 1);
        assert!(ancestor_hashes_request_statuses.contains_key(&dead_duplicate_confirmed_slot));
        // Request throttle includes one item for the request we just made
        assert_eq!(
            request_throttle.len(),
            ancestor_hashes_request_statuses.len()
        );

        // 5) If we've reached the throttle limit, no requests should be made,
        // but should still read off the channel for replay updates
        request_throttle.clear();
        request_throttle.resize(MAX_ANCESTOR_HASHES_SLOT_REQUESTS_PER_SECOND, std::u64::MAX);
        let dead_duplicate_confirmed_slot_2 = 15;
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(
                dead_duplicate_confirmed_slot_2,
            ))
            .unwrap();
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );

        assert_eq!(dead_slot_pool.len(), 1);
        assert!(dead_slot_pool.contains(&dead_slot));
        assert_eq!(repairable_dead_slot_pool.len(), 1);
        assert!(repairable_dead_slot_pool.contains(&dead_duplicate_confirmed_slot_2));
        assert_eq!(ancestor_hashes_request_statuses.len(), 1);
        assert!(ancestor_hashes_request_statuses.contains_key(&dead_duplicate_confirmed_slot));

        // 6) If root moves past slot, should remove it from all state
        let bank_forks = &repair_info.bank_forks;
        let root_bank = bank_forks.read().unwrap().root_bank();
        let new_root_slot = dead_duplicate_confirmed_slot_2 + 1;
        let new_root_bank = Bank::new_from_parent(&root_bank, &Pubkey::default(), new_root_slot);
        new_root_bank.freeze();
        {
            let mut w_bank_forks = bank_forks.write().unwrap();
            w_bank_forks.insert(new_root_bank);
            w_bank_forks.set_root(new_root_slot, &AbsRequestSender::default(), None);
        }
        assert!(!dead_slot_pool.is_empty());
        assert!(!repairable_dead_slot_pool.is_empty());
        assert!(!ancestor_hashes_request_statuses.is_empty());
        request_throttle.clear();
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );
        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.is_empty());
        assert!(ancestor_hashes_request_statuses.is_empty());
    }

    #[test]
    fn test_verify_and_process_ancestor_responses_invalid_packet() {
        let bank0 = Bank::default_for_tests();
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank0)));

        let ManageAncestorHashesState {
            ancestor_hashes_request_statuses,
            outstanding_requests,
            ..
        } = ManageAncestorHashesState::new(bank_forks);

        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&ledger_path).unwrap();

        // Create invalid packet with fewer bytes than the size of the nonce
        let mut packet = Packet::default();
        packet.meta.size = 0;

        assert!(AncestorHashesService::verify_and_process_ancestor_response(
            &packet,
            &ancestor_hashes_request_statuses,
            &mut AncestorHashesResponsesStats::default(),
            &outstanding_requests,
            &blockstore,
        )
        .is_none());
    }

    #[test]
    fn test_ancestor_hashes_service_manage_ancestor_hashes_after_replay_dump() {
        let dead_slot = MAX_ANCESTOR_RESPONSES as Slot;
        let responder_threads = ResponderThreads::new(dead_slot);

        let ResponderThreads {
            ref responder_info,
            ref response_receiver,
            ref correct_bank_hashes,
            ..
        } = responder_threads;

        let ReplayBlockstoreComponents {
            blockstore: requester_blockstore,
            vote_simulator,
            ..
        } = setup_dead_slot(dead_slot, correct_bank_hashes);

        let VoteSimulator {
            bank_forks,
            mut progress,
            ..
        } = vote_simulator;

        let ManageAncestorHashesState {
            ancestor_hashes_request_statuses,
            ancestor_hashes_request_socket,
            requester_serve_repair,
            repair_info,
            outstanding_requests,
            mut dead_slot_pool,
            mut repairable_dead_slot_pool,
            mut request_throttle,
            ancestor_hashes_replay_update_sender,
            ancestor_hashes_replay_update_receiver,
            retryable_slots_receiver,
            ..
        } = ManageAncestorHashesState::new(bank_forks.clone());

        let RepairInfo {
            cluster_info: ref requester_cluster_info,
            ref cluster_slots,
            ..
        } = repair_info;

        // Add the responder to the eligible list for requests
        let responder_id = responder_info.id;
        cluster_slots.insert_node_id(dead_slot, responder_id);
        requester_cluster_info.insert_info(responder_info.clone());

        // Simulate getting duplicate confirmed dead slot
        ancestor_hashes_replay_update_sender
            .send(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(
                dead_slot,
            ))
            .unwrap();

        // Simulate Replay dumping this slot
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        duplicate_slots_to_repair.insert(dead_slot, Hash::new_unique());
        ReplayStage::dump_then_repair_correct_slots(
            &mut duplicate_slots_to_repair,
            &mut bank_forks.read().unwrap().ancestors(),
            &mut bank_forks.read().unwrap().descendants(),
            &mut progress,
            &bank_forks,
            &requester_blockstore,
            None,
        );

        // Simulate making a request
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );

        assert_eq!(ancestor_hashes_request_statuses.len(), 1);
        assert!(ancestor_hashes_request_statuses.contains_key(&dead_slot));

        // Should have received valid response
        let mut response_packet = response_receiver
            .recv_timeout(Duration::from_millis(10_000))
            .unwrap();
        let packet = &mut response_packet.packets[0];
        packet.meta.set_addr(&responder_info.serve_repair);
        let decision = AncestorHashesService::verify_and_process_ancestor_response(
            packet,
            &ancestor_hashes_request_statuses,
            &mut AncestorHashesResponsesStats::default(),
            &outstanding_requests,
            &requester_blockstore,
        )
        .unwrap();

        assert_matches!(
            decision,
            (
                _dead_slot,
                DuplicateAncestorDecision::EarliestAncestorNotFrozen(_)
            )
        );
        assert_eq!(
            decision
                .1
                .repair_status()
                .unwrap()
                .correct_ancestors_to_repair,
            vec![(dead_slot, *correct_bank_hashes.get(&dead_slot).unwrap())]
        );

        // Should have removed the ancestor status on successful
        // completion
        assert!(ancestor_hashes_request_statuses.is_empty());
        responder_threads.shutdown();
    }

    #[test]
    fn test_ancestor_hashes_service_retryable_duplicate_ancestor_decision() {
        let vote_simulator = VoteSimulator::new(1);
        let ManageAncestorHashesState {
            ancestor_hashes_request_statuses,
            ancestor_hashes_request_socket,
            requester_serve_repair,
            repair_info,
            outstanding_requests,
            mut dead_slot_pool,
            mut repairable_dead_slot_pool,
            mut request_throttle,
            ancestor_hashes_replay_update_receiver,
            retryable_slots_receiver,
            retryable_slots_sender,
            ..
        } = ManageAncestorHashesState::new(vote_simulator.bank_forks);

        let decision = DuplicateAncestorDecision::SampleNotDuplicateConfirmed;
        assert!(decision.is_retryable());

        // Simulate network response processing thread reaching a retryable
        // decision
        let request_slot = 10;
        AncestorHashesService::handle_ancestor_request_decision(
            request_slot,
            decision,
            &repair_info.duplicate_slots_reset_sender,
            &retryable_slots_sender,
        );

        // Simulate ancestor request thread getting the retry signal
        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.is_empty());
        AncestorHashesService::manage_ancestor_requests(
            &ancestor_hashes_request_statuses,
            &ancestor_hashes_request_socket,
            &repair_info,
            &outstanding_requests,
            &ancestor_hashes_replay_update_receiver,
            &retryable_slots_receiver,
            &requester_serve_repair,
            &mut AncestorRepairRequestsStats::default(),
            &mut dead_slot_pool,
            &mut repairable_dead_slot_pool,
            &mut request_throttle,
        );

        assert!(dead_slot_pool.is_empty());
        assert!(repairable_dead_slot_pool.contains(&request_slot));
    }
}
