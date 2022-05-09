#![allow(clippy::implicit_hasher)]
use {
    crate::shred::{ShredType, SIZE_OF_NONCE},
    rayon::{
        iter::{
            IndexedParallelIterator, IntoParallelIterator, IntoParallelRefMutIterator,
            ParallelIterator,
        },
        ThreadPool,
    },
    sha2::{Digest, Sha512},
    solana_metrics::inc_new_counter_debug,
    solana_perf::{
        cuda_runtime::PinnedVec,
        packet::{limited_deserialize, Packet, PacketBatch},
        perf_libs,
        recycler_cache::RecyclerCache,
        sigverify::{self, count_packets_in_batches, TxOffset},
    },
    solana_rayon_threadlimit::get_thread_count,
    solana_sdk::{
        clock::Slot,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
    },
    std::{collections::HashMap, mem::size_of, sync::Arc},
};

pub const SIGN_SHRED_GPU_MIN: usize = 256;

lazy_static! {
    pub static ref SIGVERIFY_THREAD_POOL: ThreadPool = rayon::ThreadPoolBuilder::new()
        .num_threads(get_thread_count())
        .thread_name(|ix| format!("sigverify_shreds_{}", ix))
        .build()
        .unwrap();
}

/// Assuming layout is
/// signature: Signature
/// signed_msg: {
///   type: ShredType
///   slot: u64,
///   ...
/// }
/// Signature is the first thing in the packet, and slot is the first thing in the signed message.
pub fn verify_shred_cpu(packet: &Packet, slot_leaders: &HashMap<u64, [u8; 32]>) -> Option<u8> {
    let sig_start = 0;
    let sig_end = size_of::<Signature>();
    let slot_start = sig_end + size_of::<ShredType>();
    let slot_end = slot_start + size_of::<u64>();
    let msg_start = sig_end;
    if packet.meta.discard() {
        return Some(0);
    }
    trace!("slot start and end {} {}", slot_start, slot_end);
    if packet.meta.size < slot_end {
        return Some(0);
    }
    let slot: u64 = limited_deserialize(&packet.data[slot_start..slot_end]).ok()?;
    let msg_end = if packet.meta.repair() {
        packet.meta.size.saturating_sub(SIZE_OF_NONCE)
    } else {
        packet.meta.size
    };
    trace!("slot {}", slot);
    let pubkey = slot_leaders.get(&slot)?;
    if packet.meta.size < sig_end {
        return Some(0);
    }
    let signature = Signature::new(&packet.data[sig_start..sig_end]);
    trace!("signature {}", signature);
    if !signature.verify(pubkey, &packet.data[msg_start..msg_end]) {
        return Some(0);
    }
    Some(1)
}

fn verify_shreds_cpu(
    batches: &[PacketBatch],
    slot_leaders: &HashMap<u64, [u8; 32]>,
) -> Vec<Vec<u8>> {
    use rayon::prelude::*;
    let packet_count = count_packets_in_batches(batches);
    debug!("CPU SHRED ECDSA for {}", packet_count);
    let rv = SIGVERIFY_THREAD_POOL.install(|| {
        batches
            .into_par_iter()
            .map(|batch| {
                batch
                    .packets
                    .par_iter()
                    .map(|p| verify_shred_cpu(p, slot_leaders).unwrap_or(0))
                    .collect()
            })
            .collect()
    });
    inc_new_counter_debug!("ed25519_shred_verify_cpu", packet_count);
    rv
}

fn slot_key_data_for_gpu<
    T: Sync + Sized + Default + std::fmt::Debug + Eq + std::hash::Hash + Clone + Copy + AsRef<[u8]>,
>(
    offset_start: usize,
    batches: &[PacketBatch],
    slot_keys: &HashMap<u64, T>,
    recycler_cache: &RecyclerCache,
) -> (PinnedVec<u8>, TxOffset, usize) {
    //TODO: mark Pubkey::default shreds as failed after the GPU returns
    assert_eq!(slot_keys.get(&std::u64::MAX), Some(&T::default()));
    let slots: Vec<Vec<Slot>> = SIGVERIFY_THREAD_POOL.install(|| {
        batches
            .into_par_iter()
            .map(|batch| {
                batch
                    .packets
                    .iter()
                    .map(|packet| {
                        let slot_start = size_of::<Signature>() + size_of::<ShredType>();
                        let slot_end = slot_start + size_of::<u64>();
                        if packet.meta.size < slot_end || packet.meta.discard() {
                            return std::u64::MAX;
                        }
                        let slot: Option<u64> =
                            limited_deserialize(&packet.data[slot_start..slot_end]).ok();
                        match slot {
                            Some(slot) if slot_keys.get(&slot).is_some() => slot,
                            _ => std::u64::MAX,
                        }
                    })
                    .collect()
            })
            .collect()
    });
    let mut keys_to_slots: HashMap<T, Vec<u64>> = HashMap::new();
    for batch in slots.iter() {
        for slot in batch.iter() {
            let key = slot_keys.get(slot).unwrap();
            keys_to_slots
                .entry(*key)
                .or_insert_with(Vec::new)
                .push(*slot);
        }
    }
    let mut keyvec = recycler_cache.buffer().allocate("shred_gpu_pubkeys");
    keyvec.set_pinnable();
    let mut slot_to_key_ix = HashMap::new();

    let keyvec_size = keys_to_slots.len() * size_of::<T>();
    keyvec.resize(keyvec_size, 0);

    for (i, (k, slots)) in keys_to_slots.iter().enumerate() {
        let start = i * size_of::<T>();
        let end = start + size_of::<T>();
        keyvec[start..end].copy_from_slice(k.as_ref());
        for s in slots {
            slot_to_key_ix.insert(s, i);
        }
    }
    let mut offsets = recycler_cache.offsets().allocate("shred_offsets");
    offsets.set_pinnable();
    slots.iter().for_each(|packet_slots| {
        packet_slots.iter().for_each(|slot| {
            offsets
                .push((offset_start + (slot_to_key_ix.get(slot).unwrap() * size_of::<T>())) as u32);
        });
    });
    let num_in_packets = resize_vec(&mut keyvec);
    trace!("keyvec.len: {}", keyvec.len());
    trace!("keyvec: {:?}", keyvec);
    trace!("offsets: {:?}", offsets);
    (keyvec, offsets, num_in_packets)
}

fn vec_size_in_packets(keyvec: &PinnedVec<u8>) -> usize {
    (keyvec.len() + (size_of::<Packet>() - 1)) / size_of::<Packet>()
}

fn resize_vec(keyvec: &mut PinnedVec<u8>) -> usize {
    //HACK: Pubkeys vector is passed along as a `PacketBatch` buffer to the GPU
    //TODO: GPU needs a more opaque interface, which can handle variable sized structures for data
    //Pad the Pubkeys buffer such that it is bigger than a buffer of Packet sized elems
    let num_in_packets = (keyvec.len() + (size_of::<Packet>() - 1)) / size_of::<Packet>();
    keyvec.resize(num_in_packets * size_of::<Packet>(), 0u8);
    num_in_packets
}

fn shred_gpu_offsets(
    mut pubkeys_end: usize,
    batches: &[PacketBatch],
    recycler_cache: &RecyclerCache,
) -> (TxOffset, TxOffset, TxOffset, Vec<Vec<u32>>) {
    let mut signature_offsets = recycler_cache.offsets().allocate("shred_signatures");
    signature_offsets.set_pinnable();
    let mut msg_start_offsets = recycler_cache.offsets().allocate("shred_msg_starts");
    msg_start_offsets.set_pinnable();
    let mut msg_sizes = recycler_cache.offsets().allocate("shred_msg_sizes");
    msg_sizes.set_pinnable();
    let mut v_sig_lens = vec![];
    for batch in batches.iter() {
        let mut sig_lens = Vec::new();
        for packet in batch.packets.iter() {
            let sig_start = pubkeys_end;
            let sig_end = sig_start + size_of::<Signature>();
            let msg_start = sig_end;
            let msg_end = if packet.meta.repair() {
                sig_start + packet.meta.size.saturating_sub(SIZE_OF_NONCE)
            } else {
                sig_start + packet.meta.size
            };
            signature_offsets.push(sig_start as u32);
            msg_start_offsets.push(msg_start as u32);
            let msg_size = if msg_end < msg_start {
                0
            } else {
                msg_end - msg_start
            };
            msg_sizes.push(msg_size as u32);
            sig_lens.push(1);
            pubkeys_end += size_of::<Packet>();
        }
        v_sig_lens.push(sig_lens);
    }
    (signature_offsets, msg_start_offsets, msg_sizes, v_sig_lens)
}

pub fn verify_shreds_gpu(
    batches: &[PacketBatch],
    slot_leaders: &HashMap<u64, [u8; 32]>,
    recycler_cache: &RecyclerCache,
) -> Vec<Vec<u8>> {
    let api = perf_libs::api();
    if api.is_none() {
        return verify_shreds_cpu(batches, slot_leaders);
    }
    let api = api.unwrap();

    let mut elems = Vec::new();
    let mut rvs = Vec::new();
    let packet_count = count_packets_in_batches(batches);
    let (pubkeys, pubkey_offsets, mut num_packets) =
        slot_key_data_for_gpu(0, batches, slot_leaders, recycler_cache);
    //HACK: Pubkeys vector is passed along as a `PacketBatch` buffer to the GPU
    //TODO: GPU needs a more opaque interface, which can handle variable sized structures for data
    let pubkeys_len = num_packets * size_of::<Packet>();
    trace!("num_packets: {}", num_packets);
    trace!("pubkeys_len: {}", pubkeys_len);
    let (signature_offsets, msg_start_offsets, msg_sizes, v_sig_lens) =
        shred_gpu_offsets(pubkeys_len, batches, recycler_cache);
    let mut out = recycler_cache.buffer().allocate("out_buffer");
    out.set_pinnable();
    elems.push(perf_libs::Elems {
        #[allow(clippy::cast_ptr_alignment)]
        elems: pubkeys.as_ptr() as *const solana_sdk::packet::Packet,
        num: num_packets as u32,
    });

    for batch in batches {
        elems.push(perf_libs::Elems {
            elems: batch.packets.as_ptr(),
            num: batch.packets.len() as u32,
        });
        let mut v = Vec::new();
        v.resize(batch.packets.len(), 0);
        rvs.push(v);
        num_packets += batch.packets.len();
    }
    out.resize(signature_offsets.len(), 0);

    trace!("Starting verify num packets: {}", num_packets);
    trace!("elem len: {}", elems.len() as u32);
    trace!("packet sizeof: {}", size_of::<Packet>() as u32);
    const USE_NON_DEFAULT_STREAM: u8 = 1;
    unsafe {
        let res = (api.ed25519_verify_many)(
            elems.as_ptr(),
            elems.len() as u32,
            size_of::<Packet>() as u32,
            num_packets as u32,
            signature_offsets.len() as u32,
            msg_sizes.as_ptr(),
            pubkey_offsets.as_ptr(),
            signature_offsets.as_ptr(),
            msg_start_offsets.as_ptr(),
            out.as_mut_ptr(),
            USE_NON_DEFAULT_STREAM,
        );
        if res != 0 {
            trace!("RETURN!!!: {}", res);
        }
    }
    trace!("done verify");
    trace!("out buf {:?}", out);

    sigverify::copy_return_values(&v_sig_lens, &out, &mut rvs);

    inc_new_counter_debug!("ed25519_shred_verify_gpu", packet_count);
    rvs
}

/// Assuming layout is
/// signature: Signature
/// signed_msg: {
///   type: ShredType
///   slot: u64,
///   ...
/// }
/// Signature is the first thing in the packet, and slot is the first thing in the signed message.
fn sign_shred_cpu(keypair: &Keypair, packet: &mut Packet) {
    let sig_start = 0;
    let sig_end = sig_start + size_of::<Signature>();
    let msg_start = sig_end;
    let msg_end = packet.meta.size;
    assert!(
        packet.meta.size >= msg_end,
        "packet is not large enough for a signature"
    );
    let signature = keypair.sign_message(&packet.data[msg_start..msg_end]);
    trace!("signature {:?}", signature);
    packet.data[0..sig_end].copy_from_slice(signature.as_ref());
}

pub fn sign_shreds_cpu(keypair: &Keypair, batches: &mut [PacketBatch]) {
    use rayon::prelude::*;
    let packet_count = count_packets_in_batches(batches);
    debug!("CPU SHRED ECDSA for {}", packet_count);
    SIGVERIFY_THREAD_POOL.install(|| {
        batches.par_iter_mut().for_each(|batch| {
            batch.packets[..]
                .par_iter_mut()
                .for_each(|p| sign_shred_cpu(keypair, p));
        });
    });
    inc_new_counter_debug!("ed25519_shred_verify_cpu", packet_count);
}

pub fn sign_shreds_gpu_pinned_keypair(keypair: &Keypair, cache: &RecyclerCache) -> PinnedVec<u8> {
    let mut vec = cache.buffer().allocate("pinned_keypair");
    let pubkey = keypair.pubkey().to_bytes();
    let secret = keypair.secret().to_bytes();
    let mut hasher = Sha512::default();
    hasher.update(&secret);
    let mut result = hasher.finalize();
    result[0] &= 248;
    result[31] &= 63;
    result[31] |= 64;
    vec.resize(pubkey.len() + result.len(), 0);
    vec[0..pubkey.len()].copy_from_slice(&pubkey);
    vec[pubkey.len()..].copy_from_slice(&result);
    resize_vec(&mut vec);
    vec
}

pub fn sign_shreds_gpu(
    keypair: &Keypair,
    pinned_keypair: &Option<Arc<PinnedVec<u8>>>,
    batches: &mut [PacketBatch],
    recycler_cache: &RecyclerCache,
) {
    let sig_size = size_of::<Signature>();
    let pubkey_size = size_of::<Pubkey>();
    let api = perf_libs::api();
    let packet_count = count_packets_in_batches(batches);
    if api.is_none() || packet_count < SIGN_SHRED_GPU_MIN || pinned_keypair.is_none() {
        return sign_shreds_cpu(keypair, batches);
    }
    let api = api.unwrap();
    let pinned_keypair = pinned_keypair.as_ref().unwrap();

    let mut elems = Vec::new();
    let offset: usize = pinned_keypair.len();
    let num_keypair_packets = vec_size_in_packets(pinned_keypair);
    let mut num_packets = num_keypair_packets;

    //should be zero
    let mut pubkey_offsets = recycler_cache.offsets().allocate("pubkey offsets");
    pubkey_offsets.resize(packet_count, 0);

    let mut secret_offsets = recycler_cache.offsets().allocate("secret_offsets");
    secret_offsets.resize(packet_count, pubkey_size as u32);

    trace!("offset: {}", offset);
    let (signature_offsets, msg_start_offsets, msg_sizes, _v_sig_lens) =
        shred_gpu_offsets(offset, batches, recycler_cache);
    let total_sigs = signature_offsets.len();
    let mut signatures_out = recycler_cache.buffer().allocate("ed25519 signatures");
    signatures_out.set_pinnable();
    signatures_out.resize(total_sigs * sig_size, 0);
    elems.push(perf_libs::Elems {
        #[allow(clippy::cast_ptr_alignment)]
        elems: pinned_keypair.as_ptr() as *const solana_sdk::packet::Packet,
        num: num_keypair_packets as u32,
    });

    for batch in batches.iter() {
        elems.push(perf_libs::Elems {
            elems: batch.packets.as_ptr(),
            num: batch.packets.len() as u32,
        });
        let mut v = Vec::new();
        v.resize(batch.packets.len(), 0);
        num_packets += batch.packets.len();
    }

    trace!("Starting verify num packets: {}", num_packets);
    trace!("elem len: {}", elems.len() as u32);
    trace!("packet sizeof: {}", size_of::<Packet>() as u32);
    const USE_NON_DEFAULT_STREAM: u8 = 1;
    unsafe {
        let res = (api.ed25519_sign_many)(
            elems.as_mut_ptr(),
            elems.len() as u32,
            size_of::<Packet>() as u32,
            num_packets as u32,
            total_sigs as u32,
            msg_sizes.as_ptr(),
            pubkey_offsets.as_ptr(),
            secret_offsets.as_ptr(),
            msg_start_offsets.as_ptr(),
            signatures_out.as_mut_ptr(),
            USE_NON_DEFAULT_STREAM,
        );
        if res != 0 {
            trace!("RETURN!!!: {}", res);
        }
    }
    trace!("done sign");
    let mut sizes: Vec<usize> = vec![0];
    sizes.extend(batches.iter().map(|b| b.packets.len()));
    for i in 0..sizes.len() {
        if i == 0 {
            continue;
        }
        sizes[i] += sizes[i - 1];
    }
    SIGVERIFY_THREAD_POOL.install(|| {
        batches
            .par_iter_mut()
            .enumerate()
            .for_each(|(batch_ix, batch)| {
                let num_packets = sizes[batch_ix];
                batch.packets[..]
                    .par_iter_mut()
                    .enumerate()
                    .for_each(|(packet_ix, packet)| {
                        let sig_ix = packet_ix + num_packets;
                        let sig_start = sig_ix * sig_size;
                        let sig_end = sig_start + sig_size;
                        packet.data[0..sig_size]
                            .copy_from_slice(&signatures_out[sig_start..sig_end]);
                    });
            });
    });
    inc_new_counter_debug!("ed25519_shred_sign_gpu", packet_count);
}

#[cfg(test)]
pub mod tests {
    use {
        super::*,
        crate::shred::{Shred, ShredFlags, SIZE_OF_DATA_SHRED_PAYLOAD},
        solana_sdk::signature::{Keypair, Signer},
    };

    fn run_test_sigverify_shred_cpu(slot: Slot) {
        solana_logger::setup();
        let mut packet = Packet::default();
        let mut shred = Shred::new_from_data(
            slot,
            0xc0de,
            0xdead,
            &[1, 2, 3, 4],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            0,
            0xc0de,
        );
        assert_eq!(shred.slot(), slot);
        let keypair = Keypair::new();
        shred.sign(&keypair);
        trace!("signature {}", shred.signature());
        packet.data[0..shred.payload().len()].copy_from_slice(shred.payload());
        packet.meta.size = shred.payload().len();

        let leader_slots = [(slot, keypair.pubkey().to_bytes())]
            .iter()
            .cloned()
            .collect();
        let rv = verify_shred_cpu(&packet, &leader_slots);
        assert_eq!(rv, Some(1));

        let wrong_keypair = Keypair::new();
        let leader_slots = [(slot, wrong_keypair.pubkey().to_bytes())]
            .iter()
            .cloned()
            .collect();
        let rv = verify_shred_cpu(&packet, &leader_slots);
        assert_eq!(rv, Some(0));

        let leader_slots = HashMap::new();
        let rv = verify_shred_cpu(&packet, &leader_slots);
        assert_eq!(rv, None);
    }

    #[test]
    fn test_sigverify_shred_cpu() {
        run_test_sigverify_shred_cpu(0xdead_c0de);
    }

    fn run_test_sigverify_shreds_cpu(slot: Slot) {
        solana_logger::setup();
        let mut batches = [PacketBatch::default()];
        let mut shred = Shred::new_from_data(
            slot,
            0xc0de,
            0xdead,
            &[1, 2, 3, 4],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            0,
            0xc0de,
        );
        let keypair = Keypair::new();
        shred.sign(&keypair);
        batches[0].packets.resize(1, Packet::default());
        batches[0].packets[0].data[0..shred.payload().len()].copy_from_slice(shred.payload());
        batches[0].packets[0].meta.size = shred.payload().len();

        let leader_slots = [(slot, keypair.pubkey().to_bytes())]
            .iter()
            .cloned()
            .collect();
        let rv = verify_shreds_cpu(&batches, &leader_slots);
        assert_eq!(rv, vec![vec![1]]);

        let wrong_keypair = Keypair::new();
        let leader_slots = [(slot, wrong_keypair.pubkey().to_bytes())]
            .iter()
            .cloned()
            .collect();
        let rv = verify_shreds_cpu(&batches, &leader_slots);
        assert_eq!(rv, vec![vec![0]]);

        let leader_slots = HashMap::new();
        let rv = verify_shreds_cpu(&batches, &leader_slots);
        assert_eq!(rv, vec![vec![0]]);

        let leader_slots = [(slot, keypair.pubkey().to_bytes())]
            .iter()
            .cloned()
            .collect();
        batches[0].packets[0].meta.size = 0;
        let rv = verify_shreds_cpu(&batches, &leader_slots);
        assert_eq!(rv, vec![vec![0]]);
    }

    #[test]
    fn test_sigverify_shreds_cpu() {
        run_test_sigverify_shreds_cpu(0xdead_c0de);
    }

    fn run_test_sigverify_shreds_gpu(slot: Slot) {
        solana_logger::setup();
        let recycler_cache = RecyclerCache::default();

        let mut batches = [PacketBatch::default()];
        let mut shred = Shred::new_from_data(
            slot,
            0xc0de,
            0xdead,
            &[1, 2, 3, 4],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            0,
            0xc0de,
        );
        let keypair = Keypair::new();
        shred.sign(&keypair);
        batches[0].packets.resize(1, Packet::default());
        batches[0].packets[0].data[0..shred.payload().len()].copy_from_slice(shred.payload());
        batches[0].packets[0].meta.size = shred.payload().len();

        let leader_slots = [
            (std::u64::MAX, Pubkey::default().to_bytes()),
            (slot, keypair.pubkey().to_bytes()),
        ]
        .iter()
        .cloned()
        .collect();
        let rv = verify_shreds_gpu(&batches, &leader_slots, &recycler_cache);
        assert_eq!(rv, vec![vec![1]]);

        let wrong_keypair = Keypair::new();
        let leader_slots = [
            (std::u64::MAX, Pubkey::default().to_bytes()),
            (slot, wrong_keypair.pubkey().to_bytes()),
        ]
        .iter()
        .cloned()
        .collect();
        let rv = verify_shreds_gpu(&batches, &leader_slots, &recycler_cache);
        assert_eq!(rv, vec![vec![0]]);

        let leader_slots = [(std::u64::MAX, [0u8; 32])].iter().cloned().collect();
        let rv = verify_shreds_gpu(&batches, &leader_slots, &recycler_cache);
        assert_eq!(rv, vec![vec![0]]);

        batches[0].packets[0].meta.size = 0;
        let leader_slots = [
            (std::u64::MAX, Pubkey::default().to_bytes()),
            (slot, keypair.pubkey().to_bytes()),
        ]
        .iter()
        .cloned()
        .collect();
        let rv = verify_shreds_gpu(&batches, &leader_slots, &recycler_cache);
        assert_eq!(rv, vec![vec![0]]);
    }

    #[test]
    fn test_sigverify_shreds_gpu() {
        run_test_sigverify_shreds_gpu(0xdead_c0de);
    }

    fn run_test_sigverify_shreds_sign_gpu(slot: Slot) {
        solana_logger::setup();
        let recycler_cache = RecyclerCache::default();

        let mut packet_batch = PacketBatch::default();
        let num_packets = 32;
        let num_batches = 100;
        packet_batch.packets.resize(num_packets, Packet::default());
        for (i, p) in packet_batch.packets.iter_mut().enumerate() {
            let shred = Shred::new_from_data(
                slot,
                0xc0de,
                i as u16,
                &[5; SIZE_OF_DATA_SHRED_PAYLOAD],
                ShredFlags::LAST_SHRED_IN_SLOT,
                1,
                2,
                0xc0de,
            );
            shred.copy_to_packet(p);
        }
        let mut batches = vec![packet_batch; num_batches];
        let keypair = Keypair::new();
        let pinned_keypair = sign_shreds_gpu_pinned_keypair(&keypair, &recycler_cache);
        let pinned_keypair = Some(Arc::new(pinned_keypair));
        let pubkeys = [
            (std::u64::MAX, Pubkey::default().to_bytes()),
            (slot, keypair.pubkey().to_bytes()),
        ]
        .iter()
        .cloned()
        .collect();
        //unsigned
        let rv = verify_shreds_gpu(&batches, &pubkeys, &recycler_cache);
        assert_eq!(rv, vec![vec![0; num_packets]; num_batches]);
        //signed
        sign_shreds_gpu(&keypair, &pinned_keypair, &mut batches, &recycler_cache);
        let rv = verify_shreds_cpu(&batches, &pubkeys);
        assert_eq!(rv, vec![vec![1; num_packets]; num_batches]);

        let rv = verify_shreds_gpu(&batches, &pubkeys, &recycler_cache);
        assert_eq!(rv, vec![vec![1; num_packets]; num_batches]);
    }

    #[test]
    fn test_sigverify_shreds_sign_gpu() {
        run_test_sigverify_shreds_sign_gpu(0xdead_c0de);
    }

    fn run_test_sigverify_shreds_sign_cpu(slot: Slot) {
        solana_logger::setup();

        let mut batches = [PacketBatch::default()];
        let keypair = Keypair::new();
        let shred = Shred::new_from_data(
            slot,
            0xc0de,
            0xdead,
            &[1, 2, 3, 4],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            0,
            0xc0de,
        );
        batches[0].packets.resize(1, Packet::default());
        batches[0].packets[0].data[0..shred.payload().len()].copy_from_slice(shred.payload());
        batches[0].packets[0].meta.size = shred.payload().len();
        let pubkeys = [
            (slot, keypair.pubkey().to_bytes()),
            (std::u64::MAX, Pubkey::default().to_bytes()),
        ]
        .iter()
        .cloned()
        .collect();
        //unsigned
        let rv = verify_shreds_cpu(&batches, &pubkeys);
        assert_eq!(rv, vec![vec![0]]);
        //signed
        sign_shreds_cpu(&keypair, &mut batches);
        let rv = verify_shreds_cpu(&batches, &pubkeys);
        assert_eq!(rv, vec![vec![1]]);
    }

    #[test]
    fn test_sigverify_shreds_sign_cpu() {
        run_test_sigverify_shreds_sign_cpu(0xdead_c0de);
    }
}
