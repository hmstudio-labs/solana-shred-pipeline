use std::{collections::HashSet, hash::Hash, sync::atomic::Ordering};

use itertools::Itertools;
use jito_protos::{
    shredstream::TraceShred,
    filtered::TxData,
};
use log::{debug, warn};
use prost::Message;
use solana_ledger::{
    blockstore::MAX_DATA_SHREDS_PER_SLOT,
    shred::{
        merkle::{Shred, ShredCode},
        ReedSolomonCache, ShredType, Shredder,
    },
};
use solana_metrics::datapoint_warn;
use solana_perf::packet::PacketBatch;
use solana_sdk::{clock::{Slot, MAX_PROCESSING_AGE}, pubkey::Pubkey};
use tokio::sync::broadcast::Sender;

use crate::forwarder::ShredMetrics;

// ======================================================
// Transaction Filtering Constants
// ======================================================
pub const RAYDIUM_PROGRAM_ID: Pubkey = Pubkey::from_str_const("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK");
pub const PUMPFUN_AMM_PROGRAM_ID: Pubkey = Pubkey::from_str_const("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");

static STABLE_MINTS: Lazy<HashSet<Pubkey>> = Lazy::new(|| {
    HashSet::from([
        Pubkey::from_str_const("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"), // USDC
        Pubkey::from_str_const("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"), // USDT
        Pubkey::from_str_const("27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4"), // JLP
        Pubkey::from_str_const("HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr"), // EURC
        Pubkey::from_str_const("4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"), // RAY
        Pubkey::from_str_const("BkDKvbUQpr17c5w3zZzEA1VvpirgWcKuMEHtiYGEaP1c"), // SMRT
    ])
});

const RAY_DECREASE_LIQUIDITY_V2: [u8; 8] = [58, 127, 188, 62, 79, 82, 196, 96];

#[inline(always)]
fn is_raydium_decrease_liquidity_v2(data: &[u8]) -> bool {
    data.len() >= 8 && data[..8] == RAY_DECREASE_LIQUIDITY_V2
}

/// Filter transactions from already-deserialized entries
fn filter_entries(slot: u64, entries: &[solana_entry::entry::Entry], filtered_sender: &Sender<TxData>) {
    for entry in entries.iter() {
        for tx in &entry.transactions {
            let accounts = tx.message.static_account_keys();

            // Fast path: skip simple transactions with <= 20 accounts
            // DEX operations typically involve more accounts (tokens, pools, authority, etc.)
            if accounts.len() <= 20 {
                continue;
            }

            // Check for Raydium program
            if let Some(pid) = accounts.iter().position(|k| *k == RAYDIUM_PROGRAM_ID) {
                for instruction in tx.message.instructions().iter() {
                    if instruction.program_id_index as usize != pid {
                        continue;
                    }

                    // Filter Raydium decrease_liquidity_v2
                    if is_raydium_decrease_liquidity_v2(&instruction.data) && instruction.accounts.len() >= 17 {
                        let index = instruction.accounts[15] as usize;
                        if index >= accounts.len() {
                            continue;
                        }
                        let mint_pubkey = &accounts[index];

                        // Skip stable mints
                        if STABLE_MINTS.contains(mint_pubkey) {
                            continue;
                        }

                        debug!(
                            "Found Raydium decrease_liquidity_v2: slot={}, tx={}, mint={}",
                            slot,
                            tx.signatures[0].to_string(),
                            mint_pubkey.to_string()
                        );

                        let _ = filtered_sender.send(TxData {
                            slot,
                            mint: mint_pubkey.to_string(),
                        });
                    }
                }
            }

            // Check for PumpFun AMM program
            if let Some(_pid) = accounts.iter().position(|k| *k == PUMPFUN_AMM_PROGRAM_ID) {
                for instruction in tx.message.instructions().iter() {
                    if instruction.program_id_index as usize != _pid {
                        continue;
                    }

                    debug!(
                        "Found PumpFun AMM instruction: slot={}, tx={}",
                        slot,
                        tx.signatures[0].to_string()
                    );

                    let _ = filtered_sender.send(TxData {
                        slot,
                        mint: String::new(),
                    });
                }
            }
        }
    }
}

use once_cell::sync::Lazy;

#[derive(Default, Debug, Copy, Clone, Eq, PartialEq)]
enum ShredStatus {
    #[default]
    Unknown,
    /// Shred that is **not** marked as [ShredFlags::DATA_COMPLETE_SHRED]
    NotDataComplete,
    /// Shred that is marked as [ShredFlags::DATA_COMPLETE_SHRED]
    DataComplete,
}

/// Tracks per-slot shred information for data shreds
/// Guaranteed to have MAX_DATA_SHREDS_PER_SLOT entries in each Vec
#[derive(Debug)]
pub struct ShredsStateTracker {
    /// Compact status of each data shred for fast iteration.
    data_status: Vec<ShredStatus>,
    /// Data shreds received for the slot (not coding!)
    data_shreds: Vec<Option<Shred>>,
    /// array of bools that track which FEC set indexes have been already recovered
    already_recovered_fec_sets: Vec<bool>,
    /// array of bools that track which data shred indexes have been already deshredded
    already_deshredded: Vec<bool>,
}
impl Default for ShredsStateTracker {
    fn default() -> Self {
        Self {
            data_status: vec![ShredStatus::Unknown; MAX_DATA_SHREDS_PER_SLOT],
            data_shreds: vec![None; MAX_DATA_SHREDS_PER_SLOT],
            already_recovered_fec_sets: vec![false; MAX_DATA_SHREDS_PER_SLOT],
            already_deshredded: vec![false; MAX_DATA_SHREDS_PER_SLOT],
        }
    }
}

/// Returns the number of shreds reconstructed
/// Updates all_shreds with current state, and deshredded_entries with returned values
/// receive shreds per FEC set, attempting to recover the other shreds in the fec set so you do not have to wait until all data shreds have arrived.
/// every time a fec is recovered, scan for neighbouring DATA_COMPLETE_SHRED flags in the shreds, attempting to deserialize into solana entries when there are no missing shreds between the DATA_COMPLETE_SHRED flags.
/// note that an FEC set doesn't necessarily contain DATA_COMPLETE_SHRED in the last shred. when deserializing the bincode data, you must use data between shreds starting at the last DATA_COMPLETE_SHRED (not inclusive) to the next DATA_COMPLETE_SHRED (inclusive)
pub fn reconstruct_shreds(
    packet_batch: PacketBatch,
    all_shreds: &mut ahash::HashMap<
        Slot,
        (
            ahash::HashMap<u32 /* fec_set_index */, HashSet<ComparableShred>>,
            ShredsStateTracker,
        ),
    >,
    slot_fec_indexes_to_iterate: &mut Vec<(Slot, u32)>,
    deshredded_entries: &mut Vec<(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)>,
    highest_slot_seen: &mut Slot,
    rs_cache: &ReedSolomonCache,
    metrics: &ShredMetrics,
    filtered_tx_sender: &Option<&Sender<TxData>>,
) -> usize {
    deshredded_entries.clear();
    slot_fec_indexes_to_iterate.clear();
    // ingest all packets
    for packet in packet_batch.iter().filter_map(|p| p.data(..)) {
        match solana_ledger::shred::Shred::new_from_serialized_shred(packet.to_vec())
            .and_then(Shred::try_from)
        {
            Ok(shred) => {
                let slot = shred.common_header().slot;
                let index = shred.index() as usize;
                let fec_set_index = shred.fec_set_index();
                let (all_shreds, state_tracker) = all_shreds.entry(slot).or_default();
                if highest_slot_seen.saturating_sub(SLOT_LOOKBACK) > slot {
                    debug!(
                        "Old shred slot: {slot}, fec_set_index: {fec_set_index}, index: {index}"
                    );
                    continue;
                }
                if state_tracker.already_recovered_fec_sets[fec_set_index as usize]
                    || state_tracker.already_deshredded[index]
                {
                    debug!("Already completed slot: {slot}, fec_set_index: {fec_set_index}, index: {index}");
                    continue;
                }
                let Some(_shred_index) = update_state_tracker(&shred, state_tracker) else {
                    continue;
                };

                all_shreds
                    .entry(fec_set_index)
                    .or_default()
                    .insert(ComparableShred(shred));
                slot_fec_indexes_to_iterate.push((slot, fec_set_index)); // use Vec so we can sort to make sure if any earlier FEC sets have DATA_SHRED_COMPLETE, later entries can use the flag to find the bounds
                *highest_slot_seen = std::cmp::max(*highest_slot_seen, slot);
            }
            Err(e) => {
                if TraceShred::decode(packet).is_ok() {
                    continue;
                }
                warn!("Failed to decode shred. Err: {e:?}");
            }
        }
    }
    slot_fec_indexes_to_iterate.sort_unstable();
    slot_fec_indexes_to_iterate.dedup();

    // try recovering by FEC set
    // already checked if FEC set is completed or deserialized
    let mut total_recovered_count = 0;
    for (slot, fec_set_index) in slot_fec_indexes_to_iterate.iter() {
        let (all_shreds, state_tracker) = all_shreds.entry(*slot).or_default();
        let shreds = all_shreds.entry(*fec_set_index).or_default();
        let (
            num_expected_data_shreds,
            num_expected_coding_shreds,
            num_data_shreds,
            num_coding_shreds,
        ) = get_data_shred_info(shreds);

        // haven't received last data shred, haven't seen any coding shreds, so wait until more arrive
        let min_shreds_needed_to_recover = num_expected_data_shreds as usize;
        if num_expected_data_shreds == 0
            || shreds.len() < min_shreds_needed_to_recover
            || num_data_shreds == num_expected_data_shreds
        {
            continue;
        }

        // try to recover if we have enough shreds in the FEC set
        let merkle_shreds = shreds
            .iter()
            .sorted_by_key(|s| (u8::MAX - s.shred_type() as u8, s.index()))
            .map(|s| s.0.clone())
            .collect_vec();
        let recovered = match solana_ledger::shred::merkle::recover(merkle_shreds, rs_cache) {
            Ok(r) => r, // data shreds followed by code shreds (whatever was missing from to_deshred_payload)
            Err(e) => {
                warn!(
                    "Failed to recover shreds for slot {slot} fec_set_index {fec_set_index}. num_expected_data_shreds: {num_expected_data_shreds}, num_data_shreds: {num_data_shreds} num_expected_coding_shreds: {num_expected_coding_shreds} num_coding_shreds: {num_coding_shreds} Err: {e}",
                );
                continue;
            }
        };

        let mut fec_set_recovered_count = 0;
        for shred in recovered {
            match shred {
                Ok(shred) => {
                    if update_state_tracker(&shred, state_tracker).is_none() {
                        continue; // already seen before in state tracker
                    }
                    // shreds.insert(ComparableShred(shred)); // optional since all data shreds are in state_tracker
                    total_recovered_count += 1;
                    fec_set_recovered_count += 1;
                }
                Err(e) => warn!(
                    "Failed to recover shred for slot {slot}, fec set: {fec_set_index}. Err: {e}"
                ),
            }
        }

        if fec_set_recovered_count > 0 {
            debug!("recovered slot: {slot}, fec_index: {fec_set_index}, recovered count: {fec_set_recovered_count}");
            state_tracker.already_recovered_fec_sets[*fec_set_index as usize] = true;
            shreds.clear();
        }
    }

    // deshred and bincode deserialize
    for (slot, fec_set_index) in slot_fec_indexes_to_iterate.iter() {
        let (_all_shreds, state_tracker) = all_shreds.entry(*slot).or_default();
        let Some((start_data_complete_idx, end_data_complete_idx, unknown_start)) =
            get_indexes(state_tracker, *fec_set_index as usize)
        else {
            continue;
        };
        if unknown_start {
            metrics
                .unknown_start_position_count
                .fetch_add(1, Ordering::Relaxed);
        }

        let to_deshred =
            &state_tracker.data_shreds[start_data_complete_idx..=end_data_complete_idx];
        let deshredded_payload = match Shredder::deshred(
            to_deshred.iter().map(|s| s.as_ref().unwrap().payload()),
        ) {
            Ok(v) => v,
            Err(e) => {
                warn!("slot {slot} failed to deshred slot: {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}. Err: {e}");
                metrics
                    .fec_recovery_error_count
                    .fetch_add(1, Ordering::Relaxed);
                if unknown_start {
                    metrics
                        .unknown_start_position_error_count
                        .fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
        };

        let entries = match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(
            &deshredded_payload,
        ) {
            Ok(entries) => entries,
            Err(e) => {
                debug!(
                        "Failed to deserialize bincode payload of size {} for slot {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}, unknown_start: {unknown_start}. Err: {e}",
                        deshredded_payload.len()
                    );
                metrics
                    .bincode_deserialize_error_count
                    .fetch_add(1, Ordering::Relaxed);
                if unknown_start {
                    metrics
                        .unknown_start_position_error_count
                        .fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
        };
        metrics
            .entry_count
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        let txn_count = entries.iter().map(|e| e.transactions.len() as u64).sum();
        metrics.txn_count.fetch_add(txn_count, Ordering::Relaxed);
        debug!(
            "Successfully decoded slot: {slot} start_data_complete_idx: {start_data_complete_idx} end_data_complete_idx: {end_data_complete_idx} with entry count: {}, txn count: {txn_count}",
            entries.len(),
        );

        // Filter transactions from already-deserialized entries (no extra deserialization needed!)
        if let Some(sender) = filtered_tx_sender {
            filter_entries(*slot, &entries, sender);
        }

        deshredded_entries.push((*slot, entries, deshredded_payload));
        to_deshred.iter().for_each(|shred| {
            let Some(shred) = shred.as_ref() else {
                return;
            };
            state_tracker.already_recovered_fec_sets[shred.fec_set_index() as usize] = true;
            state_tracker.already_deshredded[shred.index() as usize] = true;
        })
    }

    if all_shreds.len() > MAX_PROCESSING_AGE {
        let slot_threshold = highest_slot_seen.saturating_sub(SLOT_LOOKBACK);
        let mut incomplete_fec_sets = ahash::HashMap::<Slot, Vec<_>>::default();
        let mut incomplete_fec_sets_count = 0;
        all_shreds.retain(|slot, (fec_set_indexes, state_tracker)| {
            if *slot >= slot_threshold {
                return true;
            }

            // count missing fec sets before clearing
            for (fec_set_index, shreds) in fec_set_indexes.iter() {
                if state_tracker.already_recovered_fec_sets[*fec_set_index as usize] {
                    continue;
                }
                let (
                    num_expected_data_shreds,
                    _num_expected_coding_shreds,
                    _num_data_shreds,
                    _num_coding_shreds,
                ) = get_data_shred_info(shreds);

                incomplete_fec_sets_count += 1;
                incomplete_fec_sets
                    .entry(*slot)
                    .and_modify(|fec_set_data| {
                        fec_set_data.push((*fec_set_index, num_expected_data_shreds, shreds.len()))
                    })
                    .or_insert_with(|| {
                        vec![(*fec_set_index, num_expected_data_shreds, shreds.len())]
                    });
            }

            false
        });
        if incomplete_fec_sets_count > 0 {
            incomplete_fec_sets
                .iter_mut()
                .for_each(|(_slot, fec_set_indexes)| fec_set_indexes.sort_unstable());
            datapoint_warn!(
                "shredstream_proxy-deshred_missed_fec_sets",
                (
                    "slot_fec_set_indexes",
                    format!("{:?}", incomplete_fec_sets.iter().sorted().collect_vec()),
                    String
                ),
                ("slot_count", incomplete_fec_sets.len(), i64),
                ("fec_set_count", incomplete_fec_sets_count, i64),
            );
        }
    }

    if total_recovered_count > 0 {
        metrics
            .recovered_count
            .fetch_add(total_recovered_count as u64, Ordering::Relaxed);
    }

    total_recovered_count
}

#[allow(unused)]
fn debug_remaining_shreds(
    all_shreds: &mut ahash::HashMap<
        Slot,
        (
            ahash::HashMap<u32, HashSet<ComparableShred>>,
            ShredsStateTracker,
        ),
    >,
) {
    let mut incomplete_fec_sets = ahash::HashMap::<Slot, Vec<_>>::default();
    let mut incomplete_fec_sets_count = 0;
    all_shreds
        .iter()
        .for_each(|(slot, (fec_set_indexes, state_tracker))| {
            // count missing fec sets before clearing
            for (fec_set_index, shreds) in fec_set_indexes.iter() {
                if state_tracker.already_recovered_fec_sets[*fec_set_index as usize] {
                    continue;
                }
                let (
                    num_expected_data_shreds,
                    _num_expected_coding_shreds,
                    _num_data_shreds,
                    _num_coding_shreds,
                ) = get_data_shred_info(shreds);

                incomplete_fec_sets_count += 1;
                incomplete_fec_sets
                    .entry(*slot)
                    .and_modify(|fec_set_data| {
                        fec_set_data.push((*fec_set_index, num_expected_data_shreds, shreds.len()))
                    })
                    .or_insert_with(|| {
                        vec![(*fec_set_index, num_expected_data_shreds, shreds.len())]
                    });
            }
        });
    incomplete_fec_sets
        .iter_mut()
        .for_each(|(_slot, fec_set_indexes)| fec_set_indexes.sort_unstable());
    println!("{:?}", incomplete_fec_sets.iter().sorted().collect_vec());
}

/// Return the inclusive range of shreds that constitute one complete segment: [0+ NotDataComplete, DataComplete]
/// Rules:
/// * A segment **ends** at the first `DataComplete` *at or after* `index`.
/// * It **starts** one position after the previous `DataComplete`, or at the beginning of the vector if there is none.
/// * If an `Unknown` is seen while searching towards the right, the segment is discarded and `None` is returned.
/// * We allow `Unknown` towards the left since sometimes entire FEC sets are not sent out
fn get_indexes(
    tracker: &ShredsStateTracker,
    index: usize,
) -> Option<(
    usize, /* start_data_complete_idx */
    usize, /* end_data_complete_idx */
    bool,  /* unknown start index */
)> {
    if index >= tracker.data_status.len() {
        return None;
    }

    // find the right boundary (first DataComplete ≥ index)
    let mut end = index;
    while end < tracker.data_status.len() {
        if tracker.already_deshredded[end] {
            return None;
        }
        match &tracker.data_status[end] {
            ShredStatus::Unknown => return None,
            ShredStatus::DataComplete => break,
            ShredStatus::NotDataComplete => end += 1,
        }
    }
    if end == tracker.data_status.len() {
        return None; // never saw a DataComplete
    }

    if end == 0 {
        return Some((0, 0, false)); // the vec *starts* with DataComplete
    }
    if index == 0 {
        return Some((0, end, false));
    }

    // find the left boundary (prev DataComplete + 1)
    let mut start = index;
    let mut next = start - 1;
    loop {
        match tracker.data_status[next] {
            ShredStatus::NotDataComplete => {
                if tracker.already_deshredded[next] {
                    return None; // already covered by some other iteration
                }
                if next == 0 {
                    return Some((0, end, false)); // no earlier DataComplete
                }
                start = next;
                next -= 1;
            }
            ShredStatus::DataComplete => return Some((start, end, false)),
            ShredStatus::Unknown => return Some((start, end, true)), // sometimes we don't have the previous starting shreds, make best guess
        }
    }
}

/// Upon receiving a new shred (either from recovery or receiving a UDP packet), update the state tracker
/// Returns shred index on new insert, None if already exists
fn update_state_tracker(shred: &Shred, state_tracker: &mut ShredsStateTracker) -> Option<usize> {
    let index = shred.index() as usize;
    if state_tracker.already_recovered_fec_sets[shred.fec_set_index() as usize] {
        return None;
    }
    if shred.shred_type() == ShredType::Data
        && (state_tracker.data_shreds[index].is_some()
            || !matches!(state_tracker.data_status[index], ShredStatus::Unknown))
    {
        return None;
    }
    if let Shred::ShredData(s) = &shred {
        state_tracker.data_shreds[index] = Some(shred.clone());
        if s.data_complete() || s.last_in_slot() {
            state_tracker.data_status[index] = ShredStatus::DataComplete;
        } else {
            state_tracker.data_status[index] = ShredStatus::NotDataComplete;
        }
    };
    Some(index)
}

const SLOT_LOOKBACK: Slot = 50;

/// check if we can reconstruct (having minimum number of data + coding shreds)
fn get_data_shred_info(
    shreds: &HashSet<ComparableShred>,
) -> (
    u16, /* num_expected_data_shreds */
    u16, /* num_expected_coding_shreds */
    u16, /* num_data_shreds */
    u16, /* num_coding_shreds */
) {
    let mut num_expected_data_shreds = 0;
    let mut num_expected_coding_shreds = 0;
    let mut num_data_shreds = 0;
    let mut num_coding_shreds = 0;
    for shred in shreds {
        match &shred.0 {
            Shred::ShredCode(s) => {
                num_coding_shreds += 1;
                num_expected_data_shreds = s.coding_header.num_data_shreds;
                num_expected_coding_shreds = s.coding_header.num_coding_shreds;
            }
            Shred::ShredData(s) => {
                num_data_shreds += 1;
                if num_expected_data_shreds == 0 && (s.data_complete() || s.last_in_slot()) {
                    num_expected_data_shreds =
                        (shred.0.index() - shred.0.fec_set_index()) as u16 + 1;
                }
            }
        }
    }
    (
        num_expected_data_shreds,
        num_expected_coding_shreds,
        num_data_shreds,
        num_coding_shreds,
    )
}

/// Issue: datashred equality comparison is wrong due to data size being smaller than the 1203 bytes allocated
#[derive(Clone, Debug, Eq)]
pub struct ComparableShred(Shred);

impl std::ops::Deref for ComparableShred {
    type Target = Shred;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for ComparableShred {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Shred::ShredCode(s) => {
                s.common_header.hash(state);
                s.coding_header.hash(state);
            }
            Shred::ShredData(s) => {
                s.common_header.hash(state);
                s.data_header.hash(state);
            }
        }
    }
}

impl PartialEq for ComparableShred {
    // Custom comparison to avoid random bytes that are part of payload
    fn eq(&self, other: &Self) -> bool {
        match &self.0 {
            Shred::ShredCode(s1) => match &other.0 {
                Shred::ShredCode(s2) => {
                    let solana_ledger::shred::ShredVariant::MerkleCode {
                        proof_size,
                        chained: _,
                        resigned,
                    } = s1.common_header.shred_variant
                    else {
                        return false;
                    };

                    // see https://github.com/jito-foundation/jito-solana/blob/d6c73374e3b4f863436e4b7d4d1ce5eea01cd262/ledger/src/shred/merkle.rs#L346, and re-add the proof component
                    let comparison_len =
                        <ShredCode as solana_ledger::shred::traits::Shred>::SIZE_OF_PAYLOAD
                            .saturating_sub(
                                usize::from(proof_size)
                                    * solana_ledger::shred::merkle::SIZE_OF_MERKLE_PROOF_ENTRY
                                    + if resigned {
                                        solana_ledger::shred::SIZE_OF_SIGNATURE
                                    } else {
                                        0
                                    },
                            );

                    s1.coding_header == s2.coding_header
                        && s1.common_header == s2.common_header
                        && s1.payload[..comparison_len] == s2.payload[..comparison_len]
                }
                Shred::ShredData(_) => false,
            },
            Shred::ShredData(s1) => match &other.0 {
                Shred::ShredCode(_) => false,
                Shred::ShredData(s2) => {
                    let Ok(s1_data) = solana_ledger::shred::layout::get_data(self.payload()) else {
                        return false;
                    };
                    let Ok(s2_data) = solana_ledger::shred::layout::get_data(other.payload())
                    else {
                        return false;
                    };
                    s1.data_header == s2.data_header
                        && s1.common_header == s2.common_header
                        && s1_data == s2_data
                }
            },
        }
    }
}
#[cfg(test)]
mod tests {
    use std::{
        collections::{hash_map::Entry, HashSet},
        io::{Read, Write},
        net::UdpSocket,
        sync::Arc,
    };

    use borsh::BorshDeserialize;
    use itertools::Itertools;
    use rand::Rng;
    use solana_ledger::{
        blockstore::make_slot_entries_with_transactions,
        shred::{merkle::Shred, ProcessShredsStats, ReedSolomonCache, ShredCommonHeader, Shredder},
    };
    use solana_perf::packet::{Packet, PacketBatch};
    use solana_sdk::{clock::Slot, hash::Hash, signature::Keypair};

    use crate::{
        deshred::{reconstruct_shreds, ComparableShred},
        forwarder::ShredMetrics,
    };

    /// For serializing packets to disk
    #[derive(borsh::BorshSerialize, borsh::BorshDeserialize, PartialEq, Debug)]
    struct Packets {
        pub packets: Vec<Vec<u8>>,
    }

    #[allow(unused)]
    fn listen_and_write_shreds() -> std::io::Result<()> {
        let socket = UdpSocket::bind("127.0.0.1:5000")?;
        println!("Listening on {}", socket.local_addr()?);

        let mut map = ahash::HashMap::<usize, usize>::default();
        let mut buf = [0u8; 1500];
        let mut vec = Packets {
            packets: Vec::new(),
        };

        let mut i = 0;
        loop {
            i += 1;
            match socket.recv_from(&mut buf) {
                Ok((amt, _src)) => {
                    vec.packets.push(buf[..amt].to_vec());
                    match map.entry(amt) {
                        Entry::Occupied(mut e) => *e.get_mut() += 1,
                        Entry::Vacant(e) => {
                            e.insert(1);
                        }
                    }
                    *map.get_mut(&amt).unwrap_or(&mut 0) += 1;
                }
                Err(e) => {
                    eprintln!("Error receiving data: {}", e);
                }
            }
            if i % 50000 == 0 {
                dbg!(&map);
                // size 1203 are data shreds: https://github.com/jito-foundation/jito-solana/blob/1742826fca975bd6d17daa5693abda861bbd2adf/ledger/src/shred/merkle.rs#L42
                // size 1228 are coding shreds: https://github.com/jito-foundation/jito-solana/blob/1742826fca975bd6d17daa5693abda861bbd2adf/ledger/src/shred/shred_code.rs#L16
                let mut file = std::fs::File::create("serialized_shreds.bin")?;
                file.write_all(&borsh::to_vec(&vec)?)?;
                return Ok(());
            }
        }
    }

    /// Helper function to compare all shred output
    #[allow(unused)]
    fn debug_to_disk(
        deshredded_entries: &[(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)],
        filepath: &str,
    ) {
        let entries = deshredded_entries
            .iter()
            .map(|(slot, entries, _entries_bytes)| (slot, entries))
            .into_group_map_by(|(slot, _entries)| *slot)
            .into_iter()
            .map(|(key, values)| {
                (
                    key,
                    values.into_iter().fold(Vec::new(), |mut acc, (_, v)| {
                        acc.extend(v);
                        acc
                    }),
                )
            })
            .map(|(slot, entries)| {
                let mut vec = entries
                    .iter()
                    .flat_map(|x| x.transactions.iter())
                    .map(|x| x.signatures[0])
                    .collect::<Vec<_>>();
                vec.sort();
                vec.dedup();
                (slot, vec)
            })
            .sorted_by_key(|x| x.0)
            .dedup_by(|lhs, rhs| lhs.0 == rhs.0)
            .collect_vec();
        let mut file = std::fs::File::create(filepath).unwrap();
        write!(file, "entries: {:#?}", &entries).unwrap();
    }
}
#[cfg(test)]
mod get_indexes_tests {
    use super::{get_indexes, ShredStatus, ShredsStateTracker};

    fn make_test_statustracker(statuses: &[ShredStatus]) -> ShredsStateTracker {
        let mut tracker = ShredsStateTracker::default();
        tracker.data_status[..statuses.len()].copy_from_slice(statuses);
        tracker
    }

    #[test]
    fn start_at_index_zero() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), Some((0, 2, false)));

        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), Some((0, 0, false)));

        let s = [
            ShredStatus::Unknown,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);
    }

    #[test]
    fn start_just_after_data_complete() {
        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 3, false)));
    }

    #[test]
    fn start_just_before_data_complete() {
        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 2, false)));
    }

    #[test]
    fn two_consecutive_data_complete() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((0, 1, false)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
    }

    #[test]
    fn three_consecutive_data_complete() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((0, 1, false)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
        assert_eq!(get_indexes(&tracker, 3), Some((3, 3, false)));
    }

    #[test]
    fn unknown_discards_segment() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::Unknown,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);

        let s = [
            ShredStatus::Unknown,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 2, true)));
    }

    #[test]
    fn test_unknown() {
        let s = [
            ShredStatus::Unknown,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 1, true)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
        assert_eq!(get_indexes(&tracker, 3), Some((3, 4, false)));
    }
}
