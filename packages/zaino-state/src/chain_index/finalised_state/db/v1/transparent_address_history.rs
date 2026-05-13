//! ZainoDB::V1 transparent address history indexing functionality.

use crate::chain_index::finalised_state::db::v1::TX_OUT_SET_INFO_ACCUMULATOR_KEY;
use crate::chain_index::types::db::metadata::{
    is_unspendable_tx_out, tx_out_set_entry_digest, FinalisedTxOutSetInfoAccumulator,
    ZAINO_TXOUTSET_ENTRY_LEN,
};

use super::*;

/// [`TransparentHistExt`] capability implementation for [`DbV1`].
///
/// Provides address history queries built over the LMDB `DUP_SORT`/`DUP_FIXED` address-history
/// database.
#[async_trait]
impl TransparentHistExt for DbV1 {
    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_records(
        &self,
        addr_script: AddrScript,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        self.addr_records(addr_script).await
    }

    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_and_index_records(
        &self,
        addr_script: AddrScript,
        tx_location: TxLocation,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        self.addr_and_index_records(addr_script, tx_location).await
    }

    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_tx_locations_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<TxLocation>>, FinalisedStateError> {
        self.addr_tx_locations_by_range(addr_script, start_height, end_height)
            .await
    }

    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_utxos_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<(TxLocation, u16, u64)>>, FinalisedStateError> {
        self.addr_utxos_by_range(addr_script, start_height, end_height)
            .await
    }

    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_balance_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<i64, FinalisedStateError> {
        self.addr_balance_by_range(addr_script, start_height, end_height)
            .await
    }

    async fn get_outpoint_spender(
        &self,
        outpoint: Outpoint,
    ) -> Result<Option<TxLocation>, FinalisedStateError> {
        self.get_outpoint_spender(outpoint).await
    }

    async fn get_outpoint_spenders(
        &self,
        outpoints: Vec<Outpoint>,
    ) -> Result<Vec<Option<TxLocation>>, FinalisedStateError> {
        self.get_outpoint_spenders(outpoints).await
    }

    async fn get_tx_out_set_info_accumulator(
        &self,
    ) -> Result<FinalisedTxOutSetInfoAccumulator, FinalisedStateError> {
        self.get_tx_out_set_info_accumulator().await
    }
}

impl DbV1 {
    // *** Public fetcher methods - Used by DbReader ***

    /// Fetch all address history records for a given transparent address.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more valid records exist,
    /// - `Ok(None)` if no records exist (not an error),
    /// - `Err(...)` if any decoding or DB error occurs.
    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_records(
        &self,
        addr_script: AddrScript,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            let mut raw_records = Vec::new();

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN {
                    continue;
                }
                if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                    continue;
                }
                raw_records.push(val.to_vec());
            }

            if raw_records.is_empty() {
                return Ok(None);
            }

            let mut records = Vec::with_capacity(raw_records.len());
            for val in raw_records {
                let entry = StoredEntryFixed::<AddrEventBytes>::from_bytes(&val).map_err(|e| {
                    FinalisedStateError::Custom(format!("addrhist decode error: {e}"))
                })?;
                records.push(entry.item);
            }

            Ok(Some(records))
        })
    }

    /// Fetch all address history records for a given address and TxLocation.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more matching records are found at that index,
    /// - `Ok(None)` if no matching records exist (not an error),
    /// - `Err(...)` on decode or DB failure.
    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_and_index_records(
        &self,
        addr_script: AddrScript,
        tx_location: TxLocation,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        let rec_results = tokio::task::block_in_place(|| {
            let ro = self.env.begin_ro_txn()?;
            let fetch_records_result =
                self.addr_hist_records_by_addr_and_index_in_txn(&ro, &addr_bytes, tx_location);
            ro.commit()?;
            fetch_records_result
        });

        let raw_records = match rec_results {
            Ok(records) => records,
            Err(FinalisedStateError::LmdbError(lmdb::Error::NotFound)) => return Ok(None),
            Err(e) => return Err(e),
        };

        if raw_records.is_empty() {
            return Ok(None);
        }

        let mut records = Vec::with_capacity(raw_records.len());

        for val in raw_records {
            let entry = StoredEntryFixed::<AddrEventBytes>::from_bytes(&val)
                .map_err(|e| FinalisedStateError::Custom(format!("addrhist decode error: {e}")))?;
            records.push(entry.item);
        }

        Ok(Some(records))
    }

    /// Fetch all distinct `TxLocation` values for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more matching records are found,
    /// - `Ok(None)` if no matches found (not an error),
    /// - `Err(...)` on decode or DB failure.
    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_tx_locations_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<TxLocation>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };
            let mut set: HashSet<TxLocation> = HashSet::new();

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN
                    || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                {
                    continue;
                }

                // Parse the tx_location out of val:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let block_height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
                if block_height < start_height.0 || block_height > end_height.0 {
                    continue;
                }

                let tx_index = u16::from_be_bytes([val[6], val[7]]);
                set.insert(TxLocation::new(block_height, tx_index));
            }
            let mut indices: Vec<_> = set.into_iter().collect();
            indices.sort_by_key(|txi| (txi.block_height(), txi.tx_index()));

            if indices.is_empty() {
                Ok(None)
            } else {
                Ok(Some(indices))
            }
        })
    }

    /// Fetch all UTXOs (unspent mined outputs) for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Each entry is `(TxLocation, vout, value)`.
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more UTXOs are found,
    /// - `Ok(None)` if none found (not an error),
    /// - `Err(...)` on decode or DB failure.
    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_utxos_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<(TxLocation, u16, u64)>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };
            let mut utxos = Vec::new();

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN
                    || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                {
                    continue;
                }

                // Parse the tx_location out of val:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let block_height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
                if block_height < start_height.0 || block_height > end_height.0 {
                    continue;
                }

                let flags = val[10];
                if (flags & AddrEventBytes::FLAG_MINED == 0)
                    || (flags & AddrEventBytes::FLAG_SPENT != 0)
                {
                    continue;
                }

                let tx_index = u16::from_be_bytes([val[6], val[7]]);
                let vout = u16::from_be_bytes([val[8], val[9]]);
                let value = u64::from_le_bytes([
                    val[11], val[12], val[13], val[14], val[15], val[16], val[17], val[18],
                ]);

                utxos.push((TxLocation::new(block_height, tx_index), vout, value));
            }

            if utxos.is_empty() {
                Ok(None)
            } else {
                Ok(Some(utxos))
            }
        })
    }

    /// Computes the transparent balance change for `addr_script` over the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Includes:
    /// - `+value` for mined outputs
    /// - `−value` for spent inputs
    ///
    /// Returns the signed net value as `i64`, or error on failure.
    #[cfg(feature = "transparent_address_history_experimental")]
    async fn addr_balance_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<i64, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => {
                    return Err(FinalisedStateError::DataUnavailable(
                        "no data for address".to_string(),
                    ))
                }
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            let mut balance: i64 = 0;

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => {
                    return Err(FinalisedStateError::DataUnavailable(
                        "no data for address".to_string(),
                    ))
                }
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN
                    || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                {
                    continue;
                }

                // Parse the tx_location out of val:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
                if height < start_height.0 || height > end_height.0 {
                    continue;
                }

                let flags = val[10];
                let value = u64::from_le_bytes([
                    val[11], val[12], val[13], val[14], val[15], val[16], val[17], val[18],
                ]) as i64;

                if flags & AddrEventBytes::FLAG_IS_INPUT != 0 {
                    balance -= value;
                } else if flags & AddrEventBytes::FLAG_MINED != 0 {
                    balance += value;
                }
            }

            Ok(balance)
        })
    }

    /// Fetch the `TxLocation` that spent a given outpoint, if any.
    ///
    /// Returns:
    /// - `Ok(Some(TxLocation))` if the outpoint is spent.
    /// - `Ok(None)` if no entry exists (not spent or not known).
    /// - `Err(...)` on deserialization or DB error.
    async fn get_outpoint_spender(
        &self,
        outpoint: Outpoint,
    ) -> Result<Option<TxLocation>, FinalisedStateError> {
        let key = outpoint.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        tokio::task::block_in_place(|| match txn.get(self.spent, &key) {
            Ok(bytes) => {
                let entry = StoredEntryFixed::<TxLocation>::from_bytes(bytes).map_err(|e| {
                    FinalisedStateError::Custom(format!("spent entry decode error: {e}"))
                })?;
                Ok(Some(entry.item))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(FinalisedStateError::LmdbError(e)),
        })
    }

    /// Fetch the `TxLocation` entries for a batch of outpoints.
    ///
    /// For each input:
    /// - Returns `Some(TxLocation)` if spent,
    /// - `None` if not found,
    /// - or returns `Err` immediately if any DB or decode error occurs.
    async fn get_outpoint_spenders(
        &self,
        outpoints: Vec<Outpoint>,
    ) -> Result<Vec<Option<TxLocation>>, FinalisedStateError> {
        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            outpoints
                .into_iter()
                .map(|outpoint| {
                    let key = outpoint.to_bytes()?;
                    match txn.get(self.spent, &key) {
                        Ok(bytes) => {
                            let entry =
                                StoredEntryFixed::<TxLocation>::from_bytes(bytes).map_err(|e| {
                                    FinalisedStateError::Custom(format!(
                                        "spent entry decode error for {outpoint:?}: {e}"
                                    ))
                                })?;
                            Ok(Some(entry.item))
                        }
                        Err(lmdb::Error::NotFound) => Ok(None),
                        Err(e) => Err(FinalisedStateError::LmdbError(e)),
                    }
                })
                .collect()
        })
    }

    /// Returns the finalised-state txout-set accumulator.
    ///
    /// This reads the singleton accumulator entry. It does not compute or repair the accumulator;
    /// accumulator creation, backfill, and updates are handled by migrations and write paths.
    async fn get_tx_out_set_info_accumulator(
        &self,
    ) -> Result<FinalisedTxOutSetInfoAccumulator, FinalisedStateError> {
        tokio::task::block_in_place(|| {
            let transaction = self.env.begin_ro_txn()?;

            let raw_accumulator = match transaction.get(
                self.tx_out_set_info_accumulator,
                &TX_OUT_SET_INFO_ACCUMULATOR_KEY,
            ) {
                Ok(value) => value,
                Err(lmdb::Error::NotFound) => {
                    return Err(FinalisedStateError::DataUnavailable(
                        "finalised txout-set accumulator missing from database".to_string(),
                    ));
                }
                Err(error) => return Err(FinalisedStateError::LmdbError(error)),
            };

            let accumulator_entry =
                StoredEntryFixed::<FinalisedTxOutSetInfoAccumulator>::from_bytes(raw_accumulator)
                    .map_err(|error| {
                    FinalisedStateError::Custom(format!(
                        "txout-set accumulator decode error: {error}"
                    ))
                })?;

            if !accumulator_entry.verify(TX_OUT_SET_INFO_ACCUMULATOR_KEY) {
                return Err(FinalisedStateError::Custom(
                    "txout-set accumulator checksum mismatch".to_string(),
                ));
            }

            Ok(accumulator_entry.item)
        })
    }

    // *** Internal DB methods ***

    /// Returns all raw AddrHist records for a given AddrScript and TxLocation.
    ///
    /// Returns a Vec of serialized entries, for given addr_script and ix_index.
    ///
    /// Efficiently filters by matching block + tx index bytes in-place.
    ///
    /// WARNING: This operates *inside* an existing RO txn.
    #[cfg(feature = "transparent_address_history_experimental")]
    pub(super) fn addr_hist_records_by_addr_and_index_in_txn(
        &self,
        txn: &lmdb::RoTransaction<'_>,
        addr_script_bytes: &[u8],
        tx_location: TxLocation,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        // Open a single cursor.
        let cursor = txn.open_ro_cursor(self.address_history)?;
        let mut results: Vec<Vec<u8>> = Vec::new();

        // Build the seek data prefix that matches the stored bytes:
        // [StoredEntry version, record version, height_be(4), tx_index_be(2)]
        let stored_entry_tag = StoredEntryFixed::<AddrEventBytes>::VERSION;
        let record_tag = AddrEventBytes::VERSION;

        // Reserve the exact number of bytes we need for the SET_RANGE value prefix:
        //
        //  - 1 byte: outer StoredEntry version (StoredEntryFixed::<AddrEventBytes>::VERSION)
        //  - 1 byte: inner record version (AddrEventBytes::VERSION)
        //  - 4 bytes: block_height  (big-endian)
        //  - 2 bytes: tx_index     (big-endian)
        //
        // This minimal prefix (2 + 4 + 2 = 8 bytes) is all we need for MDB_SET_RANGE to
        // position at the first duplicate whose value >= (height, tx_index). Using
        // `with_capacity` avoids reallocations while we build the prefix.  We do *not*
        // append vout/flags/value/checksum here because we only need the leading bytes
        // to seek into the dup-sorted data.
        let mut seek_data = Vec::with_capacity(2 + 4 + 2);
        seek_data.push(stored_entry_tag);
        seek_data.push(record_tag);
        seek_data.extend_from_slice(&tx_location.block_height().to_be_bytes());
        seek_data.extend_from_slice(&tx_location.tx_index().to_be_bytes());

        // Use MDB_SET_RANGE to position the cursor at the first duplicate for this key whose
        // duplicate value is >= seek_data (this is the efficient B-tree seek).
        let op_set_range = lmdb_sys::MDB_SET_RANGE;
        match cursor.get(Some(addr_script_bytes), Some(&seek_data[..]), op_set_range) {
            Ok((maybe_key, mut cur_val)) => {
                // If there's no key, nothing to do
                let mut cur_key = match maybe_key {
                    Some(k) => k,
                    None => return Ok(results),
                };

                // If the seek landed on a different key, there are no candidates for this addr.
                if cur_key.len() != AddrScript::VERSIONED_LEN
                    || &cur_key[..AddrScript::VERSIONED_LEN] != addr_script_bytes
                {
                    return Ok(results);
                }

                // Iterate from the positioned duplicate forward using MDB_NEXT_DUP.
                let op_next_dup = lmdb_sys::MDB_NEXT_DUP;

                loop {
                    // Validate lengths, same as original function.
                    if cur_key.len() != AddrScript::VERSIONED_LEN {
                        return Err(FinalisedStateError::Custom(
                            "address history key length mismatch".into(),
                        ));
                    }
                    if cur_val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                        return Err(FinalisedStateError::Custom(
                            "address history value length mismatch".into(),
                        ));
                    }
                    if cur_val[0] != StoredEntryFixed::<AddrEventBytes>::VERSION
                        || cur_val[1] != AddrEventBytes::VERSION
                    {
                        return Err(FinalisedStateError::Custom(
                            "address history value version tag mismatch".into(),
                        ));
                    }

                    // Read height and tx_index *in-place* from the value bytes:
                    // - [0] stored entry tag
                    // - [1] record tag
                    // - [2..=5] height (BE)
                    // - [6..=7] tx_index (BE)
                    let block_index =
                        u32::from_be_bytes([cur_val[2], cur_val[3], cur_val[4], cur_val[5]]);
                    let tx_idx = u16::from_be_bytes([cur_val[6], cur_val[7]]);

                    if block_index == tx_location.block_height() && tx_idx == tx_location.tx_index()
                    {
                        // Matching entry — collect the full stored entry bytes (same behaviour).
                        results.push(cur_val.to_vec());
                    } else if block_index > tx_location.block_height()
                        || (block_index == tx_location.block_height()
                            && tx_idx > tx_location.tx_index())
                    {
                        // We've passed the requested tx_location in duplicate ordering -> stop
                        // (duplicates are ordered by value, so once we pass, no matches remain).
                        break;
                    }

                    // Advance to the next duplicate for the same key.
                    match cursor.get(None, None, op_next_dup) {
                        Ok((maybe_k, next_val)) => {
                            // If key changed or no key returned, stop.
                            let k = match maybe_k {
                                Some(k) => k,
                                None => break,
                            };
                            if k.len() != AddrScript::VERSIONED_LEN
                                || &k[..AddrScript::VERSIONED_LEN] != addr_script_bytes
                            {
                                break;
                            }
                            // Update cur_key and cur_val and continue.
                            cur_key = k;
                            cur_val = next_val;
                            continue;
                        }
                        Err(lmdb::Error::NotFound) => break,
                        Err(e) => return Err(e.into()),
                    }
                } // loop
            }
            Err(lmdb::Error::NotFound) => {
                // Nothing at or after seek -> empty result
            }
            Err(e) => return Err(e.into()),
        }

        Ok(results)
    }

    /// Inserts a mined-output record into the address‐history map.
    #[cfg(feature = "transparent_address_history_experimental")]
    #[inline]
    pub(super) fn build_transaction_output_histories<'a>(
        map: &mut HashMap<AddrScript, Vec<AddrHistRecord>>,
        tx_location: TxLocation,
        outputs: impl Iterator<Item = (usize, &'a TxOutCompact)>,
    ) {
        for (output_idx, output) in outputs {
            let addr_script = AddrScript::new(*output.script_hash(), output.script_type());
            let output_record = AddrHistRecord::new(
                tx_location,
                output_idx as u16,
                output.value(),
                AddrHistRecord::FLAG_MINED,
            );
            map.entry(addr_script)
                .and_modify(|v| v.push(output_record))
                .or_insert_with(|| vec![output_record]);
        }
    }

    /// Inserts both the “spend” record and the “mined” previous‐output record
    /// (used to update the output record spent in this transaction).
    #[cfg(feature = "transparent_address_history_experimental")]
    #[inline]
    #[allow(clippy::type_complexity)]
    pub(super) fn build_input_history(
        map: &mut HashMap<AddrScript, Vec<(AddrHistRecord, (AddrScript, AddrHistRecord))>>,
        input_tx_location: TxLocation,
        input_index: u16,
        input: &TxInCompact,
        prev_output: &TxOutCompact,
        prev_output_tx_location: TxLocation,
    ) {
        let addr_script = AddrScript::new(*prev_output.script_hash(), prev_output.script_type());
        let input_record = AddrHistRecord::new(
            input_tx_location,
            input_index,
            prev_output.value(),
            AddrHistRecord::FLAG_IS_INPUT,
        );
        let prev_output_record = (
            AddrScript::new(*prev_output.script_hash(), prev_output.script_type()),
            AddrHistRecord::new(
                prev_output_tx_location,
                input.prevout_index() as u16,
                prev_output.value(),
                AddrHistRecord::FLAG_MINED,
            ),
        );
        map.entry(addr_script)
            .and_modify(|v| v.push((input_record, prev_output_record)))
            .or_insert_with(|| vec![(input_record, prev_output_record)]);
    }

    /// Delete all `addrhist` duplicates for `addr_bytes` that
    ///   * belong to `block_height`, **and**
    ///   * match the requested record type(s).
    ///
    /// * `delete_inputs`  – remove records whose flag-byte contains FLAG_IS_INPUT
    /// * `delete_outputs` – remove records whose flag-byte contains FLAG_MINED
    ///
    /// `expected` is the number of records to delete;
    ///
    /// WARNING: This operates *inside* an existing RW txn and must **not** commit it.
    #[cfg(feature = "transparent_address_history_experimental")]
    pub(super) fn delete_addrhist_dups_in_txn(
        &self,
        txn: &mut lmdb::RwTransaction<'_>,
        addr_bytes: &[u8],
        block_height: Height,
        delete_inputs: bool,
        delete_outputs: bool,
        expected: usize,
    ) -> Result<(), FinalisedStateError> {
        if !delete_inputs && !delete_outputs {
            return Err(FinalisedStateError::Custom(
                "called delete_addrhist_dups with neither inputs nor outputs to delete".into(),
            ));
        }
        if expected == 0 {
            return Err(FinalisedStateError::Custom(
                "called delete_addrhist_dups with 0 expected deletes".into(),
            ));
        }

        let mut remaining = expected;
        let height_be = block_height.0.to_be_bytes();

        let mut cur = txn.open_rw_cursor(self.address_history)?;

        match cur
            .get(Some(addr_bytes), None, lmdb_sys::MDB_SET_KEY)
            .and_then(|_| cur.get(None, None, lmdb_sys::MDB_LAST_DUP))
        {
            Ok((_k, mut val)) => loop {
                // Parse AddrEventBytes:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum
                if val.len() == StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                    && val[2..6] == height_be
                {
                    let flags = val[10];
                    let is_input = flags & AddrEventBytes::FLAG_IS_INPUT != 0;
                    let is_output = flags & AddrEventBytes::FLAG_MINED != 0;

                    if (delete_inputs && is_input) || (delete_outputs && is_output) {
                        cur.del(WriteFlags::empty())?;
                        remaining -= 1;
                        if remaining == 0 {
                            break;
                        }
                    }
                } else if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                    tracing::warn!("bad addrhist dup (len={})", val.len());
                }

                // step backwards through duplicates
                match cur.get(None, None, lmdb_sys::MDB_PREV_DUP) {
                    Ok((_k, v)) => val = v,
                    Err(lmdb::Error::NotFound) => {
                        if remaining == 0 {
                            break;
                        }
                        return Err(FinalisedStateError::Custom(format!(
                            "expected {expected} records, deleted {}",
                            expected - remaining
                        )));
                    }
                    Err(e) => return Err(FinalisedStateError::LmdbError(e)),
                }
            },
            Err(lmdb::Error::NotFound) => {
                return Err(FinalisedStateError::Custom(
                    "no addrhist record for key".into(),
                ));
            }
            Err(e) => return Err(FinalisedStateError::LmdbError(e)),
        }

        drop(cur);
        Ok(())
    }

    /// Mark a specific AddrHistRecord as spent in the addrhist DB.
    /// Looks up a record by script and tx_location, sets FLAG_SPENT, and updates it in place.
    ///
    /// Returns Ok(true) if a record was updated, Ok(false) if not found, or Err on DB error.
    ///
    /// WARNING: This operates *inside* an existing RW txn and must **not** commit it.
    #[cfg(feature = "transparent_address_history_experimental")]
    pub(super) fn mark_addr_hist_record_spent_in_txn(
        &self,
        txn: &mut lmdb::RwTransaction<'_>,
        addr_script: &AddrScript,

        expected_prev_entry_bytes: &[u8],
    ) -> Result<bool, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        let mut cur = txn.open_rw_cursor(self.address_history)?;

        for (key, val) in cur.iter_dup_of(&addr_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN {
                return Err(FinalisedStateError::Custom(
                    "address history key length mismatch".into(),
                ));
            }
            if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                return Err(FinalisedStateError::Custom(
                    "address history value length mismatch".into(),
                ));
            }

            if val != expected_prev_entry_bytes {
                continue;
            }

            let mut hist_record = [0u8; StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN];
            hist_record.copy_from_slice(val);

            let flags = hist_record[10];
            if (flags & AddrHistRecord::FLAG_IS_INPUT) != 0 {
                return Err(FinalisedStateError::Custom(
                    "attempt to mark an input-row as spent".into(),
                ));
            }
            // idempotent
            if (flags & AddrHistRecord::FLAG_SPENT) != 0 {
                return Ok(true);
            }

            if (flags & AddrHistRecord::FLAG_MINED) == 0 {
                return Err(FinalisedStateError::Custom(
                    "attempt to mark non-mined addrhist record as spent".into(),
                ));
            }

            hist_record[10] |= AddrHistRecord::FLAG_SPENT;

            let checksum = StoredEntryFixed::<AddrEventBytes>::blake2b256(
                &[&addr_bytes, &hist_record[1..19]].concat(),
            );
            hist_record[19..51].copy_from_slice(&checksum);

            cur.put(&addr_bytes, &hist_record, WriteFlags::CURRENT)?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Mark a specific AddrHistRecord as unspent in the addrhist DB.
    /// Looks up a record by script and tx_location, sets FLAG_SPENT, and updates it in place.
    ///
    /// Returns Ok(true) if a record was updated, Ok(false) if not found, or Err on DB error.
    ///
    /// WARNING: This operates *inside* an existing RW txn and must **not** commit it.
    #[cfg(feature = "transparent_address_history_experimental")]
    pub(super) fn mark_addr_hist_record_unspent_in_txn(
        &self,
        txn: &mut lmdb::RwTransaction<'_>,
        addr_script: &AddrScript,

        expected_prev_entry_bytes: &[u8],
    ) -> Result<bool, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        let mut cur = txn.open_rw_cursor(self.address_history)?;

        for (key, val) in cur.iter_dup_of(&addr_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN {
                return Err(FinalisedStateError::Custom(
                    "address history key length mismatch".into(),
                ));
            }
            if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                return Err(FinalisedStateError::Custom(
                    "address history value length mismatch".into(),
                ));
            }

            if val != expected_prev_entry_bytes {
                continue;
            }

            // we've located the exact duplicate bytes we built earlier.
            let mut hist_record = [0u8; StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN];
            hist_record.copy_from_slice(val);

            // parse flags (located at byte index 10 in the StoredEntry layout)
            let flags = hist_record[10];

            // Sanity: the record we intend to mark should be a mined output (not an input).
            if (flags & AddrHistRecord::FLAG_IS_INPUT) != 0 {
                return Err(FinalisedStateError::Custom(
                    "attempt to mark an input-row as unspent".into(),
                ));
            }

            // If it's already unspent, treat as successful (idempotent).
            if (flags & AddrHistRecord::FLAG_SPENT) == 0 {
                drop(cur);
                return Ok(true);
            }

            // If the record is not marked MINED, that's an invariant failure.
            // We surface it rather than producing a non-mined record.
            if (flags & AddrHistRecord::FLAG_MINED) == 0 {
                return Err(FinalisedStateError::Custom(
                    "attempt to mark non-mined addrhist record as unspent".into(),
                ));
            }

            // Preserve all existing flags (including MINED), and remove SPENT.
            hist_record[10] &= !AddrHistRecord::FLAG_SPENT;

            // Recompute checksum over entry header + payload (bytes 1..19).
            let checksum = StoredEntryFixed::<AddrEventBytes>::blake2b256(
                &[&addr_bytes, &hist_record[1..19]].concat(),
            );
            hist_record[19..51].copy_from_slice(&checksum);

            // Write back in place for the exact duplicate we matched.
            cur.put(&addr_bytes, &hist_record, WriteFlags::CURRENT)?;
            drop(cur);

            return Ok(true);
        }

        Ok(false)
    }

    /// Fetches the previous transparent output for the given outpoint.
    /// Returns `TxOutCompact` or an explicit error if not found or invalid.
    ///
    /// Used to build addrhist records.
    ///
    /// WARNING: This is a blocking function and **MUST** be called within a blocking thread / task.
    pub(crate) fn get_previous_output_blocking(
        &self,
        outpoint: Outpoint,
    ) -> Result<TxOutCompact, FinalisedStateError> {
        // Find the tx’s location in the chain
        let prev_txid = TransactionHash::from(*outpoint.prev_txid());
        let tx_location = self
            .find_txid_index_blocking(&prev_txid)?
            .ok_or_else(|| FinalisedStateError::Custom("Previous txid not found".into()))?;

        // Fetch the output from the transparent db.
        let block_height = tx_location.block_height();
        let tx_index = tx_location.tx_index() as usize;
        let out_index = outpoint.prev_index() as usize;

        let ro = self.env.begin_ro_txn()?;
        let height_key = Height(block_height).to_bytes()?;
        let stored_bytes = ro.get(self.transparent, &height_key)?;

        Self::find_txout_in_stored_transparent_tx_list(stored_bytes, tx_index, out_index)
            .ok_or_else(|| {
                FinalisedStateError::Custom("Previous output not found at given index".into())
            })
    }

    /// Efficiently scans a raw `StoredEntryVar<TransparentTxList>` buffer to locate the
    /// specific output at [tx_idx, output_idx] without full deserialization.
    ///
    /// # Arguments
    /// - `stored`: the raw LMDB byte buffer
    /// - `target_tx_idx`: index in the tx list
    /// - `target_output_idx`: index in the outputs of that tx
    ///
    /// # Returns
    /// - `Some(TxOutCompact)` if found and present, otherwise `None`
    #[inline]
    fn find_txout_in_stored_transparent_tx_list(
        stored: &[u8],
        target_tx_idx: usize,
        target_output_idx: usize,
    ) -> Option<TxOutCompact> {
        const CHECKSUM_LEN: usize = 32;

        if stored.len() < TransactionHash::VERSION_TAG_LEN + 8 + CHECKSUM_LEN {
            return None;
        }

        let mut cursor = &stored[TransactionHash::VERSION_TAG_LEN..];
        let item_len = CompactSize::read(&mut cursor).ok()? as usize;
        if cursor.len() < item_len + CHECKSUM_LEN {
            return None;
        }

        let (_record_version, mut remaining) = cursor.split_first()?;
        let vec_len = CompactSize::read(&mut remaining).ok()? as usize;

        for i in 0..vec_len {
            let (option_tag, rest) = remaining.split_first()?;
            remaining = rest;

            if *option_tag == 0 {
                // None: nothing to skip, go to next
                if i == target_tx_idx {
                    return None;
                }
            } else if *option_tag == 1 {
                let (_tx_version, rest) = remaining.split_first()?;
                remaining = rest;

                let vin_len = CompactSize::read(&mut remaining).ok()? as usize;

                for _ in 0..vin_len {
                    if remaining.len() < TxInCompact::VERSIONED_LEN {
                        return None;
                    }
                    remaining = &remaining[TxInCompact::VERSIONED_LEN..];
                }

                let vout_len = CompactSize::read(&mut remaining).ok()? as usize;

                for out_idx in 0..vout_len {
                    if remaining.len() < TxOutCompact::VERSIONED_LEN {
                        return None;
                    }

                    let out_bytes = &remaining[..TxOutCompact::VERSIONED_LEN];

                    if i == target_tx_idx && out_idx == target_output_idx {
                        return TxOutCompact::from_bytes(out_bytes).ok();
                    }

                    remaining = &remaining[TxOutCompact::VERSIONED_LEN..];
                }
            } else {
                // Non-canonical Option tag
                return None;
            }
        }
        None
    }

    /// Applies a list of UTXO entries to the multiset commitment fields of the accumulator.
    ///
    /// For each entry the digest is XORed into `hash_serialized` (XOR is self-inverse, so the same
    /// call site works for both add and remove). The integer fields `total_zatoshis` and
    /// `bytes_serialized` move in the direction selected by `adding`.
    fn apply_tx_out_set_entries_delta(
        accumulator: &mut FinalisedTxOutSetInfoAccumulator,
        entries: &[(Outpoint, TxOutCompact)],
        adding: bool,
    ) -> Result<(), FinalisedStateError> {
        for (outpoint, out) in entries {
            let digest = tx_out_set_entry_digest(outpoint, out);
            for (dst, src) in accumulator.hash_serialized.iter_mut().zip(digest.iter()) {
                *dst ^= *src;
            }

            if adding {
                accumulator.total_zatoshis =
                    accumulator
                        .total_zatoshis
                        .checked_add(out.value())
                        .ok_or_else(|| {
                            FinalisedStateError::Custom(
                                "txout-set accumulator total_zatoshis overflow".to_string(),
                            )
                        })?;
                accumulator.bytes_serialized = accumulator
                    .bytes_serialized
                    .checked_add(ZAINO_TXOUTSET_ENTRY_LEN)
                    .ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator bytes_serialized overflow".to_string(),
                        )
                    })?;
            } else {
                accumulator.total_zatoshis =
                    accumulator
                        .total_zatoshis
                        .checked_sub(out.value())
                        .ok_or_else(|| {
                            FinalisedStateError::Custom(
                                "txout-set accumulator total_zatoshis underflow".to_string(),
                            )
                        })?;
                accumulator.bytes_serialized = accumulator
                    .bytes_serialized
                    .checked_sub(ZAINO_TXOUTSET_ENTRY_LEN)
                    .ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator bytes_serialized underflow".to_string(),
                        )
                    })?;
            }
        }
        Ok(())
    }

    /// Resolves each spent outpoint to its previous [`TxOutCompact`].
    ///
    /// Same-block spends are resolved from the in-block `transparent` slice via the
    /// `txid_to_block_index` map. Prior-block spends are resolved via
    /// [`DbV1::get_previous_output_blocking`] inside a `block_in_place` to honour the read/write
    /// boundary requirements documented on that method.
    fn resolve_spent_outpoints_for_set_info(
        &self,
        spent_map: &HashMap<Outpoint, TxLocation>,
        txid_to_block_index: &HashMap<TransactionHash, usize>,
        transparent: &[Option<TransparentCompactTx>],
    ) -> Result<Vec<(Outpoint, TxOutCompact)>, FinalisedStateError> {
        let mut resolved = Vec::with_capacity(spent_map.len());

        for outpoint in spent_map.keys().copied() {
            let prev_txid = TransactionHash::from(*outpoint.prev_txid());
            let prev_index = outpoint.prev_index() as usize;

            let prev_out = if let Some(block_tx_index) = txid_to_block_index.get(&prev_txid) {
                let tx = transparent[*block_tx_index].as_ref().ok_or_else(|| {
                    FinalisedStateError::Custom(format!(
                        "txout-set accumulator cannot be calculated: same-block spend of {prev_txid:?} has no transparent transaction data"
                    ))
                })?;
                *tx.outputs().get(prev_index).ok_or_else(|| {
                    FinalisedStateError::Custom(format!(
                        "txout-set accumulator cannot be calculated: same-block spend of {prev_txid:?} index {prev_index} out of range"
                    ))
                })?
            } else {
                tokio::task::block_in_place(|| self.get_previous_output_blocking(outpoint))?
            };

            resolved.push((outpoint, prev_out));
        }

        Ok(resolved)
    }

    /// Calculates the finalised txout-set accumulator after applying the block currently being written.
    ///
    /// This method uses the data already built by `write_block`:
    /// - `txids`: block-local transaction hashes in transaction-index order.
    /// - `transparent`: block-local transparent transaction data in transaction-index order.
    /// - `spent_map`: distinct transparent outpoints spent by this block.
    ///
    /// Missing accumulator data is only valid for a completely empty database before writing genesis.
    /// In every other case, a missing accumulator is treated as database corruption / failed migration.
    ///
    /// The returned accumulator must be written inside the same LMDB write transaction as the block.
    pub(crate) async fn calculate_tx_out_set_info_accumulator_after_block(
        &self,
        block_height: Height,
        txids: &[TransactionHash],
        transparent: &[Option<TransparentCompactTx>],
        spent_map: &HashMap<Outpoint, TxLocation>,
    ) -> Result<FinalisedTxOutSetInfoAccumulator, FinalisedStateError> {
        // The block-local transaction arrays must stay index-aligned.
        if txids.len() != transparent.len() {
            return Err(FinalisedStateError::Custom(format!(
            "txout-set accumulator cannot be calculated: txid count ({}) does not match transparent transaction count ({})",
            txids.len(),
            transparent.len()
        )));
        }

        // Load the existing accumulator. Only a fresh empty DB writing genesis may start from zero.
        let mut accumulator =
            match <Self as TransparentHistExt>::get_tx_out_set_info_accumulator(self).await {
                Ok(accumulator) => accumulator,
                Err(FinalisedStateError::DataUnavailable(_)) => {
                    let current_tip = self.tip_height().await?;

                    if current_tip.is_none() && block_height == GENESIS_HEIGHT {
                        FinalisedTxOutSetInfoAccumulator::empty()
                    } else {
                        return Err(FinalisedStateError::Custom(
                            "txout-set accumulator missing from non-empty database".to_string(),
                        ));
                    }
                }
                Err(error) => return Err(error),
            };

        // Record how many transparent outputs each transaction in this block creates.
        //
        // `created_output_count_by_transaction_hash` counts every transparent output (used for
        // the bound check `spent_output_index < created_output_count`).
        // `spendable_created_output_count_by_transaction_hash` excludes provably-unspendable
        // outputs (NonStandard script types — see `is_unspendable_tx_out`) and is what drives
        // the UTXO-set deltas: `transaction_outputs`, the 0→>0 transition, etc.
        let mut created_output_count_by_transaction_hash: HashMap<TransactionHash, u32> =
            HashMap::with_capacity(txids.len());
        let mut spendable_created_output_count_by_transaction_hash: HashMap<
            TransactionHash,
            u32,
        > = HashMap::with_capacity(txids.len());

        for (transaction_index, transaction_hash) in txids.iter().copied().enumerate() {
            let (output_count, spendable_output_count) = transparent[transaction_index]
                .as_ref()
                .map(|transparent_transaction| {
                    let total = transparent_transaction.outputs().len();
                    let spendable = transparent_transaction
                        .outputs()
                        .iter()
                        .filter(|out| !is_unspendable_tx_out(out))
                        .count();
                    (total, spendable)
                })
                .unwrap_or((0, 0));

            let output_count = u32::try_from(output_count).map_err(|_| {
                FinalisedStateError::Custom(
                    "txout-set accumulator cannot be calculated: transparent output count does not fit into u32"
                        .to_string(),
                )
            })?;
            let spendable_output_count = u32::try_from(spendable_output_count).map_err(|_| {
                FinalisedStateError::Custom(
                    "txout-set accumulator cannot be calculated: spendable output count does not fit into u32"
                        .to_string(),
                )
            })?;

            // Duplicate txids would make the transaction-level accumulator ambiguous.
            if created_output_count_by_transaction_hash
                .insert(transaction_hash, output_count)
                .is_some()
            {
                return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be calculated: duplicate transaction hash in block: {transaction_hash:?}"
            )));
            }
            spendable_created_output_count_by_transaction_hash
                .insert(transaction_hash, spendable_output_count);
        }

        // Group this block's spent outpoints by the transaction they spend from.
        let mut spent_output_indices_by_transaction_hash: HashMap<TransactionHash, HashSet<u32>> =
            HashMap::new();
        let mut spent_outpoints = Vec::with_capacity(spent_map.len());

        for outpoint in spent_map.keys().copied() {
            let previous_transaction_hash = TransactionHash::from(*outpoint.prev_txid());

            let inserted = spent_output_indices_by_transaction_hash
                .entry(previous_transaction_hash)
                .or_default()
                .insert(outpoint.prev_index());

            // A single block must not spend the same outpoint twice.
            if !inserted {
                return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be calculated: duplicate transparent spend for outpoint {outpoint:?}"
            )));
            }

            spent_outpoints.push(outpoint);
        }

        // Update the UTXO count using the direct output delta. Only spendable outputs count
        // toward `transaction_outputs`; consensus rejects spends of unspendable outputs, so
        // `spent_outpoints` already excludes them.
        let created_output_count = spendable_created_output_count_by_transaction_hash
            .values()
            .try_fold(0u64, |total, output_count| {
                total.checked_add(u64::from(*output_count)).ok_or_else(|| {
                    FinalisedStateError::Custom(
                        "txout-set accumulator created output count overflow".to_string(),
                    )
                })
            })?;

        let spent_output_count = u64::try_from(spent_outpoints.len()).map_err(|_| {
            FinalisedStateError::Custom(
                "txout-set accumulator spent output count does not fit into u64".to_string(),
            )
        })?;

        accumulator.transaction_outputs = accumulator
            .transaction_outputs
            .checked_add(created_output_count)
            .and_then(|transaction_outputs| transaction_outputs.checked_sub(spent_output_count))
            .ok_or_else(|| {
                FinalisedStateError::Custom(
                    "txout-set accumulator transaction output count underflow or overflow"
                        .to_string(),
                )
            })?;

        // Validate that old outpoints spent by this block are not already spent in finalised state.
        if !spent_outpoints.is_empty() {
            let existing_spenders =
                <Self as TransparentHistExt>::get_outpoint_spenders(self, spent_outpoints.clone())
                    .await?;

            for (spent_outpoint, existing_spender) in spent_outpoints.iter().zip(existing_spenders)
            {
                // Same-block spends are not in the finalised spent table yet, so skip this check.
                if created_output_count_by_transaction_hash
                    .contains_key(&TransactionHash::from(*spent_outpoint.prev_txid()))
                {
                    continue;
                }

                if let Some(existing_spender) = existing_spender {
                    return Err(FinalisedStateError::Custom(format!(
                    "txout-set accumulator cannot be calculated: block spends already-spent outpoint {spent_outpoint:?}; existing spender is {existing_spender:?}"
                )));
                }
            }
        }

        // Count new transactions entering the UTXO set.
        //
        // A block-created transaction is counted only if at least one of its *spendable*
        // transparent outputs survives any same-block spend. NonStandard outputs are excluded
        // here for consistency with `apply_added_output` further down.
        for (transaction_hash, created_output_count) in &created_output_count_by_transaction_hash {
            let spent_output_indices =
                spent_output_indices_by_transaction_hash.get(transaction_hash);

            // Same-block spends must refer to outputs that this transaction actually created.
            // The bound check uses the *full* output count (NonStandard outputs are still real
            // outputs in the wire layout — the consensus check is positional).
            if let Some(spent_output_indices) = spent_output_indices {
                for spent_output_index in spent_output_indices {
                    if spent_output_index >= created_output_count {
                        return Err(FinalisedStateError::Custom(format!(
                        "txout-set accumulator cannot be calculated: transaction {transaction_hash:?} spends same-block output index {spent_output_index}, but the transaction only has {created_output_count} transparent outputs"
                    )));
                    }
                }
            }

            let spent_created_output_count = spent_output_indices
                .map(|spent_output_indices| spent_output_indices.len())
                .unwrap_or(0);

            let spent_created_output_count =
                u32::try_from(spent_created_output_count).map_err(|_| {
                    FinalisedStateError::Custom(
                        "txout-set accumulator same-block spent output count does not fit into u32"
                            .to_string(),
                    )
                })?;

            // Use the *spendable* count for the transition: a tx with only NonStandard outputs
            // never enters the in-set count.
            let spendable_created_output_count = spendable_created_output_count_by_transaction_hash
                .get(transaction_hash)
                .copied()
                .unwrap_or(0);

            // Transition: 0 unspent outputs before block -> >0 after block.
            if spendable_created_output_count > spent_created_output_count {
                accumulator.transactions =
                    accumulator.transactions.checked_add(1).ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator transaction count overflow".to_string(),
                        )
                    })?;
            }
        }

        // Count old transactions leaving the UTXO set.
        //
        // Only transactions not created by this block can leave the existing finalised UTXO set.
        for (transaction_hash, spent_output_indices_in_block) in
            &spent_output_indices_by_transaction_hash
        {
            if created_output_count_by_transaction_hash.contains_key(transaction_hash) {
                continue;
            }

            // Fetch the previous transaction so we can inspect all of its transparent outputs.
            let Some(transaction_location) =
                <Self as BlockCoreExt>::get_tx_location(self, transaction_hash).await?
            else {
                return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be calculated: spent transaction {transaction_hash:?} is missing from the txid index"
            )));
            };

            let Some(transparent_transaction) =
                <Self as BlockTransparentExt>::get_transparent(self, transaction_location).await?
            else {
                return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be calculated: spent transaction {transaction_hash:?} has no transparent transaction data"
            )));
            };

            let previous_output_count = u32::try_from(transparent_transaction.outputs().len())
                .map_err(|_| {
                    FinalisedStateError::Custom(
                    "txout-set accumulator previous transparent output count does not fit into u32"
                        .to_string(),
                )
                })?;

            // This block must not spend an output index beyond the previous transaction's outputs.
            for spent_output_index in spent_output_indices_in_block {
                if *spent_output_index >= previous_output_count {
                    return Err(FinalisedStateError::Custom(format!(
                    "txout-set accumulator cannot be calculated: transaction {transaction_hash:?} spends output index {spent_output_index}, but the previous transaction only has {previous_output_count} transparent outputs"
                )));
                }
            }

            // Build the previous transaction's *spendable* outputs that are not spent by this
            // block. NonStandard outputs were never in the UTXO set, so excluding them here
            // matches `apply_added_output`'s view.
            let mut remaining_outpoints_not_spent_by_this_block = Vec::new();

            for (output_index, prev_output) in transparent_transaction.outputs().iter().enumerate()
            {
                let output_index = output_index as u32;

                if is_unspendable_tx_out(prev_output) {
                    continue;
                }
                if spent_output_indices_in_block.contains(&output_index) {
                    continue;
                }

                remaining_outpoints_not_spent_by_this_block
                    .push(Outpoint::new(transaction_hash.0, output_index));
            }

            // If this block spends every spendable output from the previous transaction, it
            // leaves the UTXO set.
            if remaining_outpoints_not_spent_by_this_block.is_empty() {
                accumulator.transactions =
                    accumulator.transactions.checked_sub(1).ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator transaction count underflow".to_string(),
                        )
                    })?;

                continue;
            }

            // Check whether any output not spent by this block was still unspent before this block.
            let remaining_spenders = <Self as TransparentHistExt>::get_outpoint_spenders(
                self,
                remaining_outpoints_not_spent_by_this_block,
            )
            .await?;

            let has_remaining_unspent_output = remaining_spenders
                .into_iter()
                .any(|spender| spender.is_none());

            // Transition: >0 unspent outputs before block -> 0 after block.
            if !has_remaining_unspent_output {
                accumulator.transactions =
                    accumulator.transactions.checked_sub(1).ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator transaction count underflow".to_string(),
                        )
                    })?;
            }
        }

        // Update bytes_serialized, hash_serialized and total_zatoshis.
        //
        // Created outputs are added to the multiset; spent prev outputs are removed. Same-block
        // spends are resolved from the in-block transparent slice; prior-block spends are resolved
        // from the finalised database. NonStandard (unspendable) outputs are skipped — they were
        // never counted on the way in, so they must not be counted on the way out either.
        let mut created_entries: Vec<(Outpoint, TxOutCompact)> = Vec::new();
        let mut txid_to_block_index: HashMap<TransactionHash, usize> =
            HashMap::with_capacity(txids.len());

        for (transaction_index, transaction_hash) in txids.iter().copied().enumerate() {
            txid_to_block_index.insert(transaction_hash, transaction_index);

            let Some(transparent_transaction) = transparent[transaction_index].as_ref() else {
                continue;
            };

            for (output_index, output) in transparent_transaction.outputs().iter().enumerate() {
                if is_unspendable_tx_out(output) {
                    continue;
                }
                let outpoint = Outpoint::new(transaction_hash.0, output_index as u32);
                created_entries.push((outpoint, *output));
            }
        }

        let spent_entries = self.resolve_spent_outpoints_for_set_info(
            spent_map,
            &txid_to_block_index,
            transparent,
        )?;

        Self::apply_tx_out_set_entries_delta(&mut accumulator, &created_entries, true)?;
        Self::apply_tx_out_set_entries_delta(&mut accumulator, &spent_entries, false)?;

        Ok(accumulator)
    }

    /// Calculates the finalised txout-set accumulator after deleting the tip block.
    ///
    /// This is the exact inverse of `calculate_tx_out_set_info_accumulator_after_block`.
    ///
    /// The database must still contain the block being deleted when this method is called.
    /// The returned accumulator must be written inside the same LMDB transaction that deletes the block.
    pub(crate) async fn calculate_tx_out_set_info_accumulator_after_delete_block(
        &self,
        txids: &[TransactionHash],
        transparent: &[Option<TransparentCompactTx>],
        spent_map: &HashMap<Outpoint, TxLocation>,
    ) -> Result<FinalisedTxOutSetInfoAccumulator, FinalisedStateError> {
        // The block-local transaction arrays must stay index-aligned.
        if txids.len() != transparent.len() {
            return Err(FinalisedStateError::Custom(format!(
            "txout-set accumulator cannot be calculated: txid count ({}) does not match transparent transaction count ({})",
            txids.len(),
            transparent.len()
        )));
        }

        let mut accumulator =
            match <Self as TransparentHistExt>::get_tx_out_set_info_accumulator(self).await {
                Ok(accumulator) => accumulator,
                Err(FinalisedStateError::DataUnavailable(_)) => {
                    return Err(FinalisedStateError::Custom(
                        "txout-set accumulator missing while deleting block".to_string(),
                    ));
                }
                Err(error) => return Err(error),
            };

        // See `calculate_tx_out_set_info_accumulator_after_block` for the rationale on having
        // a `created_output_count_by_transaction_hash` (full count, used for the bound check)
        // alongside a `spendable_created_output_count_by_transaction_hash` (excludes NonStandard
        // outputs, used for the UTXO-set deltas).
        let mut created_output_count_by_transaction_hash: HashMap<TransactionHash, u32> =
            HashMap::with_capacity(txids.len());
        let mut spendable_created_output_count_by_transaction_hash: HashMap<
            TransactionHash,
            u32,
        > = HashMap::with_capacity(txids.len());

        for (transaction_index, transaction_hash) in txids.iter().copied().enumerate() {
            let (output_count, spendable_output_count) = transparent[transaction_index]
                .as_ref()
                .map(|transparent_transaction| {
                    let total = transparent_transaction.outputs().len();
                    let spendable = transparent_transaction
                        .outputs()
                        .iter()
                        .filter(|out| !is_unspendable_tx_out(out))
                        .count();
                    (total, spendable)
                })
                .unwrap_or((0, 0));

            let output_count = u32::try_from(output_count).map_err(|_| {
                FinalisedStateError::Custom(
                    "txout-set accumulator cannot be calculated: transparent output count does not fit into u32"
                        .to_string(),
                )
            })?;
            let spendable_output_count = u32::try_from(spendable_output_count).map_err(|_| {
                FinalisedStateError::Custom(
                    "txout-set accumulator cannot be calculated: spendable output count does not fit into u32"
                        .to_string(),
                )
            })?;

            // Duplicate txids would make the transaction-level accumulator ambiguous.
            if created_output_count_by_transaction_hash
                .insert(transaction_hash, output_count)
                .is_some()
            {
                return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be calculated: duplicate transaction hash in block: {transaction_hash:?}"
            )));
            }
            spendable_created_output_count_by_transaction_hash
                .insert(transaction_hash, spendable_output_count);
        }

        // Group this block's spent outpoints by the transaction they spend from.
        let mut spent_output_indices_by_transaction_hash: HashMap<TransactionHash, HashSet<u32>> =
            HashMap::new();

        let mut spent_outpoints = Vec::with_capacity(spent_map.len());

        for (outpoint, tx_location) in spent_map.iter() {
            let previous_transaction_hash = TransactionHash::from(*outpoint.prev_txid());

            let inserted = spent_output_indices_by_transaction_hash
                .entry(previous_transaction_hash)
                .or_default()
                .insert(outpoint.prev_index());

            // This is defensive; duplicate outpoints should already be rejected when building spent_map.
            if !inserted {
                return Err(FinalisedStateError::Custom(format!(
            "txout-set accumulator cannot be reversed: duplicate transparent spend for outpoint {outpoint:?}"
        )));
            }

            spent_outpoints.push((*outpoint, *tx_location));
        }

        // Update the UTXO count using the direct output delta. Spendable outputs only;
        // `spent_outpoints` already excludes unspendable outputs by consensus.
        let created_output_count = spendable_created_output_count_by_transaction_hash
            .values()
            .try_fold(0u64, |total, output_count| {
                total.checked_add(u64::from(*output_count)).ok_or_else(|| {
                    FinalisedStateError::Custom(
                        "txout-set accumulator created output count overflow".to_string(),
                    )
                })
            })?;

        let spent_output_count = u64::try_from(spent_outpoints.len()).map_err(|_| {
            FinalisedStateError::Custom(
                "txout-set accumulator spent output count does not fit into u64".to_string(),
            )
        })?;

        accumulator.transaction_outputs = accumulator
            .transaction_outputs
            .checked_sub(created_output_count)
            .and_then(|transaction_outputs| transaction_outputs.checked_add(spent_output_count))
            .ok_or_else(|| {
                FinalisedStateError::Custom(
                    "txout-set accumulator transaction output count underflow or overflow"
                        .to_string(),
                )
            })?;

        // The block being deleted has already been applied, so every spent outpoint from this block
        // must exist in the spent index and must point to this block's TxLocation.
        if !spent_outpoints.is_empty() {
            let spent_outpoints_to_check: Vec<Outpoint> = spent_outpoints
                .iter()
                .map(|(outpoint, _tx_location)| *outpoint)
                .collect();

            let existing_spenders =
                <Self as TransparentHistExt>::get_outpoint_spenders(self, spent_outpoints_to_check)
                    .await?;

            for ((spent_outpoint, expected_tx_location), existing_spender) in
                spent_outpoints.iter().zip(existing_spenders)
            {
                let Some(existing_spender) = existing_spender else {
                    return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be reversed: spent index missing outpoint {spent_outpoint:?}"
            )));
                };

                if existing_spender != *expected_tx_location {
                    return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be reversed: outpoint {spent_outpoint:?} is spent by {existing_spender:?}, expected {expected_tx_location:?}"
            )));
                }
            }
        }

        // Inverse of the forward 0→>0 transition. Uses the spendable count for the comparison
        // so that NonStandard-only transactions never appear to leave/enter the set.
        for (transaction_hash, created_output_count) in &created_output_count_by_transaction_hash {
            let spent_output_indices =
                spent_output_indices_by_transaction_hash.get(transaction_hash);

            // Bound check uses the full output count (positional consensus invariant).
            if let Some(spent_output_indices) = spent_output_indices {
                for spent_output_index in spent_output_indices {
                    if spent_output_index >= created_output_count {
                        return Err(FinalisedStateError::Custom(format!(
                        "txout-set accumulator cannot be calculated: transaction {transaction_hash:?} spends same-block output index {spent_output_index}, but the transaction only has {created_output_count} transparent outputs"
                    )));
                    }
                }
            }

            let spent_created_output_count = spent_output_indices
                .map(|spent_output_indices| spent_output_indices.len())
                .unwrap_or(0);

            let spent_created_output_count =
                u32::try_from(spent_created_output_count).map_err(|_| {
                    FinalisedStateError::Custom(
                        "txout-set accumulator same-block spent output count does not fit into u32"
                            .to_string(),
                    )
                })?;

            let spendable_created_output_count = spendable_created_output_count_by_transaction_hash
                .get(transaction_hash)
                .copied()
                .unwrap_or(0);

            // Inverse transition: when applied forward this tx entered the set (spendable > 0),
            // so when reversing it now leaves the set.
            if spendable_created_output_count > spent_created_output_count {
                accumulator.transactions =
                    accumulator.transactions.checked_sub(1).ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator transaction count underflow".to_string(),
                        )
                    })?;
            }
        }

        // Count old transactions leaving the UTXO set.
        //
        // Only transactions not created by this block can leave the existing finalised UTXO set.
        for (transaction_hash, spent_output_indices_in_block) in
            &spent_output_indices_by_transaction_hash
        {
            if created_output_count_by_transaction_hash.contains_key(transaction_hash) {
                continue;
            }

            // Fetch the previous transaction so we can inspect all of its transparent outputs.
            let Some(transaction_location) =
                <Self as BlockCoreExt>::get_tx_location(self, transaction_hash).await?
            else {
                return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be calculated: spent transaction {transaction_hash:?} is missing from the txid index"
            )));
            };

            let Some(transparent_transaction) =
                <Self as BlockTransparentExt>::get_transparent(self, transaction_location).await?
            else {
                return Err(FinalisedStateError::Custom(format!(
                "txout-set accumulator cannot be calculated: spent transaction {transaction_hash:?} has no transparent transaction data"
            )));
            };

            let previous_output_count = u32::try_from(transparent_transaction.outputs().len())
                .map_err(|_| {
                    FinalisedStateError::Custom(
                    "txout-set accumulator previous transparent output count does not fit into u32"
                        .to_string(),
                )
                })?;

            // This block must not spend an output index beyond the previous transaction's outputs.
            for spent_output_index in spent_output_indices_in_block {
                if *spent_output_index >= previous_output_count {
                    return Err(FinalisedStateError::Custom(format!(
                    "txout-set accumulator cannot be calculated: transaction {transaction_hash:?} spends output index {spent_output_index}, but the previous transaction only has {previous_output_count} transparent outputs"
                )));
                }
            }

            // Build the previous transaction's *spendable* outputs that are not spent by this
            // block; NonStandard outputs were never in the UTXO set.
            let mut remaining_outpoints_not_spent_by_this_block = Vec::new();

            for (output_index, prev_output) in transparent_transaction.outputs().iter().enumerate()
            {
                let output_index = output_index as u32;

                if is_unspendable_tx_out(prev_output) {
                    continue;
                }
                if spent_output_indices_in_block.contains(&output_index) {
                    continue;
                }

                remaining_outpoints_not_spent_by_this_block
                    .push(Outpoint::new(transaction_hash.0, output_index));
            }

            // If this block spent every spendable output from the previous transaction, deleting
            // this block restores it.
            if remaining_outpoints_not_spent_by_this_block.is_empty() {
                accumulator.transactions =
                    accumulator.transactions.checked_add(1).ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator transaction count overflow".to_string(),
                        )
                    })?;

                continue;
            }

            // Check whether any output not spent by this block was still unspent before this block.
            let remaining_spenders = <Self as TransparentHistExt>::get_outpoint_spenders(
                self,
                remaining_outpoints_not_spent_by_this_block,
            )
            .await?;

            let has_remaining_unspent_output = remaining_spenders
                .into_iter()
                .any(|spender| spender.is_none());

            // Transition: >0 unspent outputs before block -> 0 after block.
            if !has_remaining_unspent_output {
                accumulator.transactions =
                    accumulator.transactions.checked_add(1).ok_or_else(|| {
                        FinalisedStateError::Custom(
                            "txout-set accumulator transaction count overflow".to_string(),
                        )
                    })?;
            }
        }

        // Reverse the multiset commitment, byte-count and zatoshi-sum delta from this block.
        //
        // The block being deleted is still in the database, so prior-block prev outputs are still
        // resolvable via `get_previous_output_blocking`. Same-block spends are resolved from the
        // in-block transparent slice (the prev tx is created in this same block). NonStandard
        // outputs are skipped — they were never added in the first place.
        let mut created_entries: Vec<(Outpoint, TxOutCompact)> = Vec::new();
        let mut txid_to_block_index: HashMap<TransactionHash, usize> =
            HashMap::with_capacity(txids.len());

        for (transaction_index, transaction_hash) in txids.iter().copied().enumerate() {
            txid_to_block_index.insert(transaction_hash, transaction_index);

            let Some(transparent_transaction) = transparent[transaction_index].as_ref() else {
                continue;
            };

            for (output_index, output) in transparent_transaction.outputs().iter().enumerate() {
                if is_unspendable_tx_out(output) {
                    continue;
                }
                let outpoint = Outpoint::new(transaction_hash.0, output_index as u32);
                created_entries.push((outpoint, *output));
            }
        }

        let spent_entries = self.resolve_spent_outpoints_for_set_info(
            spent_map,
            &txid_to_block_index,
            transparent,
        )?;

        Self::apply_tx_out_set_entries_delta(&mut accumulator, &created_entries, false)?;
        Self::apply_tx_out_set_entries_delta(&mut accumulator, &spent_entries, true)?;

        Ok(accumulator)
    }
}
