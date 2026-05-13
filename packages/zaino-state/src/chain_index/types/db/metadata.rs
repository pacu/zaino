//! Metadata objects

use corez::io::{self, Read, Write};

use crate::{read_u64_le, version, write_u64_le, FixedEncodedLen, ZainoVersionedSerde};

/// Holds information about the mempool state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MempoolInfo {
    /// Current tx count
    pub size: u64,
    /// Sum of all tx sizes
    pub bytes: u64,
    /// Total memory usage for the mempool
    pub usage: u64,
}

impl ZainoVersionedSerde for MempoolInfo {
    const VERSION: u8 = version::V1;

    fn encode_latest<W: Write>(&self, w: &mut W) -> io::Result<()> {
        Self::encode_v1(self, w)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_v1(r)
    }

    fn encode_v1<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut w = w;
        write_u64_le(&mut w, self.size)?;
        write_u64_le(&mut w, self.bytes)?;
        write_u64_le(&mut w, self.usage)
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut r = r;
        let size = read_u64_le(&mut r)?;
        let bytes = read_u64_le(&mut r)?;
        let usage = read_u64_le(&mut r)?;
        Ok(MempoolInfo { size, bytes, usage })
    }
}

/// 24 byte body.
impl FixedEncodedLen for MempoolInfo {
    /// 8 byte size + 8 byte bytes + 8 byte usage
    const ENCODED_LEN: usize = 8 + 8 + 8;
}

impl From<zaino_fetch::jsonrpsee::response::GetMempoolInfoResponse> for MempoolInfo {
    fn from(resp: zaino_fetch::jsonrpsee::response::GetMempoolInfoResponse) -> Self {
        MempoolInfo {
            size: resp.size,
            bytes: resp.bytes,
            usage: resp.usage,
        }
    }
}

impl From<MempoolInfo> for zaino_fetch::jsonrpsee::response::GetMempoolInfoResponse {
    fn from(info: MempoolInfo) -> Self {
        zaino_fetch::jsonrpsee::response::GetMempoolInfoResponse {
            size: info.size,
            bytes: info.bytes,
            usage: info.usage,
        }
    }
}

/// Holds finalised-state UTXO set accumulator data for `gettxoutsetinfo`.
///
/// This is not the full RPC response. It only contains values that the
/// finalised-state database can maintain cheaply and exactly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FinalisedTxOutSetInfoAccumulator {
    /// Number of transactions with at least one currently unspent transparent output.
    pub transactions: u64,

    /// Number of currently unspent transparent outputs.
    pub transaction_outputs: u64,
}

impl FinalisedTxOutSetInfoAccumulator {
    /// Creates a new finalised txout-set accumulator.
    pub const fn new(transactions: u64, transaction_outputs: u64) -> Self {
        Self {
            transactions,
            transaction_outputs,
        }
    }

    /// Returns an empty finalised txout-set accumulator.
    pub const fn empty() -> Self {
        Self {
            transactions: 0,
            transaction_outputs: 0,
        }
    }
}

impl ZainoVersionedSerde for FinalisedTxOutSetInfoAccumulator {
    const VERSION: u8 = version::V1;

    fn encode_latest<Writer: Write>(&self, writer: &mut Writer) -> io::Result<()> {
        Self::encode_v1(self, writer)
    }

    fn decode_latest<Reader: Read>(reader: &mut Reader) -> io::Result<Self> {
        Self::decode_v1(reader)
    }

    fn encode_v1<Writer: Write>(&self, writer: &mut Writer) -> io::Result<()> {
        write_u64_le(&mut *writer, self.transactions)?;
        write_u64_le(&mut *writer, self.transaction_outputs)
    }

    fn decode_v1<Reader: Read>(reader: &mut Reader) -> io::Result<Self> {
        let transactions = read_u64_le(&mut *reader)?;
        let transaction_outputs = read_u64_le(&mut *reader)?;

        Ok(Self {
            transactions,
            transaction_outputs,
        })
    }
}

impl FixedEncodedLen for FinalisedTxOutSetInfoAccumulator {
    /// 8 byte transactions + 8 byte transaction_outputs
    const ENCODED_LEN: usize = 8 + 8;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalised_tx_out_set_info_accumulator_roundtrips() {
        let accumulator = FinalisedTxOutSetInfoAccumulator {
            transactions: 12,
            transaction_outputs: 34,
        };

        let encoded_accumulator = accumulator
            .to_bytes()
            .expect("finalised txout set info accumulator should encode");

        assert_eq!(
            encoded_accumulator.len(),
            FinalisedTxOutSetInfoAccumulator::VERSIONED_LEN
        );

        let decoded_accumulator =
            FinalisedTxOutSetInfoAccumulator::from_bytes(&encoded_accumulator)
                .expect("finalised txout set info accumulator should decode");

        assert_eq!(decoded_accumulator, accumulator);
    }

    #[test]
    fn finalised_tx_out_set_info_accumulator_empty_is_zero() {
        let accumulator = FinalisedTxOutSetInfoAccumulator::empty();

        assert_eq!(accumulator.transactions, 0);
        assert_eq!(accumulator.transaction_outputs, 0);
    }
}
