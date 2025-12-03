use crate::proto::service::{BlockRange, PoolType};

#[derive(Debug)]
/// Errors that can arise when mapping `PoolType` from an `i32` value.
pub enum PoolTypeError {
    /// Pool Type value was map to the enum `PoolType::Invalid`.
    InvalidPoolType,
    /// Pool Type value was mapped to value that can't be mapped to a known pool type.
    UnknownPoolType(i32),
}

// Converts a vector of pool_types (i32) into its rich-type representation
// Returns `None` when invalid `pool_types` are found
pub fn pool_types_from_vector(pool_types: &[i32]) -> Result<Vec<PoolType>, PoolTypeError> {
    let pools = if pool_types.is_empty() {
        vec![PoolType::Sapling, PoolType::Orchard]
    } else {
        let mut pools: Vec<PoolType> = vec![];

        for pool in pool_types.iter() {
            match PoolType::try_from(*pool) {
                Ok(pool_type) => {
                    if pool_type == PoolType::Invalid {
                        return Err(PoolTypeError::InvalidPoolType);
                    } else {
                        pools.push(pool_type);
                    }
                }
                Err(_) => {
                    return Err(PoolTypeError::UnknownPoolType(*pool));
                }
            };
        }

        pools.clone()
    };
    Ok(pools)
}

/// Converts a `Vec<Pooltype>` into a `Vec<i32>`
pub fn pool_types_into_i32_vec(pool_types: Vec<PoolType>) -> Vec<i32> {
    pool_types.iter().map(|p| *p as i32).collect()
}

/// Errors that can be present in the request of the GetBlockRange RPC
pub enum GetBlockRangeError {
    /// Error: No start height given.    
    NoStartHeightProvided,
    /// Error: No end height given.    
    NoEndHeightProvided,
    /// Start height out of range. Failed to convert to u32.
    StartHeightOutOfRange,

    /// End height out of range. Failed to convert to u32.
    EndHeightOutOfRange,
    /// An invalid pool type request was provided.
    PoolTypeArgumentError(PoolTypeError),
}

/// `BlockRange` request that has been validated in terms of the semantics
/// of `GetBlockRange` RPC.
pub struct ValidatedBlockRangeRequest {
    start: u64,
    end: u64,
    pool_types: Vec<PoolType>,
}

impl ValidatedBlockRangeRequest {
    /// validates a BlockRange in terms of the `GetBlockRange` RPC
    pub fn new_from_block_range(
        request: &BlockRange,
    ) -> Result<ValidatedBlockRangeRequest, GetBlockRangeError> {
        let start = match &request.start {
            Some(block_id) => block_id.height,
            None => {
                return Err(GetBlockRangeError::NoStartHeightProvided);
            }
        };
        let end = match &request.end {
            Some(block_id) => block_id.height,
            None => {
                return Err(GetBlockRangeError::NoEndHeightProvided);
            }
        };

        let pool_types = pool_types_from_vector(&request.pool_types)
            .map_err(GetBlockRangeError::PoolTypeArgumentError)?;

        Ok(ValidatedBlockRangeRequest {
            start,
            end,
            pool_types,
        })
    }

    /// Start Height of the BlockRange Request
    pub fn start(&self) -> u64 {
        self.start
    }

    /// End Height of the BlockRange Request
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Pool Types of the BlockRange request
    pub fn pool_types(&self) -> Vec<PoolType> {
        self.pool_types.clone()
    }

    /// checks whether this request is specified in reversed order
    pub fn is_reverse_ordered(&self) -> bool {
        self.start > self.end
    }

    /// Reverses the order of this request
    pub fn reverse(&mut self) {
        (self.start, self.end) = (self.end, self.start);
    }
}
