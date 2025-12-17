use crate::proto::service::{BlockRange, PoolType};

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
    PoolTypArgumentError(PoolTypeError),
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
            Some(block_id) => match block_id.height.try_into() {
                Ok(height) => height,
                Err(_) => {
                    return Err(GetBlockRangeError::StartHeightOutOfRange);
                }
            },
            None => {
                return Err(GetBlockRangeError::NoStartHeightProvided);
            }
        };
        let end = match &request.end {
            Some(block_id) => match block_id.height.try_into() {
                Ok(height) => height,
                Err(_) => {
                    return Err(GetBlockRangeError::EndHeightOutOfRange);
                }
            },
            None => {
                return Err(GetBlockRangeError::NoEndHeightProvided);
            }
        };

        let pool_types = pool_types_from_vector(&request.pool_types)
            .map_err(|e| GetBlockRangeError::PoolTypArgumentError(e))?;

        Ok(ValidatedBlockRangeRequest {
            start: start,
            end: end,
            pool_types: pool_types,
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
        if self.start > self.end {
            true
        } else {
            false
        }
    }

    /// Reverses the order of this request
    pub fn reverse(&mut self) {
        (self.start, self.end) = (self.end, self.start);
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolTypeFilter {
    include_transparent: bool,
    include_sapling: bool,
    include_orchard: bool,
}

impl PoolTypeFilter {
    /// By default PoolType includes `Sapling` and `Orchard` pools.
    pub fn default() -> Self {
        PoolTypeFilter {
            include_transparent: false,
            include_sapling: true,
            include_orchard: true,
        }
    }

    /// create a `PoolTypeFilter` from a vector of `PoolType`
    /// If the vector is empty it will return `Self::default()`.
    /// If the vector contains `PoolType::Invalid`, returns `None`
    /// If the vector contains more than 3 elements, returns `None`
    pub fn new_from_pool_types(pool_types: &Vec<PoolType>) -> Option<PoolTypeFilter> {
        if pool_types.len() > PoolType::Orchard as usize {
            return None;
        }

        if pool_types.is_empty() {
            Some(Self::default())
        } else {
            let mut filter = PoolTypeFilter::empty();

            for pool_type in pool_types {
                match pool_type {
                    PoolType::Invalid => return None,
                    PoolType::Transparent => filter.include_transparent = true,
                    PoolType::Sapling => filter.include_sapling = true,
                    PoolType::Orchard => filter.include_orchard = true,
                }
            }

            // guard against returning an invalid state this shouls never happen.
            if filter.is_empty() {
                return Some(Self::default());
            } else {
                return Some(filter);
            }
        }
    }

    /// only internal use. this in an invalid state.
    fn empty() -> Self {
        Self {
            include_transparent: false,
            include_sapling: false,
            include_orchard: false,
        }
    }

    /// only internal use
    fn is_empty(&self) -> bool {
        !self.include_transparent && !self.include_sapling && !self.include_orchard
    }

    /// retuns whether the filter includes transparent data
    pub fn includes_tranparent(&self) -> bool {
        self.include_transparent
    }

    /// returns whether the filter includes orchard data
    pub fn includes_sapling(&self) -> bool {
        self.include_sapling
    }

    // returnw whether the filter includes orchard data
    pub fn includes_orchard(&self) -> bool {
        self.include_orchard
    }

    /// testing only
    #[allow(dead_code)]
    pub(crate) fn from_checked_parts(
        include_transparent: bool,
        include_sapling: bool,
        include_orchard: bool,
    ) -> Self {
        PoolTypeFilter {
            include_transparent,
            include_sapling,
            include_orchard,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::proto::{service::PoolType, utils::PoolTypeFilter};

    #[test]
    fn test_pool_type_filter_none_when_invalid() {
        let pools = [
            PoolType::Transparent,
            PoolType::Sapling,
            PoolType::Orchard,
            PoolType::Invalid,
        ]
        .to_vec();

        assert_eq!(PoolTypeFilter::new_from_pool_types(&pools), None);
    }

    #[test]
    fn test_pool_type_filter_none_when_too_many_items() {
        let pools = [
            PoolType::Transparent,
            PoolType::Sapling,
            PoolType::Orchard,
            PoolType::Orchard,
        ]
        .to_vec();

        assert_eq!(PoolTypeFilter::new_from_pool_types(&pools), None);
    }

    #[test]
    fn test_pool_type_filter_t_z_o() {
        let pools = [PoolType::Transparent, PoolType::Sapling, PoolType::Orchard].to_vec();

        assert_eq!(
            PoolTypeFilter::new_from_pool_types(&pools),
            Some(PoolTypeFilter::from_checked_parts(true, true, false))
        );
    }

    #[test]
    fn test_pool_type_filter_t() {
        let pools = [PoolType::Transparent].to_vec();

        assert_eq!(
            PoolTypeFilter::new_from_pool_types(&pools),
            Some(PoolTypeFilter::from_checked_parts(true, false, false))
        );
    }

    #[test]
    fn test_pool_type_filter_default() {
        assert_eq!(
            PoolTypeFilter::new_from_pool_types(&vec![]),
            Some(PoolTypeFilter::default())
        );
    }
}
