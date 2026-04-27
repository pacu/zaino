//! Zaino's chain fetch and tx submission backend services.

pub mod fetch;

pub mod state;

fn latest_network_upgrade(
    upgrades: &indexmap::IndexMap<
        zebra_rpc::methods::ConsensusBranchIdHex,
        zebra_rpc::methods::NetworkUpgradeInfo,
    >,
) -> Result<&zebra_rpc::methods::NetworkUpgradeInfo, tonic::Status> {
    upgrades.last().map(|(_, upgrade)| upgrade).ok_or_else(|| {
        tonic::Status::failed_precondition("validator returned no network upgrade metadata")
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn latest_network_upgrade_rejects_empty_metadata() {
        let upgrades = indexmap::IndexMap::new();
        let err = super::latest_network_upgrade(&upgrades).expect_err("empty upgrades must fail");

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "validator returned no network upgrade metadata"
        );
    }
}
