//! These tests compare the output of `FetchService` with the output of `JsonRpcConnector`.

use futures::StreamExt as _;
use zaino_proto::proto::compact_formats::CompactBlock;
use zaino_proto::proto::service::{BlockId, BlockRange, GetSubtreeRootsArg, PoolType};
#[allow(deprecated)]
use zaino_state::{FetchService, LightWalletIndexer, Status, StatusType, ZcashIndexer};
use zaino_testutils::{TestManager, ValidatorExt, ValidatorKind};
use zebra_chain::parameters::subsidy::ParameterSubsidy as _;
use zebra_rpc::client::ValidateAddressResponse;
use zebra_rpc::methods::{GetBlock, GetBlockHash};

async fn launch_fetch_service<V: ValidatorExt>(
    validator: &ValidatorKind,
    chain_cache: Option<std::path::PathBuf>,
) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, chain_cache).await;
    assert_eq!(fetch_service_subscriber.status(), StatusType::Ready);
    dbg!(fetch_service_subscriber.data.clone());
    dbg!(fetch_service_subscriber.get_info().await.unwrap());
    dbg!(fetch_service_subscriber
        .get_blockchain_info()
        .await
        .unwrap()
        .blocks());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_raw<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    dbg!(fetch_service_subscriber
        .z_get_block("1".to_string(), Some(0))
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_object<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    dbg!(fetch_service_subscriber
        .z_get_block("1".to_string(), Some(1))
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_latest_block<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let json_service = test_manager.full_node_jsonrpc_connector().await;

    let fetch_service_get_latest_block =
        dbg!(fetch_service_subscriber.get_latest_block().await.unwrap());

    let json_service_blockchain_info = json_service.get_blockchain_info().await.unwrap();

    let json_service_get_latest_block = dbg!(BlockId {
        height: json_service_blockchain_info.blocks.0 as u64,
        hash: json_service_blockchain_info.best_block_hash.0.to_vec(),
    });

    assert_eq!(fetch_service_get_latest_block.height, 3);
    assert_eq!(
        fetch_service_get_latest_block,
        json_service_get_latest_block
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn assert_fetch_service_difficulty_matches_rpc<V: ValidatorExt>(validator: &ValidatorKind) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let fetch_service_get_difficulty = fetch_service_subscriber.get_difficulty().await.unwrap();

    let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

    let rpc_difficulty_response = jsonrpc_client.get_difficulty().await.unwrap();
    assert_eq!(fetch_service_get_difficulty, rpc_difficulty_response.0);
}

#[allow(deprecated)]
async fn assert_fetch_service_mininginfo_matches_rpc<V: ValidatorExt>(validator: &ValidatorKind) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let fetch_service_mining_info = fetch_service_subscriber.get_mining_info().await.unwrap();

    let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

    let rpc_mining_info_response = jsonrpc_client.get_mining_info().await.unwrap();
    assert_eq!(fetch_service_mining_info, rpc_mining_info_response);
}

#[allow(deprecated)]
async fn assert_fetch_service_gettxoutsetinfo_matches_rpc<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let fetch_service_txoutset_info = fetch_service_subscriber
        .get_tx_out_set_info()
        .await
        .unwrap();

    let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

    let rpc_txoutset_info = jsonrpc_client.get_tx_out_set_info().await.unwrap();

    // Structural parity with zcashd: height, bestblock, transactions, txouts and total_amount
    // must match. `bytes_serialized` and `hash_serialized` are Zaino-defined (see the
    // `gettxoutsetinfo` spec in zaino-state) and intentionally diverge from zcashd; only
    // Zaino-internal invariants are asserted on those fields.
    use zaino_fetch::jsonrpsee::response::GetTxOutSetInfoResponse;
    let (zaino, zcashd) = match (fetch_service_txoutset_info, rpc_txoutset_info) {
        (GetTxOutSetInfoResponse::Info(z), GetTxOutSetInfoResponse::Info(r)) => (z, r),
        other => panic!("expected non-empty gettxoutsetinfo from both sides, got {other:?}"),
    };

    assert_eq!(zaino.height, zcashd.height, "`height` differs from zcashd");
    assert_eq!(
        zaino.best_block, zcashd.best_block,
        "`bestblock` differs from zcashd"
    );
    assert_eq!(
        zaino.transactions, zcashd.transactions,
        "`transactions` count differs from zcashd"
    );
    assert_eq!(zaino.txouts, zcashd.txouts, "`txouts` differs from zcashd");
    assert!(
        (zaino.total_amount - zcashd.total_amount).abs() < 1e-8,
        "`total_amount` differs from zcashd: zaino={} zcashd={}",
        zaino.total_amount,
        zcashd.total_amount
    );

    // Zaino-only invariants on the redefined fields.
    assert_eq!(
        zaino.bytes_serialized,
        zaino.txouts * 65,
        "`bytes_serialized` must equal `txouts * 65` under Zaino's UTXO entry encoding"
    );
    assert_eq!(
        zaino.hash_serialized.len(),
        64,
        "`hash_serialized` must be 64 lowercase hex chars"
    );
    assert!(
        zaino.hash_serialized.chars().all(|c| c.is_ascii_hexdigit()),
        "`hash_serialized` must be hex: got {}",
        zaino.hash_serialized
    );
}

#[allow(deprecated)]
async fn assert_fetch_service_peerinfo_matches_rpc<V: ValidatorExt>(validator: &ValidatorKind) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let fetch_service_get_peer_info = fetch_service_subscriber.get_peer_info().await.unwrap();

    let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

    let rpc_peer_info_response = jsonrpc_client.get_peer_info().await.unwrap();

    dbg!(&rpc_peer_info_response);
    dbg!(&fetch_service_get_peer_info);
    assert_eq!(fetch_service_get_peer_info, rpc_peer_info_response);
}

#[allow(deprecated)]
async fn fetch_service_get_block_subsidy<V: ValidatorExt>(validator: &ValidatorKind) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let first_halving_height = fetch_service_subscriber
        .network()
        .to_zebra_network()
        .height_for_first_halving();
    let block_limit = match validator {
        // Block generation is more expensive in zcashd, and 10 is sufficient
        ValidatorKind::Zcashd => 10,
        // To stay consistent with zcashd, ten successful examples. Any calls
        // below the first halving height should fail.
        ValidatorKind::Zebrad => first_halving_height.0 + 10,
    };

    for i in 0..block_limit {
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        let fetch_service_get_block_subsidy =
            fetch_service_subscriber.get_block_subsidy(i).await.unwrap();

        let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

        let rpc_block_subsidy_response = jsonrpc_client.get_block_subsidy(i).await.unwrap();
        assert_eq!(fetch_service_get_block_subsidy, rpc_block_subsidy_response);
    }
}

#[allow(deprecated)]
async fn fetch_service_get_block<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let block_id = BlockId {
        height: 1,
        hash: Vec::new(),
    };

    let fetch_service_get_block = dbg!(fetch_service_subscriber
        .get_block(block_id.clone())
        .await
        .unwrap());

    assert_eq!(fetch_service_get_block.height, block_id.height);
    let block_id_by_hash = BlockId {
        height: 0,
        hash: fetch_service_get_block.hash.clone(),
    };
    let fetch_service_get_block_by_hash = fetch_service_subscriber
        .get_block(block_id_by_hash.clone())
        .await
        .unwrap();
    assert_eq!(fetch_service_get_block_by_hash.hash, block_id_by_hash.hash);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_header<V: ValidatorExt>(validator: &ValidatorKind) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    const BLOCK_LIMIT: u32 = 10;

    for i in 0..BLOCK_LIMIT {
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;

        let block = fetch_service_subscriber
            .z_get_block(i.to_string(), Some(1))
            .await
            .unwrap();

        let block_hash = match block {
            GetBlock::Object(block) => block.hash(),
            GetBlock::Raw(_) => panic!("Expected block object"),
        };

        let fetch_service_get_block_header = fetch_service_subscriber
            .get_block_header(block_hash.to_string(), false)
            .await
            .unwrap();

        let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

        let rpc_block_header_response = jsonrpc_client
            .get_block_header(block_hash.to_string(), false)
            .await
            .unwrap();

        let fetch_service_get_block_header_verbose = fetch_service_subscriber
            .get_block_header(block_hash.to_string(), true)
            .await
            .unwrap();

        let rpc_block_header_response_verbose = jsonrpc_client
            .get_block_header(block_hash.to_string(), true)
            .await
            .unwrap();

        assert_eq!(fetch_service_get_block_header, rpc_block_header_response);
        assert_eq!(
            fetch_service_get_block_header_verbose,
            rpc_block_header_response_verbose
        );
    }
}

#[allow(deprecated)]
async fn fetch_service_get_best_blockhash<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    test_manager
        .generate_blocks_and_wait_for_tip(5, &fetch_service_subscriber)
        .await;

    let inspected_block: GetBlock = fetch_service_subscriber
        // Some(verbosity) : 1 for JSON Object, 2 for tx data as JSON instead of hex
        .z_get_block("7".to_string(), Some(1))
        .await
        .unwrap();

    let ret = match inspected_block {
        GetBlock::Object(obj) => Some(obj.hash()),
        _ => None,
    };

    let fetch_service_get_best_blockhash: GetBlockHash =
        dbg!(fetch_service_subscriber.get_best_blockhash().await.unwrap());

    assert_eq!(
        fetch_service_get_best_blockhash.hash(),
        ret.expect("ret to be Some(GetBlockHash) not None")
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_count<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    test_manager
        .generate_blocks_and_wait_for_tip(5, &fetch_service_subscriber)
        .await;

    let block_id = BlockId {
        height: 7,
        hash: Vec::new(),
    };

    let fetch_service_get_block_count =
        dbg!(fetch_service_subscriber.get_block_count().await.unwrap());

    assert_eq!(fetch_service_get_block_count.0 as u64, block_id.height);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_validate_address<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    // scriptpubkey: "76a914000000000000000000000000000000000000000088ac"
    let expected_validation = ValidateAddressResponse::new(
        true,
        Some("tm9iMLAuYMzJ6jtFLcA7rzUmfreGuKvr7Ma".to_string()),
        Some(false),
    );
    let fetch_service_validate_address = fetch_service_subscriber
        .validate_address("tm9iMLAuYMzJ6jtFLcA7rzUmfreGuKvr7Ma".to_string())
        .await
        .unwrap();

    assert_eq!(fetch_service_validate_address, expected_validation);

    // scriptpubkey: "a914000000000000000000000000000000000000000087"
    let expected_validation_script = ValidateAddressResponse::new(
        true,
        Some("t26YoyZ1iPgiMEWL4zGUm74eVWfhyDMXzY2".to_string()),
        Some(true),
    );

    let fetch_service_validate_address_script = fetch_service_subscriber
        .validate_address("t26YoyZ1iPgiMEWL4zGUm74eVWfhyDMXzY2".to_string())
        .await
        .unwrap();

    assert_eq!(
        fetch_service_validate_address_script,
        expected_validation_script
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_nullifiers<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let block_id = BlockId {
        height: 1,
        hash: Vec::new(),
    };

    let fetch_service_get_block_nullifiers = dbg!(fetch_service_subscriber
        .get_block_nullifiers(block_id.clone())
        .await
        .unwrap());

    assert_eq!(fetch_service_get_block_nullifiers.height, block_id.height);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_range<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    test_manager
        .generate_blocks_and_wait_for_tip(10, &fetch_service_subscriber)
        .await;

    let block_range = BlockRange {
        start: Some(BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: 10,
            hash: Vec::new(),
        }),
        pool_types: vec![],
    };

    let fetch_service_stream = fetch_service_subscriber
        .get_block_range(block_range.clone())
        .await
        .unwrap();
    let fetch_service_compact_blocks: Vec<_> = fetch_service_stream.collect().await;

    let fetch_blocks: Vec<_> = fetch_service_compact_blocks
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    dbg!(fetch_blocks);

    test_manager.close().await;
}

// TODO(#1088): replace deprecated nullifier-range client usage.
#[allow(deprecated)]
async fn fetch_service_get_block_range_nullifiers<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    test_manager
        .generate_blocks_and_wait_for_tip(10, &fetch_service_subscriber)
        .await;

    let block_range = BlockRange {
        start: Some(BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: 10,
            hash: Vec::new(),
        }),
        pool_types: vec![
            PoolType::Transparent as i32,
            PoolType::Sapling as i32,
            PoolType::Orchard as i32,
        ],
    };

    let fetch_service_stream = fetch_service_subscriber
        .get_block_range_nullifiers(block_range.clone())
        .await
        .unwrap();
    let fetch_service_compact_blocks: Vec<_> = fetch_service_stream.collect().await;

    let fetch_nullifiers: Vec<CompactBlock> = fetch_service_compact_blocks
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    dbg!(fetch_nullifiers);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_tree_state<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let chain_height = dbg!(fetch_service_subscriber.chain_height().await.unwrap()).0;

    let block_id = BlockId {
        height: chain_height as u64,
        hash: Vec::new(),
    };

    let fetch_service_get_tree_state = dbg!(fetch_service_subscriber
        .get_tree_state(block_id.clone())
        .await
        .unwrap());

    dbg!(fetch_service_get_tree_state);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_latest_tree_state<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    dbg!(fetch_service_subscriber
        .get_latest_tree_state()
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_subtree_roots<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let subtree_roots_arg = GetSubtreeRootsArg {
        start_index: 0,
        shielded_protocol: 1,
        max_entries: 0,
    };

    let fetch_service_stream = fetch_service_subscriber
        .get_subtree_roots(subtree_roots_arg)
        .await
        .unwrap();
    let fetch_service_roots: Vec<_> = fetch_service_stream.collect().await;

    let fetch_roots: Vec<_> = fetch_service_roots
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    dbg!(fetch_roots);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_lightd_info<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    dbg!(fetch_service_subscriber.get_lightd_info().await.unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn assert_fetch_service_getnetworksols_matches_rpc<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber::<V>(validator, None).await;

    let fetch_service_get_networksolps = fetch_service_subscriber
        .get_network_sol_ps(None, None)
        .await
        .unwrap();

    let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

    let rpc_getnetworksolps_response = jsonrpc_client.get_network_sol_ps(None, None).await.unwrap();
    assert_eq!(fetch_service_get_networksolps, rpc_getnetworksolps_response);
}

mod zcashd {

    use super::*;
    use zcash_local_net::validator::zcashd::Zcashd;

    mod launch {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn regtest_no_cache() {
            launch_fetch_service::<Zcashd>(&ValidatorKind::Zcashd, None).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "We no longer use chain caches. See zcashd::launch::regtest_no_cache."]
        pub(crate) async fn regtest_with_cache() {
            launch_fetch_service::<Zcashd>(
                &ValidatorKind::Zcashd,
                zaino_testutils::ZCASHD_CHAIN_CACHE_DIR.clone(),
            )
            .await;
        }
    }

    mod validation {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn validate_address() {
            fetch_service_validate_address::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[allow(deprecated)]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn z_validate_address() {
            let (mut test_manager, fetch_service_subscriber) =
                zaino_testutils::launch_with_fetch_subscriber::<Zcashd>(
                    &ValidatorKind::Zcashd,
                    None,
                )
                .await;

            let rpc_call = |addr: String| {
                let subscriber = &fetch_service_subscriber;
                async move { subscriber.z_validate_address(addr).await.unwrap() }
            };

            walletless_tests::rpc::z_validate_address::run_z_validate_suite(&rpc_call).await;
            walletless_tests::rpc::z_validate_address::run_z_validate_sapling(&rpc_call).await;

            test_manager.close().await;
        }
    }

    mod get {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_raw() {
            fetch_service_get_block_raw::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_object() {
            fetch_service_get_block_object::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn latest_block() {
            fetch_service_get_latest_block::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block() {
            fetch_service_get_block::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_header() {
            fetch_service_get_block_header::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn difficulty() {
            assert_fetch_service_difficulty_matches_rpc::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[allow(deprecated)]
        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_deltas() {
            let mut test_manager = TestManager::<Zcashd, FetchService>::launch(
                &ValidatorKind::Zcashd,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .await
            .unwrap();

            let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

            let current_block = fetch_service_subscriber.get_latest_block().await.unwrap();

            let block_hash_bytes: [u8; 32] = current_block.hash.as_slice().try_into().unwrap();

            let block_hash = zebra_chain::block::Hash::from(block_hash_bytes);

            // Note: we need an 'expected' block hash in order to query its deltas.
            // Having a predictable or test vector chain is the way to go here.
            let fetch_service_block_deltas = fetch_service_subscriber
                .get_block_deltas(block_hash.to_string())
                .await
                .unwrap();

            let jsonrpc_client = test_manager.full_node_jsonrpc_connector().await;

            let rpc_block_deltas = jsonrpc_client
                .get_block_deltas(block_hash.to_string())
                .await
                .unwrap();

            assert_eq!(fetch_service_block_deltas, rpc_block_deltas);
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mining_info() {
            assert_fetch_service_mininginfo_matches_rpc::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn peer_info() {
            assert_fetch_service_peerinfo_matches_rpc::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_subsidy() {
            fetch_service_get_block_subsidy::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn best_blockhash() {
            fetch_service_get_best_blockhash::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_count() {
            fetch_service_get_block_count::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_nullifiers() {
            fetch_service_get_block_nullifiers::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_range() {
            fetch_service_get_block_range::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_range_nullifiers() {
            fetch_service_get_block_range_nullifiers::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn tree_state() {
            fetch_service_get_tree_state::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn latest_tree_state() {
            fetch_service_get_latest_tree_state::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn subtree_roots() {
            fetch_service_get_subtree_roots::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn lightd_info() {
            fetch_service_get_lightd_info::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn get_network_sol_ps() {
            assert_fetch_service_getnetworksols_matches_rpc::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn get_tx_out_set_info() {
            assert_fetch_service_gettxoutsetinfo_matches_rpc::<Zcashd>(&ValidatorKind::Zcashd)
                .await;
        }
    }
}

mod zebrad {

    use super::*;
    use zcash_local_net::validator::zebrad::Zebrad;

    mod launch {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn regtest_no_cache() {
            launch_fetch_service::<Zebrad>(&ValidatorKind::Zebrad, None).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "We no longer use chain caches. See zebrad::launch::regtest_no_cache."]
        pub(crate) async fn regtest_with_cache() {
            launch_fetch_service::<Zebrad>(
                &ValidatorKind::Zebrad,
                zaino_testutils::ZEBRAD_CHAIN_CACHE_DIR.clone(),
            )
            .await;
        }
    }

    mod validation {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn validate_address() {
            fetch_service_validate_address::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[allow(deprecated)]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn z_validate_address() {
            let (mut test_manager, fetch_service_subscriber) =
                zaino_testutils::launch_with_fetch_subscriber::<Zebrad>(
                    &ValidatorKind::Zebrad,
                    None,
                )
                .await;

            let rpc_call = |addr: String| {
                let subscriber = &fetch_service_subscriber;
                async move { subscriber.z_validate_address(addr).await.unwrap() }
            };

            walletless_tests::rpc::z_validate_address::run_z_validate_suite(&rpc_call).await;
            walletless_tests::rpc::z_validate_address::run_z_validate_sapling_zebrad_passthrough_fetchservice(&rpc_call).await;

            test_manager.close().await;
        }
    }

    mod get {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_raw() {
            fetch_service_get_block_raw::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_object() {
            fetch_service_get_block_object::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn latest_block() {
            fetch_service_get_latest_block::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block() {
            fetch_service_get_block::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_header() {
            fetch_service_get_block_header::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn difficulty() {
            assert_fetch_service_difficulty_matches_rpc::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mining_info() {
            assert_fetch_service_mininginfo_matches_rpc::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn peer_info() {
            assert_fetch_service_peerinfo_matches_rpc::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_subsidy() {
            fetch_service_get_block_subsidy::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn best_blockhash() {
            fetch_service_get_best_blockhash::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_count() {
            fetch_service_get_block_count::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_nullifiers() {
            fetch_service_get_block_nullifiers::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_range_nullifiers() {
            fetch_service_get_block_range_nullifiers::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn tree_state() {
            fetch_service_get_tree_state::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn latest_tree_state() {
            fetch_service_get_latest_tree_state::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn subtree_roots() {
            fetch_service_get_subtree_roots::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn lightd_info() {
            fetch_service_get_lightd_info::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn get_network_sol_ps() {
            assert_fetch_service_getnetworksols_matches_rpc::<Zebrad>(&ValidatorKind::Zebrad).await;
        }
    }
}
