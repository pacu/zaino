use futures::StreamExt;
use zaino_fetch::jsonrpsee::response::address_deltas::GetAddressDeltasParams;
use zaino_proto::proto::service::{BlockId, BlockRange, PoolType, TransparentAddressBlockFilter};
use zaino_state::ChainIndex as _;

#[allow(deprecated)]
use zaino_state::{
    FetchService, FetchServiceSubscriber, LightWalletIndexer, StateService, StateServiceSubscriber,
    ZcashIndexer,
};
use zaino_testutils::ValidatorExt;
use zaino_testutils::{TestManager, ValidatorKind};
use zcash_local_net::validator::zebrad::Zebrad;
use zebra_chain::parameters::NetworkKind;
use zebra_chain::subtree::NoteCommitmentSubtreeIndex;
use zebra_rpc::methods::{GetAddressBalanceRequest, GetAddressTxIdsRequest};
use zip32::AccountId;

#[allow(deprecated)]
// NOTE: the fetch and state services each have a seperate chain index to the instance of zaino connected to the lightclients and may be out of sync
// the test manager now includes a service subscriber but not both fetch *and* state which are necessary for these tests.
// syncronicity is ensured in the following tests by calling `TestManager::generate_blocks_and_wait_for_tips`.
async fn create_test_manager_and_services<V: ValidatorExt>(
    validator: &ValidatorKind,
    chain_cache: Option<std::path::PathBuf>,
    enable_zaino: bool,
    network: Option<NetworkKind>,
) -> (
    TestManager<V, StateService>,
    FetchService,
    FetchServiceSubscriber,
    StateService,
    StateServiceSubscriber,
    wallet_tests::Clients,
) {
    let (test_manager, fetch_service, fetch_subscriber, state_service, state_subscriber) =
        zaino_testutils::launch_state_and_fetch_services(
            validator,
            chain_cache,
            enable_zaino,
            network,
        )
        .await;

    let clients = wallet_tests::build_clients(
        test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
        wallet_tests::default_heights(validator),
    );

    (
        test_manager,
        fetch_service,
        fetch_subscriber,
        state_service,
        state_subscriber,
        clients,
    )
}

#[allow(deprecated)]
async fn state_service_get_address_balance<V: ValidatorExt>(validator: &ValidatorKind) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    clients.send_from_faucet(recipient_taddr.as_str(), 250_000).await;
    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    clients.recipient.sync_and_await().await.unwrap();
    let recipient_balance = clients.recipient_balance().await;

    let fetch_service_balance = fetch_service_subscriber
        .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
        .await
        .unwrap();

    let state_service_balance = state_service_subscriber
        .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_taddr]))
        .await
        .unwrap();

    dbg!(&recipient_balance);
    dbg!(&fetch_service_balance);
    dbg!(&state_service_balance);

    assert_eq!(
        wallet_tests::Pool::Transparent.received_balance(&recipient_balance),
        250_000,
    );
    assert_eq!(
        wallet_tests::Pool::Transparent.received_balance(&recipient_balance),
        fetch_service_balance.balance(),
    );
    assert_eq!(fetch_service_balance, state_service_balance);

    test_manager.close().await;
}

async fn state_service_get_raw_mempool<V: ValidatorExt>(validator: &ValidatorKind) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.send_from_faucet(&recipient_taddr, 250_000).await;
    clients.send_from_faucet(&recipient_ua, 250_000).await;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut fetch_service_mempool = fetch_service_subscriber.get_raw_mempool().await.unwrap();
    let mut state_service_mempool = state_service_subscriber.get_raw_mempool().await.unwrap();

    dbg!(&fetch_service_mempool);
    fetch_service_mempool.sort();

    dbg!(&state_service_mempool);
    state_service_mempool.sort();

    assert_eq!(fetch_service_mempool, state_service_mempool);

    test_manager.close().await;
}

/// Tests whether that calls to `get_block_range` with the same block range are the same when
/// specifying the default `PoolType`s and passing and empty Vec to verify that the method falls
/// back to the default pools when these are not explicitly specified.
async fn state_service_get_block_range_returns_default_pools<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    clients.send_from_faucet(&recipient_ua, 250_000).await;

    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    let start_height: u64 = 100;
    let end_height: u64 = 103;

    let fetch_service_get_block_range = zaino_testutils::collect_block_range(
        &fetch_service_subscriber,
        start_height,
        end_height,
        vec![],
    )
    .await;

    let fetch_service_get_block_range_specifying_pools = zaino_testutils::collect_block_range(
        &fetch_service_subscriber,
        start_height,
        end_height,
        vec![PoolType::Sapling as i32, PoolType::Orchard as i32],
    )
    .await;

    assert_eq!(
        fetch_service_get_block_range,
        fetch_service_get_block_range_specifying_pools
    );

    let state_service_get_block_range_specifying_pools = zaino_testutils::collect_block_range(
        &state_service_subscriber,
        start_height,
        end_height,
        vec![PoolType::Sapling as i32, PoolType::Orchard as i32],
    )
    .await;

    let state_service_get_block_range = zaino_testutils::collect_block_range(
        &state_service_subscriber,
        start_height,
        end_height,
        vec![],
    )
    .await;

    assert_eq!(
        state_service_get_block_range,
        state_service_get_block_range_specifying_pools
    );

    // check that the block range is the same between fetch service and state service
    assert_eq!(fetch_service_get_block_range, state_service_get_block_range);

    let compact_block = state_service_get_block_range.last().unwrap();

    assert_eq!(compact_block.height, end_height);

    // the compact block has 1 transactions
    assert_eq!(compact_block.vtx.len(), 1);

    let shielded_tx = compact_block.vtx.first().unwrap();
    assert_eq!(shielded_tx.index, 1);
    // tranparent data should not be present when no pool types are requested
    assert_eq!(
        shielded_tx.vin,
        vec![],
        "transparent data should not be present when no pool types are specified in the request."
    );
    assert_eq!(
        shielded_tx.vout,
        vec![],
        "transparent data should not be present when no pool types are specified in the request."
    );
    test_manager.close().await;
}

/// tests whether the `GetBlockRange` RPC returns all pools when requested
async fn state_service_get_block_range_returns_all_pools<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        for _ in 1..4 {
            clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
            test_manager
                .generate_blocks_and_wait_for_tips(
                    1,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;

            clients.faucet.sync_and_await().await.unwrap();
        }
    };

    let recipient_transparent = clients.get_recipient_address("transparent").await;
    let deshielding_txid = clients.send_from_faucet(&recipient_transparent, 250_000).await.head;

    let recipient_sapling = clients.get_recipient_address("sapling").await;
    let sapling_txid = clients.send_from_faucet(&recipient_sapling, 250_000).await.head;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let orchard_txid =
        clients.send_from_faucet(&recipient_ua, 250_000).await.head;

    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    let start_height: u64 = 100;
    let end_height: u64 = 106;
    let all_pools = vec![
        PoolType::Transparent as i32,
        PoolType::Sapling as i32,
        PoolType::Orchard as i32,
    ];

    let fetch_service_get_block_range = zaino_testutils::collect_block_range(
        &fetch_service_subscriber,
        start_height,
        end_height,
        all_pools.clone(),
    )
    .await;

    let state_service_get_block_range = zaino_testutils::collect_block_range(
        &state_service_subscriber,
        start_height,
        end_height,
        all_pools,
    )
    .await;

    // check that the block range is the same
    assert_eq!(fetch_service_get_block_range, state_service_get_block_range);

    let compact_block = state_service_get_block_range.last().unwrap();

    assert_eq!(compact_block.height, end_height);

    // the compact block has 4 transactions (3 sent + coinbase)
    assert_eq!(compact_block.vtx.len(), 4);

    wallet_tests::assert_pool_present(
        compact_block,
        &deshielding_txid,
        wallet_tests::Pool::Transparent,
    );
    wallet_tests::assert_pool_present(compact_block, &sapling_txid, wallet_tests::Pool::Sapling);
    wallet_tests::assert_pool_present(compact_block, &orchard_txid, wallet_tests::Pool::Orchard);

    test_manager.close().await;
}

// tests whether the `GetBlockRange` returns all blocks until the first requested block in the
// range can't be bound
async fn state_service_get_block_range_out_of_range_test_upper_bound<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    clients.faucet.sync_and_await().await.unwrap();

    // Test manager generates blocks on startup, check current height to ensure we only generate up to height 100
    let chain_height = state_service_subscriber
        .get_latest_block()
        .await
        .unwrap()
        .height as u32;
    let block_required_height_100 = 100 - chain_height;

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                block_required_height_100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let start_height: u64 = 1;
    let end_height: u64 = 106;

    let block_range = BlockRange {
        start: Some(BlockId {
            height: start_height,
            hash: vec![],
        }),
        end: Some(BlockId {
            height: end_height,
            hash: vec![],
        }),
        pool_types: vec![
            PoolType::Transparent as i32,
            PoolType::Sapling as i32,
            PoolType::Orchard as i32,
        ],
    };

    let mut fetch_service_stream = fetch_service_subscriber
        .get_block_range(block_range.clone())
        .await
        .expect("get_block_range call itself should not fail");

    let mut fetch_service_blocks = Vec::new();
    let mut fetch_service_terminal_error = None;

    while let Some(item) = fetch_service_stream.next().await {
        match item {
            Ok(block) => fetch_service_blocks.push(block),
            Err(e) => {
                fetch_service_terminal_error = Some(e);
                break;
            }
        }
    }

    let mut state_service_stream = state_service_subscriber
        .get_block_range(block_range)
        .await
        .expect("get_block_range call itself should not fail");

    let mut state_service_blocks = Vec::new();
    let mut state_service_terminal_error = None;

    while let Some(item) = state_service_stream.next().await {
        match item {
            Ok(block) => state_service_blocks.push(block),
            Err(e) => {
                state_service_terminal_error = Some(e);
                break;
            }
        }
    }
    // check that the block range is the same
    assert_eq!(fetch_service_blocks, state_service_blocks);

    let compact_block = state_service_blocks.last().unwrap();

    assert!(compact_block.height < end_height);

    assert_eq!(fetch_service_blocks.len(), 100);

    // Assert – then an error, not a clean end-of-stream
    let _ = state_service_terminal_error
        .expect("state service stream should terminate with an error, not cleanly");
    let _ = fetch_service_terminal_error
        .expect("fetch service stream should terminate with an error, not cleanly");

    test_manager.close().await;
}

// tests whether the `GetBlockRange` returns all blocks until the first requested block in the
// range can't be bound
async fn state_service_get_block_range_out_of_range_test_lower_bound<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    clients.faucet.sync_and_await().await.unwrap();

    // Test manager generates blocks on startup, check current height to ensure we only generate up to height 100
    let chain_height = state_service_subscriber
        .get_latest_block()
        .await
        .unwrap()
        .height as u32;
    let block_required_height_100 = 100 - chain_height;

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                block_required_height_100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let start_height: u64 = 106;
    let end_height: u64 = 1;

    let block_range = BlockRange {
        start: Some(BlockId {
            height: start_height,
            hash: vec![],
        }),
        end: Some(BlockId {
            height: end_height,
            hash: vec![],
        }),
        pool_types: vec![
            PoolType::Transparent as i32,
            PoolType::Sapling as i32,
            PoolType::Orchard as i32,
        ],
    };

    let mut fetch_service_stream = fetch_service_subscriber
        .get_block_range(block_range.clone())
        .await
        .expect("get_block_range call itself should not fail");

    let mut fetch_service_blocks = Vec::new();
    let mut fetch_service_terminal_error = None;

    while let Some(item) = fetch_service_stream.next().await {
        match item {
            Ok(block) => fetch_service_blocks.push(block),
            Err(e) => {
                fetch_service_terminal_error = Some(e);
                break;
            }
        }
    }

    let mut state_service_stream = state_service_subscriber
        .get_block_range(block_range)
        .await
        .expect("get_block_range call itself should not fail");

    let mut state_service_blocks = Vec::new();
    let mut state_service_terminal_error = None;

    while let Some(item) = state_service_stream.next().await {
        match item {
            Ok(block) => state_service_blocks.push(block),
            Err(e) => {
                state_service_terminal_error = Some(e);
                break;
            }
        }
    }
    // check that the block range is the same
    assert_eq!(fetch_service_blocks, state_service_blocks);

    assert!(fetch_service_blocks.is_empty());

    // Assert – then an error, not a clean end-of-stream
    let _ = state_service_terminal_error
        .expect("state service stream should terminate with an error, not cleanly");
    let _ = fetch_service_terminal_error
        .expect("fetch service stream should terminate with an error, not cleanly");
    // assert!(
    //     matches!(err, ZainoStateError::BlockOutOfRange { .. }),
    //     "unexpected error variant: {err:?}"
    // );

    test_manager.close().await;
}

async fn state_service_z_get_treestate<V: ValidatorExt>(validator: &ValidatorKind) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    clients.send_from_faucet(&recipient_ua, 250_000).await;

    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    let chain_height = dbg!(state_service_subscriber.chain_height().await.unwrap()).0;

    let fetch_service_treestate = dbg!(fetch_service_subscriber
        .z_get_treestate(chain_height.to_string())
        .await
        .unwrap());

    let state_service_treestate = dbg!(state_service_subscriber
        .z_get_treestate(chain_height.to_string())
        .await
        .unwrap());

    assert_eq!(fetch_service_treestate, state_service_treestate);

    test_manager.close().await;
}

async fn state_service_z_get_subtrees_by_index<V: ValidatorExt>(validator: &ValidatorKind) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    clients.send_from_faucet(&recipient_ua, 250_000).await;

    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    let fetch_service_subtrees = dbg!(fetch_service_subscriber
        .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
        .await
        .unwrap());

    let state_service_subtrees = dbg!(state_service_subscriber
        .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
        .await
        .unwrap());

    assert_eq!(fetch_service_subtrees, state_service_subtrees);

    test_manager.close().await;
}

use zcash_local_net::logs::LogsToStdoutAndStderr;
async fn state_service_get_raw_transaction<V: ValidatorExt + LogsToStdoutAndStderr>(
    validator: &ValidatorKind,
) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await.to_string();
    let tx = clients.send_from_faucet(&recipient_ua, 250_000).await;

    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    test_manager.local_net.print_stdout();

    let fetch_service_transaction = dbg!(fetch_service_subscriber
        .get_raw_transaction(tx.first().to_string(), Some(1))
        .await
        .unwrap());

    let state_service_transaction = dbg!(state_service_subscriber
        .get_raw_transaction(tx.first().to_string(), Some(1))
        .await
        .unwrap());

    assert_eq!(fetch_service_transaction, state_service_transaction);

    test_manager.close().await;
}

async fn state_service_get_address_transactions_regtest<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager.local_net.generate_blocks(100).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
        clients.faucet.sync_and_await().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager.local_net.generate_blocks(1).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        clients.faucet.sync_and_await().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    let tx = clients.send_from_faucet(recipient_taddr.as_str(), 250_000).await;
    test_manager.local_net.generate_blocks(1).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let chain_height: u32 = {
        let idx = &fetch_service_subscriber.indexer;
        let snapshot = idx.snapshot_nonfinalized_state().await.unwrap();
        u32::from(idx.best_chaintip(&snapshot).await.unwrap().height)
    };
    dbg!(&chain_height);

    let state_service_txids = state_service_subscriber
        .get_taddress_transactions(TransparentAddressBlockFilter {
            address: recipient_taddr,
            range: Some(BlockRange {
                start: Some(BlockId {
                    height: (chain_height - 2) as u64,
                    hash: vec![],
                }),
                end: Some(BlockId {
                    height: chain_height as u64,
                    hash: vec![],
                }),
                pool_types: vec![
                    PoolType::Transparent as i32,
                    PoolType::Sapling as i32,
                    PoolType::Orchard as i32,
                ],
            }),
        })
        .await
        .unwrap();

    dbg!(&tx);

    dbg!(&state_service_txids);
    assert!(state_service_txids.count().await > 0);

    test_manager.close().await;
}
async fn state_service_get_address_tx_ids<V: ValidatorExt>(validator: &ValidatorKind) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let tx = clients.send_from_faucet(recipient_taddr.as_str(), 250_000).await;
    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    let chain_height: u32 = {
        let idx = &fetch_service_subscriber.indexer;
        let snapshot = idx.snapshot_nonfinalized_state().await.unwrap();
        u32::from(idx.best_chaintip(&snapshot).await.unwrap().height)
    };

    dbg!(&chain_height);

    let fetch_service_txids = fetch_service_subscriber
        .get_address_tx_ids(GetAddressTxIdsRequest::new(
            vec![recipient_taddr.clone()],
            Some(chain_height - 2),
            Some(chain_height),
        ))
        .await
        .unwrap();

    let state_service_txids = state_service_subscriber
        .get_address_tx_ids(GetAddressTxIdsRequest::new(
            vec![recipient_taddr],
            Some(chain_height - 2),
            Some(chain_height),
        ))
        .await
        .unwrap();

    dbg!(&tx);
    dbg!(&fetch_service_txids);
    assert_eq!(tx.first().to_string(), fetch_service_txids[0]);

    dbg!(&state_service_txids);
    assert_eq!(fetch_service_txids, state_service_txids);

    test_manager.close().await;
}

async fn state_service_get_address_utxos<V: ValidatorExt>(validator: &ValidatorKind) {
    let (
        mut test_manager,
        _fetch_service,
        fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
        mut clients,
    ) = create_test_manager_and_services::<V>(validator, None, true, None).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tips(
                100,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tips(
                1,
                &fetch_service_subscriber,
                &state_service_subscriber,
            )
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let txid_1 = clients.send_from_faucet(recipient_taddr.as_str(), 250_000).await;
    test_manager
        .generate_blocks_and_wait_for_tips(1, &fetch_service_subscriber, &state_service_subscriber)
        .await;

    clients.faucet.sync_and_await().await.unwrap();

    let fetch_service_utxos = fetch_service_subscriber
        .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
        .await
        .unwrap();
    let (_, fetch_service_txid, ..) = fetch_service_utxos[0].into_parts();

    let state_service_utxos = state_service_subscriber
        .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr]))
        .await
        .unwrap();
    let (_, state_service_txid, ..) = state_service_utxos[0].into_parts();

    dbg!(&txid_1);
    dbg!(&fetch_service_utxos);
    assert_eq!(txid_1.first().to_string(), fetch_service_txid.to_string());

    dbg!(&state_service_utxos);

    assert_eq!(
        fetch_service_txid.to_string(),
        state_service_txid.to_string()
    );

    test_manager.close().await;
}

mod zebra {

    use super::*;

    pub(crate) mod get {

        use super::*;
        use zaino_fetch::jsonrpsee::response::address_deltas::GetAddressDeltasResponse;
        use zcash_local_net::validator::zebrad::Zebrad;

        #[tokio::test(flavor = "multi_thread")]
        async fn address_utxos() {
            state_service_get_address_utxos::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn taddress_transactions_regtest() {
            state_service_get_address_transactions_regtest::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn address_tx_ids_regtest() {
            state_service_get_address_tx_ids::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn raw_transaction_regtest() {
            state_service_get_raw_transaction::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        mod z {
            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn get_block_range_default_request_returns_no_t_data_regtest() {
                state_service_get_block_range_returns_default_pools::<Zebrad>(
                    &ValidatorKind::Zebrad,
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn get_block_range_default_request_returns_all_pools_regtest() {
                state_service_get_block_range_returns_all_pools::<Zebrad>(&ValidatorKind::Zebrad)
                    .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn get_block_range_out_of_range_test_upper_bound_regtest() {
                state_service_get_block_range_out_of_range_test_upper_bound::<Zebrad>(
                    &ValidatorKind::Zebrad,
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn get_block_range_out_of_range_test_lower_bound_regtest() {
                state_service_get_block_range_out_of_range_test_lower_bound::<Zebrad>(
                    &ValidatorKind::Zebrad,
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn subtrees_by_index_regtest() {
                state_service_z_get_subtrees_by_index::<Zebrad>(&ValidatorKind::Zebrad).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn treestate_regtest() {
                state_service_z_get_treestate::<Zebrad>(&ValidatorKind::Zebrad).await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn raw_mempool_regtest() {
            state_service_get_raw_mempool::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        /// `getmempoolinfo` computed from local Broadcast state
        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn get_mempool_info() {
            let (
                mut test_manager,
                _fetch_service,
                fetch_service_subscriber,
                _state_service,
                state_service_subscriber,
                mut clients,
            ) = create_test_manager_and_services::<Zebrad>(
                &ValidatorKind::Zebrad,
                None,
                true,
                None,
            )
            .await;

            let recipient_taddr = clients.get_recipient_address("transparent").await;

            clients.faucet.sync_and_await().await.unwrap();

            test_manager
                .generate_blocks_and_wait_for_tips(
                    100,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;
            clients.faucet.sync_and_await().await.unwrap();
            clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
            test_manager
                .generate_blocks_and_wait_for_tips(
                    1,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;
            clients.faucet.sync_and_await().await.unwrap();

            clients.send_from_faucet(recipient_taddr.as_str(), 250_000).await;

            // Let the broadcaster/subscribers observe the new tx
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            // Call the internal mempool info method
            let info = state_service_subscriber.get_mempool_info().await.unwrap();

            // Derive expected values directly from the current mempool contents
            let entries = state_service_subscriber.mempool.get_mempool().await;

            assert_eq!(entries.len() as u64, info.size);
            assert!(info.size >= 1);

            let expected_bytes: u64 = entries
                .iter()
                .map(|(_, v)| v.serialized_tx.as_ref().as_ref().len() as u64)
                .sum();

            let expected_key_heap_bytes: u64 =
                entries.iter().map(|(k, _)| k.txid.capacity() as u64).sum();

            let expected_usage = expected_bytes.saturating_add(expected_key_heap_bytes);

            assert!(info.bytes > 0);
            assert_eq!(info.bytes, expected_bytes);

            assert!(info.usage >= info.bytes);
            assert_eq!(info.usage, expected_usage);

            // Optional: when exactly one tx, its serialized length must equal `bytes`
            if info.size == 1 {
                let (_, mem_value) = entries[0].clone();
                assert_eq!(
                    mem_value.serialized_tx.as_ref().as_ref().len() as u64,
                    expected_bytes
                );
            }

            test_manager.close().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn address_balance_regtest() {
            state_service_get_address_balance::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn address_deltas() {
            address_deltas::main().await;
        }

        mod address_deltas;
    }

    pub(crate) mod lightwallet_indexer {
        use futures::StreamExt as _;
        use zaino_proto::proto::{
            service::{AddressList, BlockId, GetAddressUtxosArg, PoolType, TxFilter},
            utils::pool_types_into_i32_vec,
        };
        use zebra_rpc::methods::GetAddressTxIdsRequest;

        use super::*;
        #[tokio::test(flavor = "multi_thread")]
        async fn get_transaction() {
            let (
                test_manager,
                _fetch_service,
                fetch_service_subscriber,
                _state_service,
                state_service_subscriber,
                mut clients,
            ) = create_test_manager_and_services::<Zebrad>(
                &ValidatorKind::Zebrad,
                None,
                true,
                Some(NetworkKind::Regtest),
            )
            .await;
            test_manager
                .generate_blocks_and_wait_for_tips(
                    100,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;

            clients.faucet.sync_and_await().await.unwrap();
            clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();

            test_manager
                .generate_blocks_and_wait_for_tips(
                    2,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;

            let block = BlockId {
                height: 103,
                hash: vec![],
            };
            let state_service_block_by_height = state_service_subscriber
                .get_block(block.clone())
                .await
                .unwrap();
            let coinbase_tx = state_service_block_by_height.vtx.first().unwrap();
            let hash = coinbase_tx.txid.clone();
            let request = TxFilter {
                block: None,
                index: 0,
                hash,
            };
            let fetch_service_raw_transaction = fetch_service_subscriber
                .get_transaction(request.clone())
                .await
                .unwrap();
            let state_service_raw_transaction = state_service_subscriber
                .get_transaction(request)
                .await
                .unwrap();
            assert_eq!(fetch_service_raw_transaction, state_service_raw_transaction);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_taddress_txids() {
            let (
                test_manager,
                _fetch_service,
                fetch_service_subscriber,
                _state_service,
                state_service_subscriber,
                clients,
            ) = create_test_manager_and_services::<Zebrad>(
                &ValidatorKind::Zebrad,
                None,
                true,
                Some(NetworkKind::Regtest),
            )
            .await;

            let taddr = clients.get_faucet_address("transparent").await;
            test_manager
                .generate_blocks_and_wait_for_tips(
                    100,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;

            let state_service_taddress_txids = state_service_subscriber
                .get_address_tx_ids(GetAddressTxIdsRequest::new(
                    vec![taddr.clone()],
                    Some(2),
                    Some(5),
                ))
                .await
                .unwrap();
            dbg!(&state_service_taddress_txids);
            let fetch_service_taddress_txids = fetch_service_subscriber
                .get_address_tx_ids(GetAddressTxIdsRequest::new(vec![taddr], Some(2), Some(5)))
                .await
                .unwrap();
            dbg!(&fetch_service_taddress_txids);
            assert_eq!(fetch_service_taddress_txids, state_service_taddress_txids);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_address_utxos_stream() {
            let (
                test_manager,
                _fetch_service,
                fetch_service_subscriber,
                _state_service,
                state_service_subscriber,
                mut clients,
            ) = create_test_manager_and_services::<Zebrad>(
                &ValidatorKind::Zebrad,
                None,
                true,
                Some(NetworkKind::Regtest),
            )
            .await;

            let taddr = clients.get_faucet_address("transparent").await;
            test_manager
                .generate_blocks_and_wait_for_tips(
                    5,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;
            let request = GetAddressUtxosArg {
                addresses: vec![taddr],
                start_height: 2,
                max_entries: 3,
            };
            let state_service_address_utxos_streamed = state_service_subscriber
                .get_address_utxos_stream(request.clone())
                .await
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .await;
            let fetch_service_address_utxos_streamed = fetch_service_subscriber
                .get_address_utxos_stream(request)
                .await
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .await;
            assert_eq!(
                fetch_service_address_utxos_streamed,
                state_service_address_utxos_streamed
            );
            clients.faucet.sync_and_await().await.unwrap();
            assert_eq!(
                fetch_service_address_utxos_streamed.first().unwrap().txid,
                clients
                    .faucet
                    .transaction_summaries(false)
                    .await
                    .unwrap()
                    .txids()[1]
                    .as_ref()
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_address_utxos() {
            let (
                test_manager,
                _fetch_service,
                fetch_service_subscriber,
                _state_service,
                state_service_subscriber,
                mut clients,
            ) = create_test_manager_and_services::<Zebrad>(
                &ValidatorKind::Zebrad,
                None,
                true,
                Some(NetworkKind::Regtest),
            )
            .await;

            let taddr = clients.get_faucet_address("transparent").await;
            test_manager
                .generate_blocks_and_wait_for_tips(
                    5,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;
            let request = GetAddressUtxosArg {
                addresses: vec![taddr],
                start_height: 2,
                max_entries: 3,
            };
            let state_service_address_utxos = state_service_subscriber
                .get_address_utxos(request.clone())
                .await
                .unwrap();
            let fetch_service_address_utxos = fetch_service_subscriber
                .get_address_utxos(request)
                .await
                .unwrap();
            assert_eq!(fetch_service_address_utxos, state_service_address_utxos);
            clients.faucet.sync_and_await().await.unwrap();
            assert_eq!(
                fetch_service_address_utxos
                    .address_utxos
                    .first()
                    .unwrap()
                    .txid,
                clients
                    .faucet
                    .transaction_summaries(false)
                    .await
                    .unwrap()
                    .txids()[1]
                    .as_ref()
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_taddress_balance() {
            let (
                test_manager,
                _fetch_service,
                fetch_service_subscriber,
                _state_service,
                state_service_subscriber,
                clients,
            ) = create_test_manager_and_services::<Zebrad>(
                &ValidatorKind::Zebrad,
                None,
                true,
                Some(NetworkKind::Regtest),
            )
            .await;

            let taddr = clients.get_faucet_address("transparent").await;
            test_manager
                .generate_blocks_and_wait_for_tips(
                    5,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;

            let state_service_taddress_balance = state_service_subscriber
                .get_taddress_balance(AddressList {
                    addresses: vec![taddr.clone()],
                })
                .await
                .unwrap();
            let fetch_service_taddress_balance = fetch_service_subscriber
                .get_taddress_balance(AddressList {
                    addresses: vec![taddr],
                })
                .await
                .unwrap();
            assert_eq!(
                fetch_service_taddress_balance,
                state_service_taddress_balance
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_transparent_data_from_compact_block_when_requested() {
            let (
                test_manager,
                _fetch_service,
                fetch_service_subscriber,
                _state_service,
                state_service_subscriber,
                clients,
            ) = create_test_manager_and_services::<Zebrad>(
                &ValidatorKind::Zebrad,
                None,
                true,
                Some(NetworkKind::Regtest),
            )
            .await;

            let taddr = clients.get_faucet_address("transparent").await;
            test_manager
                .generate_blocks_and_wait_for_tips(
                    5,
                    &fetch_service_subscriber,
                    &state_service_subscriber,
                )
                .await;

            let state_service_taddress_balance = state_service_subscriber
                .get_taddress_balance(AddressList {
                    addresses: vec![taddr.clone()],
                })
                .await
                .unwrap();
            let fetch_service_taddress_balance = fetch_service_subscriber
                .get_taddress_balance(AddressList {
                    addresses: vec![taddr],
                })
                .await
                .unwrap();
            assert_eq!(
                fetch_service_taddress_balance,
                state_service_taddress_balance
            );

            let chain_height = state_service_subscriber
                .get_latest_block()
                .await
                .unwrap()
                .height;

            // NOTE / TODO: Zaino can not currently serve non standard script types in compact blocks,
            // because of this it does not return the script pub key for the coinbase transaction of the
            // genesis block. We should decide whether / how to fix this.
            //
            // For this reason this test currently does not fetch the genesis block.
            //
            // Issue: https://github.com/zingolabs/zaino/issues/818
            //
            // To see bug update start height of get_block_range to 0.
            let compact_block_range = zaino_testutils::collect_block_range(
                &state_service_subscriber,
                1,
                chain_height,
                pool_types_into_i32_vec(
                    [PoolType::Transparent, PoolType::Sapling, PoolType::Orchard].to_vec(),
                ),
            )
            .await;

            for cb in compact_block_range.into_iter() {
                for tx in cb.vtx {
                    dbg!(&tx);
                    // script pub key of this transaction is not empty
                    assert!(!tx.vout.first().unwrap().script_pub_key.is_empty());
                }
            }
        }
    }
}
