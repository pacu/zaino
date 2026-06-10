//! These tests compare the output of `FetchService` with the output of `JsonRpcConnector`.

use futures::StreamExt as _;
use hex::ToHex as _;
use zaino_fetch::jsonrpsee::connector::{test_node_and_return_url, JsonRpSeeConnector};
use zaino_proto::proto::compact_formats::CompactBlock;
use zaino_proto::proto::service::{
    AddressList, BlockId, BlockRange, GetAddressUtxosArg, GetMempoolTxRequest, GetSubtreeRootsArg,
    PoolType, TransparentAddressBlockFilter, TxFilter,
};
use zaino_state::ChainIndex;
use zaino_state::FetchServiceSubscriber;
#[allow(deprecated)]
use zaino_state::{FetchService, LightWalletIndexer, Status, StatusType, ZcashIndexer};
use zaino_testutils::{TestManager, ValidatorExt, ValidatorKind};
use zebra_chain::parameters::subsidy::ParameterSubsidy as _;
use zebra_chain::subtree::NoteCommitmentSubtreeIndex;
use zebra_rpc::client::ValidateAddressResponse;
use zebra_rpc::methods::{
    GetAddressBalanceRequest, GetAddressTxIdsRequest, GetBlock, GetBlockHash,
};
use zip32::AccountId;

#[allow(deprecated)]
async fn create_test_manager_and_fetch_service<V: ValidatorExt>(
    validator: &ValidatorKind,
    chain_cache: Option<std::path::PathBuf>,
    enable_clients: bool,
) -> (TestManager<V, FetchService>, FetchServiceSubscriber) {
    let mut test_manager = TestManager::<V, FetchService>::launch(
        validator,
        None,
        None,
        chain_cache,
        true,
        false,
        enable_clients,
    )
    .await
    .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();
    (test_manager, fetch_service_subscriber)
}

#[allow(deprecated)]
async fn fetch_service_get_address_balance<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_address = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    dbg!(clients
        .faucet
        .account_balance(AccountId::ZERO)
        .await
        .unwrap());
    dbg!(clients.faucet.transaction_summaries(false).await.unwrap());

    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_address.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    clients.recipient.sync_and_await().await.unwrap();
    let recipient_balance = clients
        .recipient
        .account_balance(zip32::AccountId::ZERO)
        .await
        .unwrap();

    let fetch_service_balance = fetch_service_subscriber
        .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_address]))
        .await
        .unwrap();

    dbg!(recipient_balance.clone());
    dbg!(fetch_service_balance);

    assert_eq!(
        recipient_balance
            .confirmed_transparent_balance
            .unwrap()
            .into_u64(),
        250_000,
    );
    assert_eq!(
        recipient_balance
            .confirmed_transparent_balance
            .unwrap()
            .into_u64(),
        fetch_service_balance.balance(),
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_raw_mempool<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");

    let json_service = JsonRpSeeConnector::new_with_basic_auth(
        test_node_and_return_url(
            &test_manager.full_node_rpc_listen_address.to_string(),
            None,
            Some("xxxxxx".to_string()),
            Some("xxxxxx".to_string()),
        )
        .await
        .unwrap(),
        "xxxxxx".to_string(),
        "xxxxxx".to_string(),
    )
    .unwrap();

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua: String = clients.get_recipient_address("unified").await;
    let recipient_taddr: String = clients.get_recipient_address("transparent").await;
    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_taddr, 250_000, None)],
    )
    .await
    .unwrap();
    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut fetch_service_mempool = fetch_service_subscriber.get_raw_mempool().await.unwrap();
    let mut json_service_mempool = json_service.get_raw_mempool().await.unwrap().transactions;

    dbg!(&fetch_service_mempool);
    dbg!(&json_service_mempool);
    json_service_mempool.sort();
    fetch_service_mempool.sort();
    assert_eq!(json_service_mempool, fetch_service_mempool);

    test_manager.close().await;
}

// `getmempoolinfo` computed from local Broadcast state for all validators
#[allow(deprecated)]
pub async fn test_get_mempool_info<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;
    clients.faucet.sync_and_await().await.unwrap();

    // Zebra cannot mine directly to Orchard in this setup, so shield funds first.
    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();

        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();

        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    }

    let recipient_unified_address = clients.get_recipient_address("unified").await;
    let recipient_transparent_address = clients.get_recipient_address("transparent").await;

    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_transparent_address, 250_000, None)],
    )
    .await
    .unwrap();

    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_unified_address, 250_000, None)],
    )
    .await
    .unwrap();

    // Allow the broadcaster and subscribers to observe new transactions.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Internal method now used for all validators.
    let info = fetch_service_subscriber.get_mempool_info().await.unwrap();

    // Derive expected values directly from the current mempool contents.

    let keys = fetch_service_subscriber
        .indexer
        .get_mempool_txids()
        .await
        .unwrap();

    let values = fetch_service_subscriber
        .indexer
        .get_mempool_transactions(Vec::new())
        .await
        .unwrap();

    // Size
    assert_eq!(info.size, values.len() as u64);
    assert!(info.size >= 1);

    // Bytes: sum of SerializedTransaction lengths
    let expected_bytes: u64 = values.iter().map(|entry| entry.len() as u64).sum();

    // Key heap bytes: sum of txid String capacities
    let expected_key_heap_bytes: u64 = keys
        .iter()
        .map(|key| key.encode_hex::<String>().capacity() as u64)
        .sum();

    let expected_usage = expected_bytes.saturating_add(expected_key_heap_bytes);

    assert!(info.bytes > 0);
    assert_eq!(info.bytes, expected_bytes);

    assert!(info.usage >= info.bytes);
    assert_eq!(info.usage, expected_usage);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_z_get_treestate<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        // TODO: investigate why 101 blocks are needed instead of the previous 100 blocks (chain index integration related?)
        test_manager
            .generate_blocks_and_wait_for_tip(101, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let chain_height = dbg!(fetch_service_subscriber.chain_height().await.unwrap()).0;

    dbg!(fetch_service_subscriber
        .z_get_treestate(chain_height.to_string())
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_z_get_subtrees_by_index<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    dbg!(fetch_service_subscriber
        .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_raw_transaction<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    let tx = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    dbg!(fetch_service_subscriber
        .get_raw_transaction(tx.first().to_string(), Some(1))
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_address_tx_ids<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let tx = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_taddr.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let chain_height: u32 = {
        let idx = &fetch_service_subscriber.indexer;
        let snapshot = idx.snapshot_nonfinalized_state().await.unwrap();
        u32::from(idx.best_chaintip(&snapshot).await.unwrap().height)
    };
    dbg!(&chain_height);

    let fetch_service_txids = fetch_service_subscriber
        .get_address_tx_ids(GetAddressTxIdsRequest::new(
            vec![recipient_taddr],
            Some(chain_height - 2),
            None,
        ))
        .await
        .unwrap();

    dbg!(&tx);
    dbg!(&fetch_service_txids);
    assert_eq!(tx.first().to_string(), fetch_service_txids[0]);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_address_utxos<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let txid_1 = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_taddr.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    clients.faucet.sync_and_await().await.unwrap();

    let fetch_service_utxos = fetch_service_subscriber
        .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr]))
        .await
        .unwrap();
    let (_, fetch_service_txid, ..) = fetch_service_utxos[0].into_parts();

    dbg!(&txid_1);
    dbg!(&fetch_service_utxos);
    assert_eq!(txid_1.first().to_string(), fetch_service_txid.to_string());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_range_returns_all_pools<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");

    clients.faucet.sync_and_await().await.unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        for _ in 1..4 {
            clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();

            test_manager
                .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
                .await;
            clients.faucet.sync_and_await().await.unwrap();
        }
    } else {
        // zcashd
        test_manager
            .generate_blocks_and_wait_for_tip(14, &fetch_service_subscriber)
            .await;

        clients.faucet.sync_and_await().await.unwrap();
    }

    let recipient_transparent = clients.get_recipient_address("transparent").await;
    let deshielding_txid = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_transparent, 250_000, None)],
    )
    .await
    .unwrap()
    .head;

    let recipient_sapling = clients.get_recipient_address("sapling").await;
    let sapling_txid = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_sapling, 250_000, None)],
    )
    .await
    .unwrap()
    .head;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let orchard_txid = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap()
    .head;

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let start_height: u64 = if matches!(validator, ValidatorKind::Zebrad) {
        100
    } else {
        1
    };
    let end_height: u64 = if matches!(validator, ValidatorKind::Zebrad) {
        106
    } else {
        17
    };

    let fetch_service_get_block_range = fetch_service_subscriber
        .get_block_range(BlockRange {
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
        })
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;

    let compact_block = fetch_service_get_block_range.last().unwrap();

    assert_eq!(compact_block.height, end_height);

    // Transparent tx are now included in compact blocks unless specified so the
    // expected block count should be 4 (3 sent tx + coinbase)
    let expected_transaction_count = 4;

    // the compact block has the right number of transactions
    assert_eq!(compact_block.vtx.len(), expected_transaction_count);

    // transaction order is not guaranteed so it's necessary to look up for them by TXID
    let deshielding_tx = compact_block
        .vtx
        .iter()
        .find(|tx| tx.txid == deshielding_txid.as_ref().to_vec())
        .unwrap();

    dbg!(deshielding_tx);

    assert!(
        !deshielding_tx.vout.is_empty(),
        "transparent data should be present when transaparent pool type is specified in the request."
    );

    // transaction order is not guaranteed so it's necessary to look up for them by TXID
    let sapling_tx = compact_block
        .vtx
        .iter()
        .find(|tx| tx.txid == sapling_txid.as_ref().to_vec())
        .unwrap();

    assert!(
        !sapling_tx.outputs.is_empty(),
        "sapling data should be present when all pool types are specified in the request."
    );

    let orchard_tx = compact_block
        .vtx
        .iter()
        .find(|tx| tx.txid == orchard_txid.as_ref().to_vec())
        .unwrap();

    assert!(
        !orchard_tx.actions.is_empty(),
        "orchard data should be present when all pool types are specified in the request."
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_range_no_pools_returns_sapling_orchard<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");

    clients.faucet.sync_and_await().await.unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        for _ in 1..4 {
            clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();

            test_manager
                .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
                .await;
            clients.faucet.sync_and_await().await.unwrap();
        }
    } else {
        // zcashd
        test_manager
            .generate_blocks_and_wait_for_tip(14, &fetch_service_subscriber)
            .await;

        clients.faucet.sync_and_await().await.unwrap();
    }

    let recipient_transparent = clients.get_recipient_address("transparent").await;
    let deshielding_txid = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_transparent, 250_000, None)],
    )
    .await
    .unwrap()
    .head;

    let recipient_sapling = clients.get_recipient_address("sapling").await;
    let sapling_txid = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_sapling, 250_000, None)],
    )
    .await
    .unwrap()
    .head;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let orchard_txid = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap()
    .head;

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let start_height: u64 = if matches!(validator, ValidatorKind::Zebrad) {
        100
    } else {
        10
    };
    let end_height: u64 = if matches!(validator, ValidatorKind::Zebrad) {
        106
    } else {
        17
    };

    let fetch_service_get_block_range = fetch_service_subscriber
        .get_block_range(BlockRange {
            start: Some(BlockId {
                height: start_height,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: end_height,
                hash: vec![],
            }),
            pool_types: vec![],
        })
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;

    let compact_block = fetch_service_get_block_range.last().unwrap();

    assert_eq!(compact_block.height, end_height);

    let expected_tx_count = if matches!(validator, ValidatorKind::Zebrad) {
        3
    } else {
        4 // zcashd shields coinbase and tx count will be one more than zebra's
    };
    // the compact block has 3 transactions
    assert_eq!(compact_block.vtx.len(), expected_tx_count);

    // transaction order is not guaranteed so it's necessary to look up for them by TXID
    let deshielding_tx = compact_block
        .vtx
        .iter()
        .find(|tx| tx.txid == deshielding_txid.as_ref().to_vec())
        .unwrap();

    assert!(
        deshielding_tx.vout.is_empty(),
        "transparent data should not be present when transaparent pool type is specified in the request."
    );

    // transaction order is not guaranteed so it's necessary to look up for them by TXID
    let sapling_tx = compact_block
        .vtx
        .iter()
        .find(|tx| tx.txid == sapling_txid.as_ref().to_vec())
        .unwrap();

    assert!(
        !sapling_tx.outputs.is_empty(),
        "sapling data should be present when default pool types are specified in the request."
    );

    let orchard_tx = compact_block
        .vtx
        .iter()
        .find(|tx| tx.txid == orchard_txid.as_ref().to_vec())
        .unwrap();

    assert!(
        !orchard_tx.actions.is_empty(),
        "orchard data should be present when default pool types are specified in the request."
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_transaction_mined<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    let tx = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let tx_filter = TxFilter {
        block: None,
        index: 0,
        hash: tx.first().as_ref().to_vec(),
    };

    let fetch_service_get_transaction = dbg!(fetch_service_subscriber
        .get_transaction(tx_filter.clone())
        .await
        .unwrap());

    dbg!(fetch_service_get_transaction);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_transaction_mempool<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    let tx = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();

    let tx_filter = TxFilter {
        block: None,
        index: 0,
        hash: tx.first().as_ref().to_vec(),
    };

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let fetch_service_get_transaction = dbg!(fetch_service_subscriber
        .get_transaction(tx_filter.clone())
        .await
        .unwrap());

    dbg!(fetch_service_get_transaction);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_taddress_txids<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let tx = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_taddr, 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let chain_height: u32 = {
        let idx = &fetch_service_subscriber.indexer;
        let snapshot = idx.snapshot_nonfinalized_state().await.unwrap();
        u32::from(idx.best_chaintip(&snapshot).await.unwrap().height)
    };
    dbg!(&chain_height);

    let block_filter = TransparentAddressBlockFilter {
        address: recipient_taddr,
        range: Some(BlockRange {
            start: Some(BlockId {
                height: (chain_height - 2) as u64,
                hash: Vec::new(),
            }),
            end: Some(BlockId {
                height: chain_height as u64,
                hash: Vec::new(),
            }),
            pool_types: vec![
                PoolType::Transparent as i32,
                PoolType::Sapling as i32,
                PoolType::Orchard as i32,
            ],
        }),
    };

    let fetch_service_stream = fetch_service_subscriber
        .get_taddress_txids(block_filter.clone())
        .await
        .unwrap();
    let fetch_service_tx: Vec<_> = fetch_service_stream.collect().await;

    let fetch_tx: Vec<_> = fetch_service_tx
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    dbg!(tx);
    dbg!(&fetch_tx);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_taddress_balance<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_taddr, 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    clients.recipient.sync_and_await().await.unwrap();
    let balance = clients
        .recipient
        .account_balance(zip32::AccountId::ZERO)
        .await
        .unwrap();

    let address_list = AddressList {
        addresses: vec![recipient_taddr],
    };

    let fetch_service_balance = fetch_service_subscriber
        .get_taddress_balance(address_list.clone())
        .await
        .unwrap();

    dbg!(&fetch_service_balance);
    assert_eq!(
        fetch_service_balance.value_zat as u64,
        balance.confirmed_transparent_balance.unwrap().into_u64()
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_mempool_tx<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    let tx_1 = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_taddr, 250_000, None)],
    )
    .await
    .unwrap();
    let tx_2 = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let exclude_list_empty = GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: Vec::new(),
    };

    let fetch_service_stream = fetch_service_subscriber
        .get_mempool_tx(exclude_list_empty.clone())
        .await
        .unwrap();
    let fetch_service_mempool_tx: Vec<_> = fetch_service_stream.collect().await;

    let fetch_mempool_tx: Vec<_> = fetch_service_mempool_tx
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    let mut sorted_fetch_mempool_tx = fetch_mempool_tx.clone();
    sorted_fetch_mempool_tx.sort_by_key(|tx| tx.txid.clone());

    // Transaction IDs from quick_send are already in internal byte order,
    // which matches what the mempool returns, so no reversal needed
    let tx1_bytes = *tx_1.first().as_ref();
    let tx2_bytes = *tx_2.first().as_ref();

    let mut sorted_txids = [tx1_bytes, tx2_bytes];
    sorted_txids.sort_by_key(|hash| *hash);

    assert_eq!(sorted_fetch_mempool_tx[0].txid, sorted_txids[0]);
    assert_eq!(sorted_fetch_mempool_tx[1].txid, sorted_txids[1]);
    assert_eq!(sorted_fetch_mempool_tx.len(), 2);

    let exclude_list = GetMempoolTxRequest {
        exclude_txid_suffixes: vec![sorted_txids[0][8..].to_vec()],
        pool_types: vec![],
    };

    let exclude_fetch_service_stream = fetch_service_subscriber
        .get_mempool_tx(exclude_list.clone())
        .await
        .unwrap();
    let exclude_fetch_service_mempool_tx: Vec<_> = exclude_fetch_service_stream.collect().await;

    let exclude_fetch_mempool_tx: Vec<_> = exclude_fetch_service_mempool_tx
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    let mut sorted_exclude_fetch_mempool_tx = exclude_fetch_mempool_tx.clone();
    sorted_exclude_fetch_mempool_tx.sort_by_key(|tx| tx.txid.clone());

    assert_eq!(sorted_exclude_fetch_mempool_tx[0].txid, sorted_txids[1]);
    assert_eq!(sorted_exclude_fetch_mempool_tx.len(), 1);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_mempool_stream<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let fetch_service_subscriber_2 = fetch_service_subscriber.clone();
    let fetch_service_handle = tokio::spawn(async move {
        let fetch_service_stream = fetch_service_subscriber_2
            .get_mempool_stream()
            .await
            .unwrap();
        let fetch_service_mempool_tx: Vec<_> = fetch_service_stream.collect().await;
        fetch_service_mempool_tx
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>()
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_taddr, 250_000, None)],
    )
    .await
    .unwrap();
    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_ua, 250_000, None)],
    )
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let fetch_mempool_tx = fetch_service_handle.await.unwrap();

    let mut sorted_fetch_mempool_tx = fetch_mempool_tx.clone();
    sorted_fetch_mempool_tx.sort_by_key(|tx| tx.data.clone());

    dbg!(sorted_fetch_mempool_tx);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_taddress_utxos<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let tx = zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_taddr, 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let utxos_arg = GetAddressUtxosArg {
        addresses: vec![recipient_taddr],
        start_height: 0,
        max_entries: 0,
    };

    let fetch_service_get_taddress_utxos = fetch_service_subscriber
        .get_address_utxos(utxos_arg.clone())
        .await
        .unwrap();

    dbg!(tx);
    dbg!(&fetch_service_get_taddress_utxos);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_taddress_utxos_stream<V: ValidatorExt>(validator: &ValidatorKind) {
    let mut test_manager =
        TestManager::<V, FetchService>::launch(validator, None, None, None, true, false, true)
            .await
            .unwrap();

    let fetch_service_subscriber = test_manager.service_subscriber.take().unwrap();

    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    zaino_testutils::from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_taddr, 250_000, None)],
    )
    .await
    .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    let utxos_arg = GetAddressUtxosArg {
        addresses: vec![recipient_taddr],
        start_height: 0,
        max_entries: 0,
    };

    let fetch_service_stream = fetch_service_subscriber
        .get_address_utxos_stream(utxos_arg.clone())
        .await
        .unwrap();
    let fetch_service_utxos: Vec<_> = fetch_service_stream.collect().await;

    let fetch_utxos: Vec<_> = fetch_service_utxos
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    dbg!(fetch_utxos);

    test_manager.close().await;
}

mod zcashd {

    use super::*;
    use zcash_local_net::validator::zcashd::Zcashd;

    mod get {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn address_balance() {
            fetch_service_get_address_balance::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn raw_mempool() {
            fetch_service_get_raw_mempool::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mempool_info() {
            test_get_mempool_info::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        mod z {

            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn get_treestate() {
                fetch_service_z_get_treestate::<Zcashd>(&ValidatorKind::Zcashd).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn subtrees_by_index() {
                fetch_service_z_get_subtrees_by_index::<Zcashd>(&ValidatorKind::Zcashd).await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn raw_transaction() {
            fetch_service_get_raw_transaction::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn address_tx_ids() {
            fetch_service_get_address_tx_ids::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn address_utxos() {
            fetch_service_get_address_utxos::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_range_no_pool_type_returns_sapling_orchard() {
            fetch_service_get_block_range_no_pools_returns_sapling_orchard::<Zcashd>(
                &ValidatorKind::Zcashd,
            )
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_range_returns_all_pools_when_requested() {
            fetch_service_get_block_range_returns_all_pools::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn transaction_mined() {
            fetch_service_get_transaction_mined::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn transaction_mempool() {
            fetch_service_get_transaction_mempool::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_txids() {
            fetch_service_get_taddress_txids::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_balance() {
            fetch_service_get_taddress_balance::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mempool_tx() {
            fetch_service_get_mempool_tx::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mempool_stream() {
            fetch_service_get_mempool_stream::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_utxos() {
            fetch_service_get_taddress_utxos::<Zcashd>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_utxos_stream() {
            fetch_service_get_taddress_utxos_stream::<Zcashd>(&ValidatorKind::Zcashd).await;
        }
    }
}

mod zebrad {

    use super::*;
    use zcash_local_net::validator::zebrad::Zebrad;

    mod get {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn address_balance() {
            fetch_service_get_address_balance::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn raw_mempool() {
            fetch_service_get_raw_mempool::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mempool_info() {
            test_get_mempool_info::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        mod z {

            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn treestate() {
                fetch_service_z_get_treestate::<Zebrad>(&ValidatorKind::Zebrad).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn subtrees_by_index() {
                fetch_service_z_get_subtrees_by_index::<Zebrad>(&ValidatorKind::Zebrad).await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn raw_transaction() {
            fetch_service_get_raw_transaction::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn address_tx_ids() {
            fetch_service_get_address_tx_ids::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn address_utxos() {
            fetch_service_get_address_utxos::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_range_returns_all_pools_when_requested() {
            fetch_service_get_block_range_returns_all_pools::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn block_range_no_pool_type_returns_sapling_orchard() {
            fetch_service_get_block_range_no_pools_returns_sapling_orchard::<Zebrad>(
                &ValidatorKind::Zebrad,
            )
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn transaction_mined() {
            fetch_service_get_transaction_mined::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn transaction_mempool() {
            fetch_service_get_transaction_mempool::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_txids() {
            fetch_service_get_taddress_txids::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_balance() {
            fetch_service_get_taddress_balance::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mempool_tx() {
            fetch_service_get_mempool_tx::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn mempool_stream() {
            fetch_service_get_mempool_stream::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_utxos() {
            fetch_service_get_taddress_utxos::<Zebrad>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub(crate) async fn taddress_utxos_stream() {
            fetch_service_get_taddress_utxos_stream::<Zebrad>(&ValidatorKind::Zebrad).await;
        }
    }
}
