//! These tests compare the output of `FetchService` with the output of `JsonRpcConnector`.

use futures::StreamExt as _;
use hex::ToHex as _;
use nonempty::NonEmpty;
use zaino_proto::proto::compact_formats::CompactBlock;
use wallet_tests::from_inputs;
use zaino_proto::proto::service::{
    AddressList, BlockId, BlockRange, GetAddressUtxosArg, GetMempoolTxRequest, PoolType,
    TransparentAddressBlockFilter, TxFilter,
};
use zaino_state::ChainIndex;
use zaino_state::FetchServiceSubscriber;
#[allow(deprecated)]
use zaino_state::{FetchService, LightWalletIndexer, ZcashIndexer};
use zaino_testutils::{TestManager, ValidatorExt, ValidatorKind};
use zcash_primitives::transaction::TxId;
use zebra_chain::subtree::NoteCommitmentSubtreeIndex;
use zebra_rpc::methods::{GetAddressBalanceRequest, GetAddressTxIdsRequest};
use zip32::AccountId;

#[allow(deprecated)]
async fn create_test_manager_and_fetch_service<V: ValidatorExt>(
    validator: &ValidatorKind,
    chain_cache: Option<std::path::PathBuf>,
) -> (
    TestManager<V, FetchService>,
    FetchServiceSubscriber,
    wallet_tests::Clients,
) {
    let (test_manager, fetch_service_subscriber) =
        zaino_testutils::launch_with_fetch_subscriber(validator, chain_cache).await;
    let clients = wallet_tests::build_clients(
        test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
        wallet_tests::default_heights(validator),
    );
    (test_manager, fetch_service_subscriber, clients)
}

/// Sync the faucet; on zebrad, run `shield_rounds` rounds of "mature 100
/// coinbase blocks, sync, shield" (zebrad can't mine directly to orchard in
/// this setup), then mine one more block and sync so the shielded funds are
/// spendable. `shield_rounds` of 1 funds a single send; 2 funds two.
#[allow(deprecated)]
async fn fund_faucet<V: ValidatorExt>(
    test_manager: &TestManager<V, FetchService>,
    clients: &mut wallet_tests::Clients,
    validator: &ValidatorKind,
    fetch_service_subscriber: &FetchServiceSubscriber,
    shield_rounds: u32,
) {
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        for _ in 0..shield_rounds {
            test_manager
                .generate_blocks_and_wait_for_tip(100, fetch_service_subscriber)
                .await;
            clients.faucet.sync_and_await().await.unwrap();
            clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        }
        test_manager
            .generate_blocks_and_wait_for_tip(1, fetch_service_subscriber)
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    }
}

/// Send `amount` from the faucet to `address`, mine a block, and return the
/// transaction id(s). The standard "send one, mine one" step the mined tests
/// share; callers that don't need the txid just discard the return.
#[allow(deprecated)]
async fn send_and_mine<V: ValidatorExt>(
    test_manager: &TestManager<V, FetchService>,
    clients: &mut wallet_tests::Clients,
    fetch_service_subscriber: &FetchServiceSubscriber,
    address: &str,
    amount: u64,
) -> NonEmpty<TxId> {
    let tx = from_inputs::quick_send(&mut clients.faucet, vec![(address, amount, None)])
        .await
        .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, fetch_service_subscriber)
        .await;
    tx
}

/// Launch, fund the faucet, and send 250_000 to the recipient's transparent,
/// sapling, and unified addresses (one tx each), then mine. Returns the
/// manager, subscriber, clients, and the three txids — the shared setup for the
/// get_block_range tests, which differ only in the pools requested and asserted.
#[allow(deprecated)]
async fn block_range_fixture<V: ValidatorExt>(
    validator: &ValidatorKind,
) -> (
    TestManager<V, FetchService>,
    FetchServiceSubscriber,
    wallet_tests::Clients,
    TxId,
    TxId,
    TxId,
) {
    let (test_manager, fetch_service_subscriber, mut clients) =
        create_test_manager_and_fetch_service::<V>(validator, None).await;

    clients.faucet.sync_and_await().await.unwrap();

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
    let deshielding_txid = from_inputs::quick_send(
        &mut clients.faucet,
        vec![(&recipient_transparent, 250_000, None)],
    )
    .await
    .unwrap()
    .head;

    let recipient_sapling = clients.get_recipient_address("sapling").await;
    let sapling_txid =
        from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_sapling, 250_000, None)])
            .await
            .unwrap()
            .head;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let orchard_txid =
        from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_ua, 250_000, None)])
            .await
            .unwrap()
            .head;

    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;

    (
        test_manager,
        fetch_service_subscriber,
        clients,
        deshielding_txid,
        sapling_txid,
        orchard_txid,
    )
}

/// Whether the compact tx with `txid` carries no data for `pool` (transparent
/// `vout` / sapling `outputs` / orchard `actions`).
fn pool_tx_field_empty(block: &CompactBlock, txid: &TxId, pool: wallet_tests::Pool) -> bool {
    let tx = block
        .vtx
        .iter()
        .find(|tx| tx.txid == txid.as_ref().to_vec())
        .expect("sent tx present in compact block");
    match pool {
        wallet_tests::Pool::Transparent => tx.vout.is_empty(),
        wallet_tests::Pool::Sapling => tx.outputs.is_empty(),
        wallet_tests::Pool::Orchard => tx.actions.is_empty(),
    }
}

/// Assert the compact tx with `txid` carries `pool` data.
fn assert_pool_present(block: &CompactBlock, txid: &TxId, pool: wallet_tests::Pool) {
    assert!(
        !pool_tx_field_empty(block, txid, pool),
        "{pool:?} data should be present in the compact block"
    );
}

/// Assert the compact tx with `txid` carries no `pool` data.
fn assert_pool_absent(block: &CompactBlock, txid: &TxId, pool: wallet_tests::Pool) {
    assert!(
        pool_tx_field_empty(block, txid, pool),
        "{pool:?} data should be absent from the compact block"
    );
}

/// Launch, fund the faucet, then broadcast (without mining) one transparent and
/// one orchard send into the mempool, waiting briefly for the broadcaster and
/// subscribers to observe them. Returns the manager, subscriber, clients, and
/// the two txids (transparent first, then unified) — the shared setup for the
/// mempool query tests.
#[allow(deprecated)]
async fn fund_and_fill_mempool<V: ValidatorExt>(
    validator: &ValidatorKind,
) -> (
    TestManager<V, FetchService>,
    FetchServiceSubscriber,
    wallet_tests::Clients,
    NonEmpty<TxId>,
    NonEmpty<TxId>,
) {
    let (test_manager, fetch_service_subscriber, mut clients) =
        create_test_manager_and_fetch_service::<V>(validator, None).await;
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;
    fund_faucet(
        &test_manager,
        &mut clients,
        validator,
        &fetch_service_subscriber,
        2,
    )
    .await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    let recipient_ua = clients.get_recipient_address("unified").await;
    let transparent_txid =
        from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_taddr, 250_000, None)])
            .await
            .unwrap();
    let unified_txid =
        from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_ua, 250_000, None)])
            .await
            .unwrap();

    // Allow the broadcaster and subscribers to observe the new transactions.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    (
        test_manager,
        fetch_service_subscriber,
        clients,
        transparent_txid,
        unified_txid,
    )
}

/// Launch, fund the faucet (one shield round), and send 250_000 to the
/// recipient's `pool` address, mining it in. Returns the manager, subscriber,
/// clients, and the send txid — the shared setup for the mined query tests.
#[allow(deprecated)]
async fn fund_and_send<V: ValidatorExt>(
    validator: &ValidatorKind,
    pool: wallet_tests::Pool,
) -> (
    TestManager<V, FetchService>,
    FetchServiceSubscriber,
    wallet_tests::Clients,
    NonEmpty<TxId>,
) {
    let (test_manager, fetch_service_subscriber, mut clients) =
        create_test_manager_and_fetch_service::<V>(validator, None).await;
    fund_faucet(
        &test_manager,
        &mut clients,
        validator,
        &fetch_service_subscriber,
        1,
    )
    .await;
    let recipient = clients.get_recipient_address(pool.address_kind()).await;
    let tx = send_and_mine(
        &test_manager,
        &mut clients,
        &fetch_service_subscriber,
        &recipient,
        250_000,
    )
    .await;
    (test_manager, fetch_service_subscriber, clients, tx)
}

#[allow(deprecated)]
async fn fetch_service_get_address_balance<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber, mut clients, _tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Transparent).await;
    let recipient_address = clients.get_recipient_address("transparent").await;

    clients.recipient.sync_and_await().await.unwrap();
    let recipient_balance = clients.recipient_balance().await;

    let fetch_service_balance = fetch_service_subscriber
        .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_address]))
        .await
        .unwrap();

    dbg!(recipient_balance.clone());
    dbg!(fetch_service_balance);

    assert_eq!(
        wallet_tests::Pool::Transparent.received_balance(&recipient_balance),
        250_000,
    );
    assert_eq!(
        wallet_tests::Pool::Transparent.received_balance(&recipient_balance),
        fetch_service_balance.balance(),
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_raw_mempool<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber, _clients, _transparent_txid, _unified_txid) =
        fund_and_fill_mempool::<V>(validator).await;

    let json_service = test_manager.full_node_jsonrpc_connector().await;

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
    let (mut test_manager, fetch_service_subscriber, _clients, _transparent_txid, _unified_txid) =
        fund_and_fill_mempool::<V>(validator).await;

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
    let (mut test_manager, fetch_service_subscriber, _clients, _tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Orchard).await;

    let chain_height = dbg!(fetch_service_subscriber.chain_height().await.unwrap()).0;

    dbg!(fetch_service_subscriber
        .z_get_treestate(chain_height.to_string())
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_z_get_subtrees_by_index<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber, _clients, _tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Orchard).await;

    dbg!(fetch_service_subscriber
        .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_raw_transaction<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber, _clients, tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Orchard).await;

    dbg!(fetch_service_subscriber
        .get_raw_transaction(tx.first().to_string(), Some(1))
        .await
        .unwrap());

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_address_tx_ids<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber, clients, tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

    let chain_height = fetch_service_subscriber.chain_height().await.unwrap().0;
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
    let (mut test_manager, fetch_service_subscriber, mut clients, txid_1) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

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
    let (
        mut test_manager,
        fetch_service_subscriber,
        _clients,
        deshielding_txid,
        sapling_txid,
        orchard_txid,
    ) = block_range_fixture::<V>(validator).await;

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

    // Transparent tx are included in compact blocks unless pools are specified,
    // so expect 4 (3 sent tx + coinbase).
    assert_eq!(compact_block.vtx.len(), 4);

    assert_pool_present(compact_block, &deshielding_txid, wallet_tests::Pool::Transparent);
    assert_pool_present(compact_block, &sapling_txid, wallet_tests::Pool::Sapling);
    assert_pool_present(compact_block, &orchard_txid, wallet_tests::Pool::Orchard);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_block_range_no_pools_returns_sapling_orchard<V: ValidatorExt>(
    validator: &ValidatorKind,
) {
    let (
        mut test_manager,
        fetch_service_subscriber,
        _clients,
        deshielding_txid,
        sapling_txid,
        orchard_txid,
    ) = block_range_fixture::<V>(validator).await;

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
    assert_eq!(compact_block.vtx.len(), expected_tx_count);

    // No pools requested: transparent data is omitted, sapling/orchard default in.
    assert_pool_absent(compact_block, &deshielding_txid, wallet_tests::Pool::Transparent);
    assert_pool_present(compact_block, &sapling_txid, wallet_tests::Pool::Sapling);
    assert_pool_present(compact_block, &orchard_txid, wallet_tests::Pool::Orchard);

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_transaction_mined<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber, _clients, tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Orchard).await;

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
    let (mut test_manager, fetch_service_subscriber, mut clients) =
        create_test_manager_and_fetch_service::<V>(validator, None).await;
    fund_faucet(
        &test_manager,
        &mut clients,
        validator,
        &fetch_service_subscriber,
        1,
    )
    .await;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let tx = from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_ua, 250_000, None)])
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
    let (mut test_manager, fetch_service_subscriber, clients, tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

    let chain_height = fetch_service_subscriber.chain_height().await.unwrap().0;
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
    let (mut test_manager, fetch_service_subscriber, mut clients, _tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.recipient.sync_and_await().await.unwrap();
    let balance = clients.recipient_balance().await;

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
        wallet_tests::Pool::Transparent.received_balance(&balance)
    );

    test_manager.close().await;
}

#[allow(deprecated)]
async fn fetch_service_get_mempool_tx<V: ValidatorExt>(validator: &ValidatorKind) {
    let (mut test_manager, fetch_service_subscriber, _clients, tx_1, tx_2) =
        fund_and_fill_mempool::<V>(validator).await;

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
    let (mut test_manager, fetch_service_subscriber, mut clients) =
        create_test_manager_and_fetch_service::<V>(validator, None).await;
    test_manager
        .generate_blocks_and_wait_for_tip(1, &fetch_service_subscriber)
        .await;
    fund_faucet(
        &test_manager,
        &mut clients,
        validator,
        &fetch_service_subscriber,
        2,
    )
    .await;

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
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_taddr, 250_000, None)])
        .await
        .unwrap();
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_ua, 250_000, None)])
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
    let (mut test_manager, fetch_service_subscriber, clients, tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

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
    let (mut test_manager, fetch_service_subscriber, clients, _tx) =
        fund_and_send::<V>(validator, wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

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

/// Generates a validator's `mod get` fetch_service test wrappers: one
/// `#[tokio::test]` per `fetch_service_*` helper, turbofished to `$validator`
/// and `$kind`. A macro rather than a fn because each wrapper must be a
/// discoverable `#[tokio::test]` item, which a function cannot emit.
macro_rules! fetch_service_tests {
    ($modname:ident, $validator:ty, $kind:expr) => {
        mod $modname {
            use super::*;

            mod get {
                use super::*;

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn address_balance() {
                    fetch_service_get_address_balance::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn raw_mempool() {
                    fetch_service_get_raw_mempool::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn mempool_info() {
                    test_get_mempool_info::<$validator>(&$kind).await;
                }

                mod z {
                    use super::*;

                    #[tokio::test(flavor = "multi_thread")]
                    pub(crate) async fn treestate() {
                        fetch_service_z_get_treestate::<$validator>(&$kind).await;
                    }

                    #[tokio::test(flavor = "multi_thread")]
                    pub(crate) async fn subtrees_by_index() {
                        fetch_service_z_get_subtrees_by_index::<$validator>(&$kind).await;
                    }
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn raw_transaction() {
                    fetch_service_get_raw_transaction::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn address_tx_ids() {
                    fetch_service_get_address_tx_ids::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn address_utxos() {
                    fetch_service_get_address_utxos::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn block_range_no_pool_type_returns_sapling_orchard() {
                    fetch_service_get_block_range_no_pools_returns_sapling_orchard::<$validator>(
                        &$kind,
                    )
                    .await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn block_range_returns_all_pools_when_requested() {
                    fetch_service_get_block_range_returns_all_pools::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn transaction_mined() {
                    fetch_service_get_transaction_mined::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn transaction_mempool() {
                    fetch_service_get_transaction_mempool::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn taddress_txids() {
                    fetch_service_get_taddress_txids::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn taddress_balance() {
                    fetch_service_get_taddress_balance::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn mempool_tx() {
                    fetch_service_get_mempool_tx::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn mempool_stream() {
                    fetch_service_get_mempool_stream::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn taddress_utxos() {
                    fetch_service_get_taddress_utxos::<$validator>(&$kind).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                pub(crate) async fn taddress_utxos_stream() {
                    fetch_service_get_taddress_utxos_stream::<$validator>(&$kind).await;
                }
            }
        }
    };
}

fetch_service_tests!(
    zcashd,
    zcash_local_net::validator::zcashd::Zcashd,
    ValidatorKind::Zcashd
);
fetch_service_tests!(
    zebrad,
    zcash_local_net::validator::zebrad::Zebrad,
    ValidatorKind::Zebrad
);
