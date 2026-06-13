//! Devtool-backed ports of the wallet-to-validator tests, run against a Zaino
//! launched by `TestManager`. As the zcash-devtool client
//! ([`wallet_tests::devtool`]) reaches capability parity with the zingolib
//! `Clients`, the matching tests in `wallet_to_validator.rs` migrate here and
//! the zingolib versions are eventually retired (zingolabs/infrastructure#269).
//!
//! Covered: sends to all three pools, shielding, mining-reward receipt, and
//! the `get_transaction` / `get_block_range` query tests. Deferred: tests
//! that mine to a transparent miner (the original `send_to_transparent` /
//! `send_to_all` finalization mines), unconfirmed mempool balances
//! (`monitor_unverified_mempool`), and the zcashd matrix (its default
//! activation heights differ from the devtool client's compiled-in regtest
//! heights, which it rejects at construction). Zebrad only for the same
//! height reason.
//!
//! Requires a `zcash-devtool` binary built with `--features regtest_support`
//! in `TEST_BINARIES_DIR`/`PATH`, alongside the usual validator binaries.

use wallet_tests::devtool::DevtoolClients;
use zaino_proto::proto::service::TxFilter;
use zaino_state::{ChainIndex, LightWalletIndexer, ZcashIndexer, ZcashService};
use zaino_testutils::{PollableTip, TestManager, TestService, ValidatorKind};
use zainodlib::error::IndexerError;
use zcash_local_net::validator::zebrad::Zebrad;
use zebra_rpc::methods::{GetAddressBalanceRequest, GetAddressTxIdsRequest};

/// Launch an orchard-mining zebrad + Zaino on the `Service` backend, build
/// devtool faucet/recipient wallets against it, mine `coinbase_blocks` blocks
/// past the launch, and sync the faucet. Each mined block past the launch is
/// an orchard coinbase note for the faucet (height 1 is the
/// sapling-activation coinbase), so `coinbase_blocks` is the number of
/// spendable orchard notes the faucet ends with — one per send a test makes
/// before mining (devtool will not chain unconfirmed change). The shared
/// launch+fund preamble of the ports below.
async fn launch_and_fund_faucet<Service>(
    coinbase_blocks: u32,
) -> (TestManager<Zebrad, Service>, DevtoolClients)
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let test_manager = TestManager::<Zebrad, Service>::launch_mining_to(
        zaino_testutils::SHIELDED_FUNDING_POOL,
        &ValidatorKind::Zebrad,
        None,
        None,
        None,
        true,
        false,
        false,
    )
    .await
    .expect("launch TestManager");

    let mut clients = wallet_tests::devtool::build_clients(
        test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
    )
    .await;

    test_manager
        .generate_blocks_and_wait_for_tip(coinbase_blocks, test_manager.subscriber())
        .await;
    clients.sync_faucet().await;

    (test_manager, clients)
}

/// Port of `wallet_to_validator::check_received_mining_reward` (zebrad): the
/// faucet's synced wallet sees the orchard coinbase note.
async fn receives_mining_reward<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let (mut test_manager, clients) = launch_and_fund_faucet::<Service>(1).await;

    let faucet_balance = dbg!(clients.faucet_balance().await);
    assert!(
        faucet_balance.orchard_spendable > 0,
        "faucet should hold a spendable orchard coinbase note, got {faucet_balance:?}"
    );

    test_manager.close().await;
}

/// Port of the `assert_send_to_pool` family from `wallet_to_validator`
/// (zebrad): send 250_000 from the faucet (spending its orchard coinbase) to
/// the recipient's `pool` address, mine it in, and assert the recipient's
/// synced wallet shows the receipt in that pool. Covers `send_to_orchard`
/// (unified), `send_to_sapling`, and the basic transparent receipt — the
/// per-pool address wiring is the new surface under test. (The original
/// `send_to_transparent` additionally mines across the finalization boundary
/// under transparent mining; that variant is deferred, as it exercises the
/// transparent-coinbase detection path devtool has not yet been run against.)
async fn send_to_pool<Service>(pool: wallet_tests::Pool)
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let (mut test_manager, mut clients) = launch_and_fund_faucet::<Service>(1).await;

    let recipient = clients.get_recipient_address(pool.address_kind()).await;
    let txid = clients.send_from_faucet(&recipient, 250_000).await;
    dbg!(txid);

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.sync_recipient().await;

    assert_eq!(
        pool.spendable_balance(&clients.recipient_balance().await),
        250_000
    );

    test_manager.close().await;
}

/// Port of `wallet_to_validator::shield_for_validator` (zebrad): the faucet
/// sends 250_000 to the recipient's transparent address; the recipient
/// confirms the transparent receipt, shields it to orchard, and confirms the
/// orchard balance net of the ZIP-317 fee (250_000 − 15_000 = 235_000 — the
/// first devtool exercise of `shield`, and the constant flagged as needing
/// re-verification against devtool's note selection).
async fn shield_for_validator<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let (mut test_manager, mut clients) = launch_and_fund_faucet::<Service>(1).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    clients.send_from_faucet(&recipient_taddr, 250_000).await;
    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.sync_recipient().await;

    assert_eq!(
        wallet_tests::Pool::Transparent.spendable_balance(&clients.recipient_balance().await),
        250_000
    );

    clients.shield_recipient().await;
    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.sync_recipient().await;

    assert_eq!(
        wallet_tests::Pool::Orchard.spendable_balance(&clients.recipient_balance().await),
        235_000
    );

    test_manager.close().await;
}

/// Launch, fund the faucet with two orchard notes, and broadcast (without
/// mining) one transparent and one unified send into the mempool. Returns the
/// manager and the two broadcast txids (transparent, then unified). The
/// devtool analogue of `fund_and_fill_mempool`.
async fn fund_and_fill_mempool<Service>() -> (TestManager<Zebrad, Service>, String, String)
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    // Two orchard notes — one per unmined send.
    let (test_manager, mut clients) = launch_and_fund_faucet::<Service>(2).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    let recipient_ua = clients.get_recipient_address("unified").await;
    let transparent_txid = clients.send_from_faucet(&recipient_taddr, 250_000).await;
    let unified_txid = clients.send_from_faucet(&recipient_ua, 250_000).await;

    // Allow the broadcaster and the indexer to observe the unmined transactions.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    (test_manager, transparent_txid, unified_txid)
}

/// Fund the faucet, send 250_000 to the recipient's `pool` address, mine it
/// in, and return the manager, clients (for address lookup), and the broadcast
/// txid hex. The txid is in display (reversed) order — devtool prints it that
/// way, which matches the txid strings zaino's address queries return, so no
/// conversion is needed for those comparisons.
async fn fund_and_send_to<Service>(
    pool: wallet_tests::Pool,
) -> (TestManager<Zebrad, Service>, DevtoolClients, String)
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let (test_manager, mut clients) = launch_and_fund_faucet::<Service>(1).await;

    let recipient = clients.get_recipient_address(pool.address_kind()).await;
    let txid_hex = clients.send_from_faucet(&recipient, 250_000).await;

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;

    (test_manager, clients, txid_hex)
}

/// Port of `fetch_service_get_address_tx_ids` (zebrad): after a transparent
/// send to the recipient, `get_address_tx_ids` over the recipient's taddr
/// returns the send's txid.
async fn get_address_tx_ids<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip + LightWalletIndexer + ChainIndex,
{
    let (mut test_manager, clients, txid_hex) =
        fund_and_send_to::<Service>(wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

    let chain_height = test_manager.subscriber().chain_height().await.unwrap().0;
    let txids = test_manager
        .subscriber()
        .get_address_tx_ids(GetAddressTxIdsRequest::new(
            vec![recipient_taddr],
            Some(chain_height - 2),
            None,
        ))
        .await
        .unwrap();

    dbg!(&txid_hex, &txids);
    assert_eq!(txid_hex.trim(), txids[0]);

    test_manager.close().await;
}

/// Port of `fetch_service_get_address_utxos` (zebrad): after a transparent
/// send to the recipient, `z_get_address_utxos` over the recipient's taddr
/// returns a utxo whose txid is the send's.
async fn get_address_utxos<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip + LightWalletIndexer,
{
    let (mut test_manager, clients, txid_hex) =
        fund_and_send_to::<Service>(wallet_tests::Pool::Transparent).await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;

    let utxos = test_manager
        .subscriber()
        .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr]))
        .await
        .unwrap();
    let (_, utxo_txid, ..) = utxos[0].into_parts();

    dbg!(&txid_hex, &utxo_txid);
    assert_eq!(txid_hex.trim(), utxo_txid.to_string());

    test_manager.close().await;
}

/// Port of `fetch_service_get_raw_mempool` (zebrad): with two transactions
/// broadcast but unmined, the indexer's `get_raw_mempool` matches the
/// validator's own JSON-RPC `getrawmempool`.
async fn get_raw_mempool<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip + LightWalletIndexer,
{
    let (mut test_manager, _transparent_txid, _unified_txid) =
        fund_and_fill_mempool::<Service>().await;

    let json_service = test_manager.full_node_jsonrpc_connector().await;

    let mut zaino_mempool = test_manager.subscriber().get_raw_mempool().await.unwrap();
    let mut validator_mempool = json_service.get_raw_mempool().await.unwrap().transactions;

    dbg!(&zaino_mempool);
    dbg!(&validator_mempool);
    zaino_mempool.sort();
    validator_mempool.sort();
    assert_eq!(validator_mempool, zaino_mempool);

    test_manager.close().await;
}

/// Port of `fetch_service_get_mempool_tx` (zebrad): the `get_mempool_tx`
/// stream returns the two unmined transactions keyed by txid (internal byte
/// order), and the exclude-by-txid-suffix filter drops the named one.
async fn get_mempool_tx<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip + LightWalletIndexer,
{
    use futures::StreamExt as _;
    use zaino_proto::proto::service::GetMempoolTxRequest;

    let (mut test_manager, transparent_txid, unified_txid) = fund_and_fill_mempool::<Service>().await;

    let to_bytes = |hex: &str| -> [u8; 32] {
        wallet_tests::devtool::txid_internal_bytes(hex)
            .try_into()
            .expect("txid is 32 bytes")
    };
    let mut sorted_txids = [to_bytes(&transparent_txid), to_bytes(&unified_txid)];
    sorted_txids.sort();

    let subscriber = test_manager.subscriber().clone();
    let collect = |req: GetMempoolTxRequest| {
        let subscriber = subscriber.clone();
        async move {
            let stream_items: Vec<_> = subscriber.get_mempool_tx(req).await.unwrap().collect().await;
            let mut txs: Vec<_> = stream_items.into_iter().filter_map(|r| r.ok()).collect();
            txs.sort_by_key(|tx| tx.txid.clone());
            txs
        }
    };

    // Both transactions present, no exclusions.
    let all = collect(GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: Vec::new(),
    })
    .await;
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].txid, sorted_txids[0]);
    assert_eq!(all[1].txid, sorted_txids[1]);

    // Excluding the first by its txid suffix leaves only the second.
    let remaining = collect(GetMempoolTxRequest {
        exclude_txid_suffixes: vec![sorted_txids[0][8..].to_vec()],
        pool_types: Vec::new(),
    })
    .await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].txid, sorted_txids[1]);

    test_manager.close().await;
}

/// Fund the faucet, send 250_000 to the recipient's unified address, and mine
/// it in. Returns the manager and the broadcast txid in zaino's internal byte
/// order. The devtool analogue of `fund_and_send(Pool::Orchard)`; the wallet
/// clients are dropped once the transaction is mined (the queries below hit
/// zaino, not the wallet).
async fn fund_and_send_orchard<Service>() -> (TestManager<Zebrad, Service>, Vec<u8>)
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let (test_manager, mut clients) = launch_and_fund_faucet::<Service>(1).await;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let txid_hex = clients.send_from_faucet(&recipient_ua, 250_000).await;

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;

    (test_manager, wallet_tests::devtool::txid_internal_bytes(&txid_hex))
}

/// Port of `fetch_service_get_transaction_mined` (zebrad): the indexer serves
/// `get_transaction` for the mined orchard send, keyed by its txid. Also
/// confirms `txid_internal_bytes` yields the order the indexer matches on.
async fn get_transaction_mined<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip + LightWalletIndexer,
{
    let (mut test_manager, txid_bytes) = fund_and_send_orchard::<Service>().await;

    let tx_filter = TxFilter {
        block: None,
        index: 0,
        hash: txid_bytes,
    };
    let raw_transaction = test_manager
        .subscriber()
        .get_transaction(tx_filter)
        .await
        .unwrap();
    dbg!(raw_transaction);

    test_manager.close().await;
}

/// Port of `fetch_service_get_transaction_mempool` (zebrad): the indexer
/// serves `get_transaction` for an orchard send that is broadcast but not
/// mined, keyed by its txid — i.e. from the mempool.
async fn get_transaction_mempool<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip + LightWalletIndexer,
{
    let (mut test_manager, mut clients) = launch_and_fund_faucet::<Service>(1).await;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let txid_hex = clients.send_from_faucet(&recipient_ua, 250_000).await;
    let tx_filter = TxFilter {
        block: None,
        index: 0,
        hash: wallet_tests::devtool::txid_internal_bytes(&txid_hex),
    };

    // Let the broadcaster and the indexer observe the unmined transaction.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let raw_transaction = test_manager
        .subscriber()
        .get_transaction(tx_filter)
        .await
        .unwrap();
    dbg!(raw_transaction);

    test_manager.close().await;
}

/// Port of `state_service_get_block_range_returns_default_pools` (zebrad):
/// fund the faucet, send 250_000 to the recipient's unified address, mine it
/// in, then assert that `get_block_range` with no pools requested returns the
/// same shielded compact blocks as requesting sapling+orchard, that the
/// fetch- and state-service indexers agree, and that the tip block holds the
/// shielded coinbase and the send with no transparent data.
async fn block_range_returns_default_pools() {
    let mut svc = zaino_testutils::launch_state_and_fetch_services_mining_to::<Zebrad>(
        zaino_testutils::SHIELDED_FUNDING_POOL,
        &ValidatorKind::Zebrad,
        None,
        true,
        Some(zebra_chain::parameters::NetworkKind::Regtest),
    )
    .await;

    let mut clients = wallet_tests::devtool::build_clients(
        svc.test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
    )
    .await;

    // fund_and_send(Orchard): one orchard coinbase note, then send it to the
    // recipient's unified address and mine the send in.
    svc.generate_blocks_and_wait_for_tips(1).await;
    clients.sync_faucet().await;
    let recipient_ua = clients.get_recipient_address("unified").await;
    clients.send_from_faucet(&recipient_ua, 250_000).await;
    svc.generate_blocks_and_wait_for_tips(1).await;

    let start_height: u64 = 1;
    let end_height: u64 = svc.fetch_subscriber.tip_height().await;

    let fetch_default = zaino_testutils::collect_block_range(
        &svc.fetch_subscriber,
        start_height,
        end_height,
        vec![],
    )
    .await;
    let fetch_shielded = zaino_testutils::collect_block_range(
        &svc.fetch_subscriber,
        start_height,
        end_height,
        zaino_testutils::shielded_pools_i32(),
    )
    .await;
    assert_eq!(fetch_default, fetch_shielded);

    let state_shielded = zaino_testutils::collect_block_range(
        &svc.state_subscriber,
        start_height,
        end_height,
        zaino_testutils::shielded_pools_i32(),
    )
    .await;
    let state_default = zaino_testutils::collect_block_range(
        &svc.state_subscriber,
        start_height,
        end_height,
        vec![],
    )
    .await;
    assert_eq!(state_default, state_shielded);

    assert_eq!(fetch_default, state_default);

    let compact_block = state_default.last().unwrap();
    assert_eq!(compact_block.height, end_height);
    // The tip block holds the shielded coinbase (the miner address is a
    // shielded pool) and the send.
    assert_eq!(compact_block.vtx.len(), 2);
    assert_eq!(compact_block.vtx.last().unwrap().index, 1);
    for tx in &compact_block.vtx {
        assert_eq!(
            tx.vin,
            vec![],
            "transparent data should not be present when no pool types are specified in the request."
        );
        assert_eq!(
            tx.vout,
            vec![],
            "transparent data should not be present when no pool types are specified in the request."
        );
    }

    svc.test_manager.close().await;
}

/// Port of `state_service_get_block_range_returns_all_pools` (zebrad): fund
/// the faucet with three orchard coinbase notes, send 250_000 to the
/// recipient's transparent, sapling, and unified addresses (one tx each),
/// mine them into one block, then assert `get_block_range` with all pools
/// requested agrees between the fetch and state indexers and that the tip
/// block carries the coinbase plus all three sends with their pool data.
async fn block_range_returns_all_pools() {
    let mut svc = zaino_testutils::launch_state_and_fetch_services_mining_to::<Zebrad>(
        zaino_testutils::SHIELDED_FUNDING_POOL,
        &ValidatorKind::Zebrad,
        None,
        true,
        Some(zebra_chain::parameters::NetworkKind::Regtest),
    )
    .await;

    let mut clients = wallet_tests::devtool::build_clients(
        svc.test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
    )
    .await;

    // Three orchard coinbase notes (one per send below — devtool will not
    // chain unconfirmed change), then one send to each pool's recipient
    // address, mined into a single block.
    svc.generate_blocks_and_wait_for_tips(3).await;
    clients.sync_faucet().await;

    let recipient_t = clients.get_recipient_address("transparent").await;
    let recipient_s = clients.get_recipient_address("sapling").await;
    let recipient_u = clients.get_recipient_address("unified").await;
    let deshielding_txid = clients.send_from_faucet(&recipient_t, 250_000).await;
    let sapling_txid = clients.send_from_faucet(&recipient_s, 250_000).await;
    let orchard_txid = clients.send_from_faucet(&recipient_u, 250_000).await;
    svc.generate_blocks_and_wait_for_tips(1).await;

    let start_height: u64 = 1;
    let end_height: u64 = svc.fetch_subscriber.tip_height().await;
    let all_pools = zaino_testutils::all_pools_i32();

    let fetch_range = zaino_testutils::collect_block_range(
        &svc.fetch_subscriber,
        start_height,
        end_height,
        all_pools.clone(),
    )
    .await;
    let state_range = zaino_testutils::collect_block_range(
        &svc.state_subscriber,
        start_height,
        end_height,
        all_pools,
    )
    .await;
    assert_eq!(fetch_range, state_range);

    let compact_block = state_range.last().unwrap();
    assert_eq!(compact_block.height, end_height);
    // coinbase + the three sends
    assert_eq!(compact_block.vtx.len(), 4);

    wallet_tests::assert_pool_present(
        compact_block,
        &wallet_tests::devtool::txid_from_devtool(&deshielding_txid),
        wallet_tests::Pool::Transparent,
    );
    wallet_tests::assert_pool_present(
        compact_block,
        &wallet_tests::devtool::txid_from_devtool(&sapling_txid),
        wallet_tests::Pool::Sapling,
    );
    wallet_tests::assert_pool_present(
        compact_block,
        &wallet_tests::devtool::txid_from_devtool(&orchard_txid),
        wallet_tests::Pool::Orchard,
    );

    svc.test_manager.close().await;
}

mod zebrad {
    // FetchService is a deprecated re-export; the deprecation fires at the
    // turbofish use sites below, so the allow covers the whole module.
    #[allow(deprecated)]
    mod fetch_service {
        use zaino_state::FetchService;

        #[tokio::test(flavor = "multi_thread")]
        async fn receives_mining_reward() {
            crate::receives_mining_reward::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn send_to_orchard() {
            crate::send_to_pool::<FetchService>(wallet_tests::Pool::Orchard).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn send_to_sapling() {
            crate::send_to_pool::<FetchService>(wallet_tests::Pool::Sapling).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn send_to_transparent() {
            crate::send_to_pool::<FetchService>(wallet_tests::Pool::Transparent).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shield_for_validator() {
            crate::shield_for_validator::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_transaction_mined() {
            crate::get_transaction_mined::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_raw_mempool() {
            crate::get_raw_mempool::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_mempool_tx() {
            crate::get_mempool_tx::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_address_tx_ids() {
            crate::get_address_tx_ids::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_address_utxos() {
            crate::get_address_utxos::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_transaction_mempool() {
            crate::get_transaction_mempool::<FetchService>().await;
        }
    }

    // Span both the fetch and state indexers (compares their answers), so they
    // are not per-backend tests.
    #[tokio::test(flavor = "multi_thread")]
    async fn block_range_returns_default_pools() {
        crate::block_range_returns_default_pools().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn block_range_returns_all_pools() {
        crate::block_range_returns_all_pools().await;
    }

    mod state_service {
        #[allow(deprecated)]
        use zaino_state::StateService;

        #[tokio::test(flavor = "multi_thread")]
        async fn receives_mining_reward() {
            crate::receives_mining_reward::<StateService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn send_to_orchard() {
            crate::send_to_pool::<StateService>(wallet_tests::Pool::Orchard).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn send_to_sapling() {
            crate::send_to_pool::<StateService>(wallet_tests::Pool::Sapling).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn send_to_transparent() {
            crate::send_to_pool::<StateService>(wallet_tests::Pool::Transparent).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shield_for_validator() {
            crate::shield_for_validator::<StateService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_transaction_mined() {
            crate::get_transaction_mined::<StateService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_raw_mempool() {
            crate::get_raw_mempool::<StateService>().await;
        }

        // No get_mempool_tx here: the state backend returns mempool-tx txids
        // in display (reversed) byte order while the fetch backend returns
        // internal order, so the cross-backend txid comparison fails on
        // state (zingolabs/zaino#1225). The original test was FetchService-
        // only; re-enable once that bug is fixed.
    }
}
