//! Devtool-backed ports of the wallet-to-validator tests, run against a Zaino
//! launched by `TestManager`. As the zcash-devtool client
//! ([`wallet_tests::devtool`]) reaches capability parity with the zingolib
//! `Clients`, the matching tests in `wallet_to_validator.rs` migrate here and
//! the zingolib versions are eventually retired (zingolabs/infrastructure#269).
//!
//! Current coverage is the orchard slice of the send/receive matrix: the
//! devtool client interface exposes only the unified address, so transparent
//! and sapling recipients wait on the `address(pool)` feature (see
//! devtools/add_regtest/feature_requests.md). Zebrad only — the devtool
//! client's compiled-in regtest activation heights match zebrad's launch
//! heights; zcashd launches with different default heights, which the client
//! rejects at construction.
//!
//! Requires a `zcash-devtool` binary built with `--features regtest_support`
//! in `TEST_BINARIES_DIR`/`PATH`, alongside the usual validator binaries.

use wallet_tests::devtool::DevtoolClients;
use zaino_proto::proto::service::TxFilter;
use zaino_state::{LightWalletIndexer, ZcashIndexer, ZcashService};
use zaino_testutils::{PollableTip, TestManager, TestService, ValidatorKind};
use zainodlib::error::IndexerError;
use zcash_local_net::validator::zebrad::Zebrad;

/// Launch an orchard-mining zebrad + Zaino on the `Service` backend and build
/// devtool faucet/recipient wallets against it. Mines one block past the
/// launch (height 1 is the sapling-activation coinbase; height 2 the first
/// orchard coinbase) so the faucet holds a spendable orchard note, and syncs
/// the faucet. The shared preamble of the ports below.
async fn launch_and_fund_faucet<Service>() -> (TestManager<Zebrad, Service>, DevtoolClients)
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
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
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
    let (mut test_manager, clients) = launch_and_fund_faucet::<Service>().await;

    let faucet_balance = dbg!(clients.faucet_balance().await);
    assert!(
        faucet_balance.orchard_spendable > 0,
        "faucet should hold a spendable orchard coinbase note, got {faucet_balance:?}"
    );

    test_manager.close().await;
}

/// Port of `wallet_to_validator::send_to_orchard` (zebrad): send 250_000 from
/// the faucet to the recipient's unified address, mine it in, and assert the
/// recipient's synced wallet shows the orchard receipt.
async fn send_to_orchard<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let (mut test_manager, mut clients) = launch_and_fund_faucet::<Service>().await;

    let recipient_ua = clients.get_recipient_address("unified").await;
    let txid = clients.send_from_faucet(&recipient_ua, 250_000).await;
    dbg!(txid);

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.sync_recipient().await;

    assert_eq!(
        wallet_tests::Pool::Orchard.spendable_balance(&clients.recipient_balance().await),
        250_000
    );

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
    let (test_manager, mut clients) = launch_and_fund_faucet::<Service>().await;

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
    let (mut test_manager, mut clients) = launch_and_fund_faucet::<Service>().await;

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
            crate::send_to_orchard::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_transaction_mined() {
            crate::get_transaction_mined::<FetchService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_transaction_mempool() {
            crate::get_transaction_mempool::<FetchService>().await;
        }
    }

    // Spans both the fetch and state indexers (compares their answers), so it
    // is not a per-backend test.
    #[tokio::test(flavor = "multi_thread")]
    async fn block_range_returns_default_pools() {
        crate::block_range_returns_default_pools().await;
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
            crate::send_to_orchard::<StateService>().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_transaction_mined() {
            crate::get_transaction_mined::<StateService>().await;
        }
    }
}
