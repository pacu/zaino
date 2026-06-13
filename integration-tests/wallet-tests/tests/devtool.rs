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
use zaino_state::{ZcashIndexer, ZcashService};
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
    }
}
