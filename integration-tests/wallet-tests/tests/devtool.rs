//! Smoke test for the zcash-devtool-backed wallet clients
//! ([`wallet_tests::devtool`]) against a Zaino launched by `TestManager` —
//! the zaino-side counterpart of zcash_local_net's `devtool_client`
//! integration tests, which prove the same flow against a zainod binary.
//! Here the indexer is Zaino in-process, so this additionally smokes
//! Zaino's own gRPC serving path end to end.
//!
//! Requires a `zcash-devtool` binary built with `--features regtest_support`
//! in `TEST_BINARIES_DIR`/`PATH`, alongside the usual validator binaries.

#[allow(deprecated)]
use zaino_state::FetchService;
use zaino_testutils::{TestManager, ValidatorKind};
use zcash_local_net::validator::zebrad::Zebrad;

/// Launch an orchard-mining zebrad + Zaino, fund the devtool faucet with one
/// shielded coinbase note, send 250_000 to the devtool recipient's unified
/// address, and assert the recipient sees it — the devtool analogue of
/// `check_received_mining_reward` plus one send.
#[allow(deprecated)]
#[tokio::test(flavor = "multi_thread")]
async fn zebrad_fetch_mining_reward_and_send() {
    let mut test_manager = TestManager::<Zebrad, FetchService>::launch_mining_to(
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

    // The launch block (height 1) is the sapling activation coinbase; one
    // more block gives the faucet a spendable orchard coinbase note.
    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.sync_faucet().await;

    let faucet_balance = dbg!(clients.faucet_balance().await);
    assert!(
        faucet_balance.orchard_spendable > 0,
        "faucet should hold a spendable orchard coinbase note, got {faucet_balance:?}"
    );

    let recipient_ua = clients.get_recipient_address("unified").await;
    let txid = clients.send_from_faucet(&recipient_ua, 250_000).await;
    dbg!(txid);

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.sync_recipient().await;

    let recipient_balance = dbg!(clients.recipient_balance().await);
    assert_eq!(
        wallet_tests::Pool::Orchard.spendable_balance(&recipient_balance),
        250_000
    );

    test_manager.close().await;
}
