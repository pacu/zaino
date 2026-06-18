//! Proof-of-concept: a devtool wallet against a **zcashd**-backed Zaino.
//!
//! The zebrad devtool suite (`tests/devtool.rs`) is complete. This isolates the
//! one remaining alignment for the zcashd matrix (the `json_server` oracle tests
//! and the zcashd send/query column): launch zcashd at the same activation
//! heights zebrad uses — `ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS`, which the devtool
//! wallet's compiled-in `supported_regtest_activation_heights` requires (the 46
//! zebrad tests prove the wallet accepts them) — rather than zcashd's default
//! heights (all = 1), which would mismatch the wallet's consensus branch IDs.
//!
//! zcashd mines ORCHARD coinbase to `REG_O_ADDR_FROM_ABANDONART` (the abandon-art
//! orchard address the devtool faucet owns), so the faucet is funded with no
//! transparent-coinbase shielding — the zcashd matrix is NOT gated on round-2
//! P1, only on this heights alignment.
//!
//! If this passes, the `json_server` tests port by swapping their zingolib
//! funding for `DevtoolClients` while keeping the zaino-vs-zcashd oracle
//! comparison. If zcashd rejects the heights (e.g. the NU6.1 lockbox gotcha,
//! which bit zebrad), that's the blocker to resolve before porting the mod.

#![allow(deprecated)] // FetchService is a deprecated re-export.

use wallet_tests::devtool::DevtoolClients;
use zaino_state::{ChainIndex, FetchService, ZcashIndexer};
use zaino_testutils::{TestManager, ValidatorKind, ZcashdDualFetchServices};
use zcash_local_net::validator::zcashd::Zcashd;
use zebra_chain::subtree::NoteCommitmentSubtreeIndex;
use zebra_rpc::client::GetAddressBalanceRequest;
use zebra_rpc::methods::GetAddressTxIdsRequest;

/// Launch zcashd (orchard-mining) at the devtool-compatible activation heights,
/// build the devtool faucet against the resulting Zaino, mine two orchard
/// coinbase notes, and assert the faucet sees them.
#[tokio::test(flavor = "multi_thread")]
async fn faucet_receives_zcashd_orchard_reward() {
    let mut test_manager = TestManager::<Zcashd, FetchService>::launch_mining_to(
        zaino_testutils::SHIELDED_FUNDING_POOL, // ORCHARD
        &ValidatorKind::Zcashd,
        None, // network -> Regtest
        // The heights the devtool wallet accepts (same as the zebrad path).
        Some(zaino_common::network::ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
        None,  // no chain cache: build fresh at these heights
        true,  // enable zaino
        false, // no json-rpc server (not needed for this smoke)
        false, // no clients (the devtool wallet is built separately)
    )
    .await
    .expect("launch zcashd TestManager");

    let mut clients = wallet_tests::devtool::build_clients(
        test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
    )
    .await;

    // Two orchard coinbase notes for the abandon-art faucet.
    test_manager
        .generate_blocks_and_wait_for_tip(2, test_manager.subscriber())
        .await;
    clients.sync_faucet().await;

    let balance = clients.faucet_balance().await;
    dbg!(&balance);
    assert!(
        balance.orchard_spendable > 0,
        "devtool faucet should see zcashd's orchard coinbase"
    );

    test_manager.close().await;
}

/// Launch zcashd dual fetch services at the devtool-compatible activation
/// heights (`ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS`, which the PoC above proves
/// zcashd accepts and the devtool wallet requires) and build the devtool
/// faucet/recipient wallets against the resulting Zaino — the devtool analogue
/// of json_server's `create_zcashd_test_manager_and_fetch_services`.
async fn create_zcashd_devtool_services() -> (ZcashdDualFetchServices, DevtoolClients) {
    let services = zaino_testutils::launch_zcashd_dual_fetch_services_at(
        zaino_common::network::ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
    )
    .await;
    let clients = wallet_tests::devtool::build_clients(
        services
            .test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
    )
    .await;
    (services, clients)
}

/// Devtool analogue of json_server's `jsonrpc_fund`: fund the faucet with orchard
/// coinbase notes, sync, fetch the recipient's transparent + unified addresses,
/// and if `send` is `Some(pool)`, send 250_000 to that pool's recipient address
/// and mine it in. Returns `(recipient_taddr, recipient_ua, sent_txid_hex)`. The
/// send=None mempool tests broadcast two unmined sends, so they need two notes.
async fn jsonrpc_fund(
    services: &ZcashdDualFetchServices,
    clients: &mut DevtoolClients,
    send: Option<wallet_tests::Pool>,
) -> (String, String, Option<String>) {
    let notes: u32 = if send.is_some() { 1 } else { 2 };
    services.generate_blocks_and_wait_for_tips(notes).await;
    clients.sync_faucet().await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    let recipient_ua = clients.get_recipient_address("unified").await;

    let sent = if let Some(pool) = send {
        let addr = clients.get_recipient_address(pool.address_kind()).await;
        let txid = clients.send_from_faucet(&addr, 250_000).await;
        services.generate_blocks_and_wait_for_tips(1).await;
        Some(txid.trim().to_string())
    } else {
        None
    };

    (recipient_taddr, recipient_ua, sent)
}

/// Devtool ports of the `json_server` oracle tests: zaino's answer (through its
/// JSON-RPC server, `zaino_subscriber`) must equal zcashd's own answer
/// (`zcashd_subscriber`). Verbatim from `tests/json_server.rs` except the
/// funding (devtool, not zingolib) and the sent txid (devtool's display-order
/// hex `String`, which matches the txid strings these RPCs return).
mod json_server {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn z_get_address_balance() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        let (recipient_taddr, _recipient_ua, _txid) =
            jsonrpc_fund(&services, &mut clients, Some(wallet_tests::Pool::Transparent)).await;

        let zcashd_service_balance = services
            .zcashd_subscriber
            .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
            .await
            .unwrap();
        let zaino_service_balance = services
            .zaino_subscriber
            .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_taddr]))
            .await
            .unwrap();

        dbg!(&zcashd_service_balance);
        dbg!(&zaino_service_balance);

        // The fixture sent exactly 250_000 to the recipient taddr.
        assert_eq!(zcashd_service_balance.balance(), 250_000);
        assert_eq!(zcashd_service_balance, zaino_service_balance);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_raw_mempool() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        let (recipient_taddr, recipient_ua, _txid) =
            jsonrpc_fund(&services, &mut clients, None).await;
        clients.send_from_faucet(&recipient_taddr, 250_000).await;
        clients.send_from_faucet(&recipient_ua, 250_000).await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let mut zcashd_mempool = services.zcashd_subscriber.get_raw_mempool().await.unwrap();
        let mut zaino_mempool = services.zaino_subscriber.get_raw_mempool().await.unwrap();

        dbg!(&zcashd_mempool);
        zcashd_mempool.sort();
        dbg!(&zaino_mempool);
        zaino_mempool.sort();

        assert_eq!(zcashd_mempool, zaino_mempool);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_mempool_info() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        let (recipient_taddr, recipient_ua, _txid) =
            jsonrpc_fund(&services, &mut clients, None).await;
        clients.send_from_faucet(&recipient_taddr, 250_000).await;
        clients.send_from_faucet(&recipient_ua, 250_000).await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let zcashd_info = services.zcashd_subscriber.get_mempool_info().await.unwrap();
        let zaino_info = services.zaino_subscriber.get_mempool_info().await.unwrap();

        assert_eq!(zcashd_info, zaino_info);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn z_get_treestate() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        jsonrpc_fund(&services, &mut clients, Some(wallet_tests::Pool::Orchard)).await;

        let chain_height = dbg!(services.zaino_subscriber.chain_height().await.unwrap()).0;

        let zcashd_treestate = dbg!(services
            .zcashd_subscriber
            .z_get_treestate(chain_height.to_string())
            .await
            .unwrap());
        let zaino_treestate = dbg!(services
            .zaino_subscriber
            .z_get_treestate(chain_height.to_string())
            .await
            .unwrap());

        assert_eq!(zcashd_treestate, zaino_treestate);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn z_get_subtrees_by_index() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        jsonrpc_fund(&services, &mut clients, Some(wallet_tests::Pool::Orchard)).await;

        let zcashd_subtrees = dbg!(services
            .zcashd_subscriber
            .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
            .await
            .unwrap());
        let zaino_subtrees = dbg!(services
            .zaino_subscriber
            .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
            .await
            .unwrap());

        assert_eq!(zcashd_subtrees, zaino_subtrees);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_raw_transaction() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        let (_recipient_taddr, _recipient_ua, tx) =
            jsonrpc_fund(&services, &mut clients, Some(wallet_tests::Pool::Orchard)).await;
        let tx = tx.expect("jsonrpc_fund sends a tx when given Some(pool)");

        let zcashd_transaction = dbg!(services
            .zcashd_subscriber
            .get_raw_transaction(tx.clone(), Some(1))
            .await
            .unwrap());
        let zaino_transaction = dbg!(services
            .zaino_subscriber
            .get_raw_transaction(tx, Some(1))
            .await
            .unwrap());

        assert_eq!(zcashd_transaction, zaino_transaction);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_tx_out() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        let (recipient_taddr, _recipient_ua, _txid) =
            jsonrpc_fund(&services, &mut clients, Some(wallet_tests::Pool::Transparent)).await;

        let zcashd_utxos = services
            .zcashd_subscriber
            .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
            .await
            .unwrap();
        let (_, txid, output_index, ..) = zcashd_utxos[0].into_parts();

        let zcashd_tx_out = services
            .zcashd_subscriber
            .get_tx_out(txid.to_string(), output_index.index(), Some(true))
            .await
            .unwrap();
        let zaino_tx_out = services
            .zaino_subscriber
            .get_tx_out(txid.to_string(), output_index.index(), Some(true))
            .await
            .unwrap();

        assert_eq!(zcashd_tx_out, zaino_tx_out);

        let zcashd_missing_tx_out = services
            .zcashd_subscriber
            .get_tx_out(txid.to_string(), output_index.index() + 100, None)
            .await
            .unwrap();
        let zaino_missing_tx_out = services
            .zaino_subscriber
            .get_tx_out(txid.to_string(), output_index.index() + 100, None)
            .await
            .unwrap();

        assert_eq!(zcashd_missing_tx_out, zaino_missing_tx_out);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_address_tx_ids() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        let (recipient_taddr, _recipient_ua, tx) =
            jsonrpc_fund(&services, &mut clients, Some(wallet_tests::Pool::Transparent)).await;
        let tx = tx.expect("jsonrpc_fund sends a tx when given Some(pool)");

        let chain_height: u32 = {
            let idx = &services.zcashd_subscriber.indexer;
            let snapshot = idx.snapshot_nonfinalized_state().await.unwrap();
            u32::from(idx.best_chaintip(&snapshot).await.unwrap().height)
        };
        dbg!(&chain_height);

        let zcashd_txids = services
            .zcashd_subscriber
            .get_address_tx_ids(GetAddressTxIdsRequest::new(
                vec![recipient_taddr.clone()],
                Some(chain_height - 2),
                Some(chain_height),
            ))
            .await
            .unwrap();
        let zaino_txids = services
            .zaino_subscriber
            .get_address_tx_ids(GetAddressTxIdsRequest::new(
                vec![recipient_taddr],
                Some(chain_height - 2),
                Some(chain_height),
            ))
            .await
            .unwrap();

        dbg!(&tx);
        dbg!(&zcashd_txids);
        assert_eq!(tx, zcashd_txids[0]);

        dbg!(&zaino_txids);
        assert_eq!(zcashd_txids, zaino_txids);

        services.test_manager.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn z_get_address_utxos() {
        let (mut services, mut clients) = create_zcashd_devtool_services().await;

        let (recipient_taddr, _recipient_ua, txid_1) =
            jsonrpc_fund(&services, &mut clients, Some(wallet_tests::Pool::Transparent)).await;
        let txid_1 = txid_1.expect("jsonrpc_fund sends a tx when given Some(pool)");

        clients.sync_faucet().await;

        let zcashd_utxos = services
            .zcashd_subscriber
            .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
            .await
            .unwrap();
        let (_, zcashd_txid, ..) = zcashd_utxos[0].into_parts();

        let zaino_utxos = services
            .zaino_subscriber
            .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr]))
            .await
            .unwrap();
        let (_, zaino_txid, ..) = zaino_utxos[0].into_parts();

        dbg!(&txid_1);
        dbg!(&zcashd_utxos);
        assert_eq!(txid_1, zcashd_txid.to_string());

        dbg!(&zaino_utxos);
        assert_eq!(zcashd_txid.to_string(), zaino_txid.to_string());

        services.test_manager.close().await;
    }
}
