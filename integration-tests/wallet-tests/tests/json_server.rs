//! Tests that compare the output of both `zcashd` and `zainod` through `FetchService`.

use zaino_common::network::ActivationHeights;
use zaino_common::{DatabaseConfig, ServiceConfig, StorageConfig};

#[allow(deprecated)]
use zaino_state::{
    ChainIndex, FetchService, FetchServiceConfig, FetchServiceSubscriber, ZcashIndexer,
    ZcashService as _,
};
use wallet_tests::from_inputs;
use zaino_testutils::{TestManager, ValidatorKind};
use zcash_local_net::logs::LogsToStdoutAndStderr as _;
use zcash_local_net::validator::zcashd::Zcashd;
use zebra_chain::subtree::NoteCommitmentSubtreeIndex;
use zebra_rpc::client::GetAddressBalanceRequest;
use zebra_rpc::methods::GetAddressTxIdsRequest;

#[allow(deprecated)]
async fn create_zcashd_test_manager_and_fetch_services() -> (
    TestManager<Zcashd, FetchService>,
    FetchService,
    FetchServiceSubscriber,
    FetchService,
    FetchServiceSubscriber,
    wallet_tests::Clients,
) {
    println!("Launching test manager..");
    let test_manager = TestManager::<Zcashd, FetchService>::launch(
        &ValidatorKind::Zcashd,
        None,
        None,
        None,
        true,
        true,
        false,
    )
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("Launching zcashd fetch service..");
    let zcashd_fetch_service = FetchService::spawn(FetchServiceConfig::new(
        test_manager.full_node_rpc_listen_address.to_string(),
        None,
        None,
        None,
        ServiceConfig::default(),
        StorageConfig {
            database: DatabaseConfig {
                path: test_manager
                    .local_net
                    .data_dir()
                    .path()
                    .to_path_buf()
                    .join("zcashd-fetch-service-zaino"),
                ..Default::default()
            },
            ..Default::default()
        },
        zaino_common::Network::Regtest(ActivationHeights::default()),
        None,
    ))
    .await
    .unwrap();
    let zcashd_subscriber = zcashd_fetch_service.get_subscriber().inner();

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("Launching zaino fetch service..");
    let zaino_fetch_service = FetchService::spawn(FetchServiceConfig::new(
        test_manager
            .zaino_json_rpc_listen_address
            .expect("zaino jsonrpc address must be active for these tests")
            .to_string(),
        test_manager.json_server_cookie_dir.clone(),
        None,
        None,
        ServiceConfig::default(),
        StorageConfig {
            database: DatabaseConfig {
                path: test_manager
                    .local_net
                    .data_dir()
                    .path()
                    .to_path_buf()
                    .join("zaino-fetch-service-zaino"),
                ..Default::default()
            },
            ..Default::default()
        },
        zaino_common::Network::Regtest(ActivationHeights::default()),
        None,
    ))
    .await
    .unwrap();
    let zaino_subscriber = zaino_fetch_service.get_subscriber().inner();

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("Testmanager launch complete!");
    let clients = wallet_tests::build_clients(
        test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
        wallet_tests::default_heights(&ValidatorKind::Zcashd),
    );
    (
        test_manager,
        zcashd_fetch_service,
        zcashd_subscriber,
        zaino_fetch_service,
        zaino_subscriber,
        clients,
    )
}

#[allow(deprecated)]
async fn z_get_address_balance_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_taddr.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    clients.recipient.sync_and_await().await.unwrap();
    let recipient_balance = clients
        .recipient
        .account_balance(zip32::AccountId::ZERO)
        .await
        .unwrap();

    let zcashd_service_balance = zcashd_subscriber
        .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
        .await
        .unwrap();

    let zaino_service_balance = zaino_subscriber
        .z_get_address_balance(GetAddressBalanceRequest::new(vec![recipient_taddr]))
        .await
        .unwrap();

    dbg!(&recipient_balance);
    dbg!(&zcashd_service_balance);
    dbg!(&zaino_service_balance);

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
        zcashd_service_balance.balance(),
    );
    assert_eq!(zcashd_service_balance, zaino_service_balance);

    test_manager.close().await;
}

async fn get_raw_mempool_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    clients.faucet.sync_and_await().await.unwrap();

    let recipient_ua = &clients.get_recipient_address("unified").await;
    let recipient_taddr = &clients.get_recipient_address("transparent").await;
    from_inputs::quick_send(&mut clients.faucet, vec![(recipient_taddr, 250_000, None)])
        .await
        .unwrap();
    from_inputs::quick_send(&mut clients.faucet, vec![(recipient_ua, 250_000, None)])
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut zcashd_mempool = zcashd_subscriber.get_raw_mempool().await.unwrap();
    let mut zaino_mempool = zaino_subscriber.get_raw_mempool().await.unwrap();

    dbg!(&zcashd_mempool);
    zcashd_mempool.sort();

    dbg!(&zaino_mempool);
    zaino_mempool.sort();

    assert_eq!(zcashd_mempool, zaino_mempool);

    test_manager.close().await;
}

async fn get_mempool_info_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    clients.faucet.sync_and_await().await.unwrap();

    let recipient_ua = &clients.get_recipient_address("unified").await;
    let recipient_taddr = &clients.get_recipient_address("transparent").await;
    from_inputs::quick_send(&mut clients.faucet, vec![(recipient_taddr, 250_000, None)])
        .await
        .unwrap();
    from_inputs::quick_send(&mut clients.faucet, vec![(recipient_ua, 250_000, None)])
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let zcashd_subscriber_mempool_info = zcashd_subscriber.get_mempool_info().await.unwrap();
    let zaino_subscriber_mempool_info = zaino_subscriber.get_mempool_info().await.unwrap();

    assert_eq!(
        zcashd_subscriber_mempool_info,
        zaino_subscriber_mempool_info
    );

    test_manager.close().await;
}

async fn z_get_treestate_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    clients.faucet.sync_and_await().await.unwrap();

    let recipient_ua = &clients.get_recipient_address("unified").await;
    from_inputs::quick_send(&mut clients.faucet, vec![(recipient_ua, 250_000, None)])
        .await
        .unwrap();

    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    let chain_height = dbg!(zaino_subscriber.chain_height().await.unwrap()).0;

    let zcashd_treestate = dbg!(zcashd_subscriber
        .z_get_treestate(chain_height.to_string())
        .await
        .unwrap());

    let zaino_treestate = dbg!(zaino_subscriber
        .z_get_treestate(chain_height.to_string())
        .await
        .unwrap());

    assert_eq!(zcashd_treestate, zaino_treestate);

    test_manager.close().await;
}

async fn z_get_subtrees_by_index_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    clients.faucet.sync_and_await().await.unwrap();

    let recipient_ua = &clients.get_recipient_address("unified").await;
    from_inputs::quick_send(&mut clients.faucet, vec![(recipient_ua, 250_000, None)])
        .await
        .unwrap();

    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    let zcashd_subtrees = dbg!(zcashd_subscriber
        .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
        .await
        .unwrap());

    let zaino_subtrees = dbg!(zaino_subscriber
        .z_get_subtrees_by_index("orchard".to_string(), NoteCommitmentSubtreeIndex(0), None)
        .await
        .unwrap());

    assert_eq!(zcashd_subtrees, zaino_subtrees);

    test_manager.close().await;
}

async fn get_raw_transaction_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    clients.faucet.sync_and_await().await.unwrap();

    let recipient_ua = &clients.get_recipient_address("unified").await;
    let tx = from_inputs::quick_send(&mut clients.faucet, vec![(recipient_ua, 250_000, None)])
        .await
        .unwrap();

    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    test_manager.local_net.print_stdout();

    let zcashd_transaction = dbg!(zcashd_subscriber
        .get_raw_transaction(tx.first().to_string(), Some(1))
        .await
        .unwrap());

    let zaino_transaction = dbg!(zaino_subscriber
        .get_raw_transaction(tx.first().to_string(), Some(1))
        .await
        .unwrap());

    assert_eq!(zcashd_transaction, zaino_transaction);

    test_manager.close().await;
}

async fn get_tx_out_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_taddr.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    let zcashd_utxos = zcashd_subscriber
        .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
        .await
        .unwrap();
    let (_, txid, output_index, ..) = zcashd_utxos[0].into_parts();

    let zcashd_tx_out = zcashd_subscriber
        .get_tx_out(txid.to_string(), output_index.index(), Some(true))
        .await
        .unwrap();
    let zaino_tx_out = zaino_subscriber
        .get_tx_out(txid.to_string(), output_index.index(), Some(true))
        .await
        .unwrap();

    assert_eq!(zcashd_tx_out, zaino_tx_out);

    let zcashd_missing_tx_out = zcashd_subscriber
        .get_tx_out(txid.to_string(), output_index.index() + 100, None)
        .await
        .unwrap();
    let zaino_missing_tx_out = zaino_subscriber
        .get_tx_out(txid.to_string(), output_index.index() + 100, None)
        .await
        .unwrap();

    assert_eq!(zcashd_missing_tx_out, zaino_missing_tx_out);

    test_manager.close().await;
}

async fn get_address_tx_ids_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    let tx = from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_taddr.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    let chain_height: u32 = {
        let idx = &zcashd_subscriber.indexer;
        let snapshot = idx.snapshot_nonfinalized_state().await.unwrap();
        u32::from(idx.best_chaintip(&snapshot).await.unwrap().height)
    };
    dbg!(&chain_height);

    let zcashd_txids = zcashd_subscriber
        .get_address_tx_ids(GetAddressTxIdsRequest::new(
            vec![recipient_taddr.clone()],
            Some(chain_height - 2),
            Some(chain_height),
        ))
        .await
        .unwrap();

    let zaino_txids = zaino_subscriber
        .get_address_tx_ids(GetAddressTxIdsRequest::new(
            vec![recipient_taddr],
            Some(chain_height - 2),
            Some(chain_height),
        ))
        .await
        .unwrap();

    dbg!(&tx);
    dbg!(&zcashd_txids);
    assert_eq!(tx.first().to_string(), zcashd_txids[0]);

    dbg!(&zaino_txids);
    assert_eq!(zcashd_txids, zaino_txids);

    test_manager.close().await;
}

async fn z_get_address_utxos_inner() {
    let (
        mut test_manager,
        _zcashd_service,
        zcashd_subscriber,
        _zaino_service,
        zaino_subscriber,
        mut clients,
    ) = create_zcashd_test_manager_and_fetch_services().await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    let txid_1 = from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_taddr.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager.generate_blocks_and_wait_for_tips(1, &zaino_subscriber, &zcashd_subscriber)
    .await;

    clients.faucet.sync_and_await().await.unwrap();

    let zcashd_utxos = zcashd_subscriber
        .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr.clone()]))
        .await
        .unwrap();
    let (_, zcashd_txid, ..) = zcashd_utxos[0].into_parts();

    let zaino_utxos = zaino_subscriber
        .z_get_address_utxos(GetAddressBalanceRequest::new(vec![recipient_taddr]))
        .await
        .unwrap();
    let (_, zaino_txid, ..) = zaino_utxos[0].into_parts();

    dbg!(&txid_1);
    dbg!(&zcashd_utxos);
    assert_eq!(txid_1.first().to_string(), zcashd_txid.to_string());

    dbg!(&zaino_utxos);

    assert_eq!(zcashd_txid.to_string(), zaino_txid.to_string());

    test_manager.close().await;
}

// TODO: This module should not be called `zcashd`
mod zcashd {
    use super::*;

    pub(crate) mod zcash_indexer {

        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn z_get_address_balance() {
            z_get_address_balance_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_raw_mempool() {
            get_raw_mempool_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_mempool_info() {
            get_mempool_info_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn z_get_treestate() {
            z_get_treestate_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn z_get_subtrees_by_index() {
            z_get_subtrees_by_index_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_raw_transaction() {
            get_raw_transaction_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_tx_out() {
            get_tx_out_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn get_address_tx_ids() {
            get_address_tx_ids_inner().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn z_get_address_utxos() {
            z_get_address_utxos_inner().await;
        }
    }
}
