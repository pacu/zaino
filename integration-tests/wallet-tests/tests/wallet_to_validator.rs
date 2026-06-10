//! Holds wallet-to-validator tests for Zaino.

#![forbid(unsafe_code)]

use zaino_state::ZcashIndexer;
use zaino_state::ZcashService;
use wallet_tests::from_inputs;
use zaino_testutils::TestManager;
use zaino_testutils::ValidatorExt;
use zaino_testutils::ValidatorKind;
use zainodlib::error::IndexerError;
use zip32::AccountId;

/// Sync the faucet; on zebrad, mature 100 coinbase blocks and shield so it has
/// spendable funds (zebrad can't mine directly to orchard in this setup).
async fn fund_faucet<V, Service>(
    test_manager: &TestManager<V, Service>,
    clients: &mut wallet_tests::Clients,
    validator: &ValidatorKind,
) where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    }
}

async fn connect_to_node_get_info_for_validator<V, Service>(validator: &ValidatorKind)
where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    let (mut test_manager, clients) = wallet_tests::launch_and_build::<V, Service>(validator, None, None).await;

    clients.faucet.do_info().await;
    clients.recipient.do_info().await;

    test_manager.close().await;
}

/// The standard send rhythm: send `amount` to the recipient's `pool`, mine a
/// block, sync the recipient, and assert it received `amount`. The two simple
/// parameters (`pool`, `amount`) replace what used to be an address string plus
/// a balance-field closure.
async fn send_and_assert_received<V, Service>(
    test_manager: &TestManager<V, Service>,
    clients: &mut wallet_tests::Clients,
    pool: wallet_tests::Pool,
    amount: u64,
) where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    let recipient_address = clients.get_recipient_address(pool.address_kind()).await;
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_address, amount, None)])
        .await
        .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.recipient.sync_and_await().await.unwrap();
    assert_eq!(
        pool.received_balance(&clients.recipient_balance().await),
        amount
    );
}

/// Launch, fund the faucet, then run the standard send-and-check rhythm.
async fn assert_send_to_pool<V, Service>(
    validator: &ValidatorKind,
    pool: wallet_tests::Pool,
    amount: u64,
) where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    let (mut test_manager, mut clients) = wallet_tests::launch_and_build::<V, Service>(validator, None, None).await;
    fund_faucet(&test_manager, &mut clients, validator).await;
    send_and_assert_received(&test_manager, &mut clients, pool, amount).await;
    test_manager.close().await;
}

async fn send_to_orchard<V, Service>(validator: &ValidatorKind)
where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    assert_send_to_pool::<V, Service>(validator, wallet_tests::Pool::Orchard, 250_000).await;
}

async fn send_to_sapling<V, Service>(validator: &ValidatorKind)
where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    assert_send_to_pool::<V, Service>(validator, wallet_tests::Pool::Sapling, 250_000).await;
}

async fn send_to_transparent<V, Service>(validator: &ValidatorKind)
where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    let (mut test_manager, mut clients) = wallet_tests::launch_and_build::<V, Service>(validator, None, None).await;

    fund_faucet(&test_manager, &mut clients, validator).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_taddr, 250_000, None)])
        .await
        .unwrap();

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;

    let fetch_service = test_manager.full_node_jsonrpc_connector().await;

    println!("\n\nFetching Chain Height!\n");

    let height = dbg!(fetch_service.get_blockchain_info().await.unwrap().blocks.0);

    println!("\n\nFetching Tx From Unfinalized Chain!\n");

    let unfinalised_transactions = fetch_service
        .get_address_txids(
            vec![clients.get_recipient_address("transparent").await],
            height,
            height,
        )
        .await
        .unwrap();

    dbg!(unfinalised_transactions.clone());
    test_manager
        .generate_blocks_and_wait_for_tip(99, test_manager.subscriber())
        .await;

    println!("\n\nFetching Tx From Finalized Chain!\n");

    let finalised_transactions = fetch_service
        .get_address_txids(
            vec![clients.get_recipient_address("transparent").await],
            height,
            height,
        )
        .await
        .unwrap();

    dbg!(finalised_transactions.clone());

    clients.recipient.sync_and_await().await.unwrap();

    assert_eq!(
        wallet_tests::Pool::Transparent.received_balance(&clients.recipient_balance().await),
        250_000
    );

    assert_eq!(unfinalised_transactions, finalised_transactions);
    // test_manager.local_net.print_stdout();

    test_manager.close().await;
}

async fn send_to_all<V, Service>(validator: &ValidatorKind)
where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    let (mut test_manager, mut clients) = wallet_tests::launch_and_build::<V, Service>(validator, None, None).await;

    test_manager
        .generate_blocks_and_wait_for_tip(2, test_manager.subscriber())
        .await;
    clients.faucet.sync_and_await().await.unwrap();

    // "Create" 3 orchard notes in faucet.
    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let recipient_ua = clients.get_recipient_address("unified").await;
    let recipient_zaddr = clients.get_recipient_address("sapling").await;
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_ua, 250_000, None)])
        .await
        .unwrap();
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_zaddr, 250_000, None)])
        .await
        .unwrap();
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_taddr, 250_000, None)])
        .await
        .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
        .await;
    clients.recipient.sync_and_await().await.unwrap();

    let balance = clients.recipient_balance().await;
    assert_eq!(wallet_tests::Pool::Orchard.received_balance(&balance), 250_000);
    assert_eq!(wallet_tests::Pool::Sapling.received_balance(&balance), 250_000);
    assert_eq!(wallet_tests::Pool::Transparent.received_balance(&balance), 250_000);

    test_manager.close().await;
}

async fn shield_for_validator<V, Service>(validator: &ValidatorKind)
where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    let (mut test_manager, mut clients) = wallet_tests::launch_and_build::<V, Service>(validator, None, None).await;

    fund_faucet(&test_manager, &mut clients, validator).await;

    let recipient_taddr = clients.get_recipient_address("transparent").await;
    from_inputs::quick_send(&mut clients.faucet, vec![(&recipient_taddr, 250_000, None)])
        .await
        .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
        .await;
    clients.recipient.sync_and_await().await.unwrap();

    assert_eq!(
        clients.recipient_balance().await
            .confirmed_transparent_balance
            .unwrap()
            .into_u64(),
        250_000
    );

    clients
        .recipient
        .quick_shield(AccountId::ZERO)
        .await
        .unwrap();
    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.recipient.sync_and_await().await.unwrap();

    assert_eq!(
        clients.recipient_balance().await
            .total_orchard_balance
            .unwrap()
            .into_u64(),
        235_000
    );

    test_manager.close().await;
}

async fn monitor_unverified_mempool_for_validator<V, Service>(validator: &ValidatorKind)
where
    V: ValidatorExt,
    Service: zaino_testutils::TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: zaino_testutils::PollableTip,
{
    let (mut test_manager, mut clients) = wallet_tests::launch_and_build::<V, Service>(validator, None, None).await;

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;
    clients.faucet.sync_and_await().await.unwrap();

    if matches!(validator, ValidatorKind::Zebrad) {
        test_manager
            .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
        clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
        test_manager
            .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
            .await;
        clients.faucet.sync_and_await().await.unwrap();
    };

    let txid_1 = from_inputs::quick_send(
        &mut clients.faucet,
        vec![(
            &wallet_tests::get_base_address_macro!(&mut clients.recipient, "unified"),
            250_000,
            None,
        )],
    )
    .await
    .unwrap();
    let txid_2 = from_inputs::quick_send(
        &mut clients.faucet,
        vec![(
            &wallet_tests::get_base_address_macro!(&mut clients.recipient, "sapling"),
            250_000,
            None,
        )],
    )
    .await
    .unwrap();

    println!("\n\nStarting Mempool!\n");
    clients.recipient.wallet.write().await.clear_all();
    clients.recipient.sync_and_await().await.unwrap();

    // test_manager.local_net.print_stdout();

    let fetch_service = test_manager.full_node_jsonrpc_connector().await;

    println!("\n\nFetching Raw Mempool!\n");
    let mempool_txids = fetch_service.get_raw_mempool().await.unwrap();
    dbg!(txid_1);
    dbg!(txid_2);
    dbg!(mempool_txids.clone());

    println!("\n\nFetching Mempool Tx 1!\n");
    let _transaction_1 = dbg!(
        fetch_service
            .get_raw_transaction(mempool_txids.transactions[0].clone(), Some(1))
            .await
    );

    println!("\n\nFetching Mempool Tx 2!\n");
    let _transaction_2 = dbg!(
        fetch_service
            .get_raw_transaction(mempool_txids.transactions[1].clone(), Some(1))
            .await
    );

    assert_eq!(
        clients.recipient_balance().await
            .unconfirmed_orchard_balance
            .unwrap()
            .into_u64(),
        250_000
    );
    assert_eq!(
        clients.recipient_balance().await
            .unconfirmed_sapling_balance
            .unwrap()
            .into_u64(),
        250_000
    );

    test_manager
        .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
        .await;

    println!("\n\nFetching Mined Tx 1!\n");
    let _transaction_1 = dbg!(
        fetch_service
            .get_raw_transaction(mempool_txids.transactions[0].clone(), Some(1))
            .await
    );

    println!("\n\nFetching Mined Tx 2!\n");
    let _transaction_2 = dbg!(
        fetch_service
            .get_raw_transaction(mempool_txids.transactions[1].clone(), Some(1))
            .await
    );

    clients.recipient.sync_and_await().await.unwrap();

    assert_eq!(
        clients.recipient_balance().await
            .confirmed_orchard_balance
            .unwrap()
            .into_u64(),
        250_000
    );
    assert_eq!(
        clients.recipient_balance().await
            .confirmed_orchard_balance
            .unwrap()
            .into_u64(),
        250_000
    );

    test_manager.close().await;
}

mod zcashd {
    #[allow(deprecated)]
    use zaino_state::FetchService;
    use zcash_local_net::validator::zcashd::Zcashd;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[allow(deprecated)]
    async fn connect_to_node_get_info() {
        connect_to_node_get_info_for_validator::<Zcashd, FetchService>(&ValidatorKind::Zcashd)
            .await;
    }

    mod sent_to {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        pub(crate) async fn orchard() {
            send_to_orchard::<Zcashd, FetchService>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        pub(crate) async fn sapling() {
            send_to_sapling::<Zcashd, FetchService>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        pub(crate) async fn transparent() {
            send_to_transparent::<Zcashd, FetchService>(&ValidatorKind::Zcashd).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        pub(crate) async fn all() {
            send_to_all::<Zcashd, FetchService>(&ValidatorKind::Zcashd).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[allow(deprecated)]
    async fn shield() {
        shield_for_validator::<Zcashd, FetchService>(&ValidatorKind::Zcashd).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[allow(deprecated)]
    async fn monitor_unverified_mempool() {
        monitor_unverified_mempool_for_validator::<Zcashd, FetchService>(&ValidatorKind::Zcashd)
            .await;
    }
}

mod zebrad {
    use super::*;

    mod fetch_service {
        use zcash_local_net::validator::zebrad::Zebrad;

        use super::*;
        #[allow(deprecated)]
        use zaino_state::FetchService;

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn connect_to_node_get_info() {
            connect_to_node_get_info_for_validator::<Zebrad, FetchService>(&ValidatorKind::Zebrad)
                .await;
        }
        mod send_to {
            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn sapling() {
                send_to_sapling::<Zebrad, FetchService>(&ValidatorKind::Zebrad).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn orchard() {
                send_to_orchard::<Zebrad, FetchService>(&ValidatorKind::Zebrad).await;
            }

            /// Bug documented in https://github.com/zingolabs/zaino/issues/145.
            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn transparent() {
                send_to_transparent::<Zebrad, FetchService>(&ValidatorKind::Zebrad).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn all() {
                send_to_all::<Zebrad, FetchService>(&ValidatorKind::Zebrad).await;
            }
        }
        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn shield() {
            shield_for_validator::<Zebrad, FetchService>(&ValidatorKind::Zebrad).await;
        }
        /// Bug documented in https://github.com/zingolabs/zaino/issues/144.
        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn monitor_unverified_mempool() {
            monitor_unverified_mempool_for_validator::<Zebrad, FetchService>(
                &ValidatorKind::Zebrad,
            )
            .await;
        }
    }

    mod state_service {
        use zcash_local_net::validator::zebrad::Zebrad;

        use super::*;
        #[allow(deprecated)]
        use zaino_state::StateService;

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn connect_to_node_get_info() {
            connect_to_node_get_info_for_validator::<Zebrad, StateService>(&ValidatorKind::Zebrad)
                .await;
        }
        mod send_to {
            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn sapling() {
                send_to_sapling::<Zebrad, StateService>(&ValidatorKind::Zebrad).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn orchard() {
                send_to_orchard::<Zebrad, StateService>(&ValidatorKind::Zebrad).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn transparent() {
                send_to_transparent::<Zebrad, StateService>(&ValidatorKind::Zebrad).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            pub(crate) async fn all() {
                send_to_all::<Zebrad, StateService>(&ValidatorKind::Zebrad).await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn shield() {
            shield_for_validator::<Zebrad, StateService>(&ValidatorKind::Zebrad).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn monitor_unverified_mempool() {
            monitor_unverified_mempool_for_validator::<Zebrad, StateService>(
                &ValidatorKind::Zebrad,
            )
            .await;
        }
    }
}
