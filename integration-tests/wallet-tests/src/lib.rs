//! Wallet-to-validator integration tests.
//!
//! These exercise zingolib lightclients (a faucet and a recipient) against a
//! running Zaino indexer. They live in their own workspace so the zingolib
//! dependency stack stays out of the zingolib-free `integration-tests`
//! workspace. The clients are built from a launched
//! [`zaino_testutils::TestManager`]'s gRPC address via [`build_clients`].

#![forbid(unsafe_code)]

use zaino_common::network::{ActivationHeights, ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS};
use zaino_testutils::ValidatorKind;
use zebra_chain::parameters::testnet::ConfiguredActivationHeights;
use zingo_test_vectors::seeds;
use zingolib::lightclient::LightClient;
use zingolib_testutils::scenarios::ClientBuilder;

/// Re-exports so relocated tests keep their original call sites.
pub use zingolib::get_base_address_macro;
pub use zingolib::testutils::lightclient::from_inputs;

/// Holds zingo lightclients along with the lightclient builder for
/// wallet-to-validator tests.
pub struct Clients {
    /// Lightclient builder.
    pub client_builder: ClientBuilder,
    /// Faucet (zingolib lightclient). Mining rewards are received here.
    pub faucet: LightClient,
    /// Recipient (zingolib lightclient).
    pub recipient: LightClient,
}

impl Clients {
    /// Returns the zcash address of the faucet.
    pub async fn get_faucet_address(&self, pool: &str) -> String {
        zingolib::get_base_address_macro!(self.faucet, pool)
    }

    /// Returns the zcash address of the recipient.
    pub async fn get_recipient_address(&self, pool: &str) -> String {
        zingolib::get_base_address_macro!(self.recipient, pool)
    }
}

/// Builds the faucet + recipient lightclients pointed at a running Zaino's
/// gRPC port, seeded from the shared test mnemonic.
///
/// `activation_heights` must match the heights the validator was launched
/// with: [`ActivationHeights::default`] for zcashd,
/// [`zaino_common::network::ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS`] for zebrad.
pub fn build_clients(zaino_grpc_listen_port: u16, activation_heights: ActivationHeights) -> Clients {
    let mut client_builder = ClientBuilder::new(
        zaino_testutils::make_uri(zaino_grpc_listen_port),
        tempfile::tempdir().expect("create tempdir for lightclient wallets"),
    );

    let configured_activation_heights: ConfiguredActivationHeights = activation_heights.into();
    let faucet = client_builder.build_faucet(true, configured_activation_heights);
    let recipient = client_builder.build_client(
        seeds::HOSPITAL_MUSEUM_SEED.to_string(),
        1,
        true,
        configured_activation_heights,
    );
    Clients {
        client_builder,
        faucet,
        recipient,
    }
}

/// The activation heights `TestManager::launch` uses by default for a given
/// validator (i.e. when launched with `activation_heights: None`). Relocated
/// wallet helpers that are generic over the validator use this to build clients
/// whose view matches the launched chain.
pub fn default_heights(validator: &ValidatorKind) -> ActivationHeights {
    match validator {
        ValidatorKind::Zcashd => ActivationHeights::default(),
        ValidatorKind::Zebrad => ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
    }
}

/// Smoke tests relocated from `zaino-testutils`: launch a validator + Zaino,
/// build wallet clients against it, and exercise mining-reward receipt and
/// sends. Organised by validator / service backend.
#[cfg(test)]
mod launch_clients {
    use super::build_clients;
    use zaino_common::network::{ActivationHeights, ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS};
    use zaino_testutils::{TestManager, ValidatorKind};

    mod zcashd {
        use super::*;
        #[allow(deprecated)]
        use zaino_state::FetchService;
        use zcash_local_net::validator::zcashd::Zcashd;

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn zaino_clients() {
            let mut test_manager = TestManager::<Zcashd, FetchService>::launch(
                &ValidatorKind::Zcashd,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .await
            .unwrap();
            let clients = build_clients(
                test_manager
                    .zaino_grpc_listen_address
                    .expect("zaino enabled")
                    .port(),
                ActivationHeights::default(),
            );
            dbg!(clients.faucet.do_info().await);
            dbg!(clients.recipient.do_info().await);
            test_manager.close().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[allow(deprecated)]
        async fn zaino_clients_receive_mining_reward() {
            let mut test_manager = TestManager::<Zcashd, FetchService>::launch(
                &ValidatorKind::Zcashd,
                None,
                None,
                None,
                true,
                false,
                false,
            )
            .await
            .unwrap();
            let mut clients = build_clients(
                test_manager
                    .zaino_grpc_listen_address
                    .expect("zaino enabled")
                    .port(),
                ActivationHeights::default(),
            );

            clients.faucet.sync_and_await().await.unwrap();
            dbg!(clients
                .faucet
                .account_balance(zip32::AccountId::ZERO)
                .await
                .unwrap());

            assert!(
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0
                        || clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64() > 0,
                    "No mining reward received from Zcashd. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
                );

            test_manager.close().await;
        }
    }

    mod zebrad {
        use super::*;

        mod fetch_service {
            use super::*;
            #[allow(deprecated)]
            use zaino_state::FetchService;
            use zaino_testutils::ZEBRAD_TESTNET_CACHE_DIR;
            use zcash_local_net::validator::zebrad::Zebrad;
            use zebra_chain::parameters::NetworkKind;
            use zip32::AccountId;

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_clients() {
                let mut test_manager = TestManager::<Zebrad, FetchService>::launch(
                    &ValidatorKind::Zebrad,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_clients_receive_mining_reward() {
                let mut test_manager = TestManager::<Zebrad, FetchService>::launch(
                    &ValidatorKind::Zebrad,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let mut clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );

                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                test_manager
                    .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
                    .await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0
                        || clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64() > 0,
                    "No mining reward received from Zebrad. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_clients_receive_mining_reward_and_send() {
                let mut test_manager = TestManager::<Zebrad, FetchService>::launch(
                    &ValidatorKind::Zebrad,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let mut clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );

                test_manager
                    .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
                    .await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                        > 0,
                    "No mining reward received from Zebrad. Faucet Transparent Balance: {:}.",
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                );

                // *Send all transparent funds to own orchard address.
                clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
                test_manager
                    .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
                    .await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0,
                "No funds received from shield. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                let recipient_zaddr = clients.get_recipient_address("sapling").await.to_string();
                zingolib::testutils::lightclient::from_inputs::quick_send(
                    &mut clients.faucet,
                    vec![(&recipient_zaddr, 250_000, None)],
                )
                .await
                .unwrap();

                test_manager
                    .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
                    .await;
                clients.recipient.sync_and_await().await.unwrap();
                dbg!(clients
                    .recipient
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert_eq!(
                    clients
                        .recipient
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_sapling_balance
                        .unwrap()
                        .into_u64(),
                    250_000
                );

                test_manager.close().await;
            }

            #[ignore = "requires fully synced testnet."]
            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_testnet() {
                let mut test_manager = TestManager::<Zebrad, FetchService>::launch(
                    &ValidatorKind::Zebrad,
                    Some(NetworkKind::Testnet),
                    None,
                    ZEBRAD_TESTNET_CACHE_DIR.clone(),
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }
        }

        mod state_service {
            use super::*;
            #[allow(deprecated)]
            use zaino_state::StateService;
            use zaino_testutils::ZEBRAD_TESTNET_CACHE_DIR;
            use zcash_local_net::validator::zebrad::Zebrad;
            use zebra_chain::parameters::NetworkKind;
            use zip32::AccountId;

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_clients() {
                let mut test_manager = TestManager::<Zebrad, StateService>::launch(
                    &ValidatorKind::Zebrad,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_clients_receive_mining_reward() {
                let mut test_manager = TestManager::<Zebrad, StateService>::launch(
                    &ValidatorKind::Zebrad,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();

                let mut clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );

                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                test_manager
                    .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
                    .await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0
                        || clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64() > 0,
                    "No mining reward received from Zebrad. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_clients_receive_mining_reward_and_send() {
                let mut test_manager = TestManager::<Zebrad, StateService>::launch(
                    &ValidatorKind::Zebrad,
                    None,
                    None,
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();

                let mut clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );

                test_manager
                    .generate_blocks_and_wait_for_tip(100, test_manager.subscriber())
                    .await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                        > 0,
                    "No mining reward received from Zebrad. Faucet Transparent Balance: {:}.",
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                );

                // *Send all transparent funds to own orchard address.
                clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
                test_manager
                    .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
                    .await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0,
                "No funds received from shield. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                let recipient_zaddr = clients.get_recipient_address("sapling").await.to_string();
                zingolib::testutils::lightclient::from_inputs::quick_send(
                    &mut clients.faucet,
                    vec![(&recipient_zaddr, 250_000, None)],
                )
                .await
                .unwrap();

                test_manager
                    .generate_blocks_and_wait_for_tip(1, test_manager.subscriber())
                    .await;
                clients.recipient.sync_and_await().await.unwrap();
                dbg!(clients
                    .recipient
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert_eq!(
                    clients
                        .recipient
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_sapling_balance
                        .unwrap()
                        .into_u64(),
                    250_000
                );

                test_manager.close().await;
            }

            #[ignore = "requires fully synced testnet."]
            #[tokio::test(flavor = "multi_thread")]
            #[allow(deprecated)]
            async fn zaino_testnet() {
                let mut test_manager = TestManager::<Zebrad, StateService>::launch(
                    &ValidatorKind::Zebrad,
                    Some(NetworkKind::Testnet),
                    None,
                    ZEBRAD_TESTNET_CACHE_DIR.clone(),
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let clients = build_clients(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("zaino enabled")
                        .port(),
                    ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS,
                );
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }
        }
    }
}
