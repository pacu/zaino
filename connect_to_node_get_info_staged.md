# Staged: `connect_to_node_get_info` devtool port

Ready-to-apply zaino-side changes for the last reachable `wallet_to_validator`
test (zingolabs/infrastructure#269). **Not yet in the live tree** — it depends on
`zcash_local_net::Client::get_info`, which is not on `add_client_support` yet
(still `97f35c9`). Staging it here keeps the 44 passing devtool tests compiling.

Apply all three steps together once the driver lands. The code is written against
the exact interface pinned in the devtool round-3 spec P1 (`GetInfoResponse {
server_uri: String, chain_name: String, chain_tip_height: u64 }` in
`zcash_local_net::client`); if the driver diverges, adjust field names/types here
to match.

---

## Step 1 — bump the infra revs (`integration-tests/wallet-tests/Cargo.toml`)

Set both `zingolabs/infrastructure` revs to the `add_client_support` commit that
adds `Client::get_info` (replace `<NEW_REV>`):

```toml
zingo_test_vectors = { git = "https://github.com/zingolabs/infrastructure.git", rev = "<NEW_REV>" }
zcash_local_net    = { git = "https://github.com/zingolabs/infrastructure.git", rev = "<NEW_REV>" }
```

---

## Step 2 — adapter (`integration-tests/wallet-tests/src/devtool.rs`)

Add `GetInfoResponse` to the `zcash_local_net::client` import:

```rust
use zcash_local_net::client::{
    zcash_devtool::{ZcashDevtool, ZcashDevtoolConfig},
    AddressReceiver, Client as _, GetInfoResponse, WalletBalance,
};
```

Add these methods inside `impl DevtoolClients` (next to `faucet_balance` /
`recipient_balance`, mirroring their shape):

```rust
    /// The faucet wallet's server/chain info (devtool `wallet get-info`).
    /// The connect smoke test only asserts the call succeeds.
    pub async fn get_info_faucet(&self) -> GetInfoResponse {
        Self::get_info(&self.faucet, "faucet").await
    }

    /// The recipient wallet's server/chain info (devtool `wallet get-info`).
    pub async fn get_info_recipient(&self) -> GetInfoResponse {
        Self::get_info(&self.recipient, "recipient").await
    }

    async fn get_info(client: &ZcashDevtool, who: &str) -> GetInfoResponse {
        client
            .get_info()
            .await
            .unwrap_or_else(|e| panic!("get_info for {who}: {e:?}"))
    }
```

Also update the module-doc "Known gaps" bullet — `do_info` is no longer a gap:
delete the `No do_info / transaction listing` sentence's `do_info` clause (the
`transaction_summaries` clause stays for `get_address_utxos{,_stream}`).

---

## Step 3 — test (`integration-tests/wallet-tests/tests/devtool.rs`)

Add a no-fund launch helper (the smoke test needs launched + built clients, no
mining or sync) — place it after `launch_and_fund_faucet`:

```rust
/// Launch an orchard-mining zebrad + Zaino on the `Service` backend and build
/// devtool faucet/recipient wallets against it, without mining or syncing — the
/// minimal preamble for tests that only exercise wallet↔server connectivity.
async fn launch_and_build_clients<Service>() -> (TestManager<Zebrad, Service>, DevtoolClients)
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

    let clients = wallet_tests::devtool::build_clients(
        test_manager
            .zaino_grpc_listen_address
            .expect("zaino enabled")
            .port(),
    )
    .await;

    (test_manager, clients)
}

/// Port of `connect_to_node_get_info` (wallet_to_validator, zebrad): the faucet
/// and recipient wallets can report node/server info without erroring. Smoke
/// test — the original discards the result.
async fn connect_to_node_get_info<Service>()
where
    Service: TestService,
    IndexerError: From<<<Service as ZcashService>::Subscriber as ZcashIndexer>::Error>,
    <Service as ZcashService>::Subscriber: PollableTip,
{
    let (mut test_manager, clients) = launch_and_build_clients::<Service>().await;

    clients.get_info_faucet().await;
    clients.get_info_recipient().await;

    test_manager.close().await;
}
```

Register one entry in `mod fetch_service` and one in `mod state_service`
(alongside `receives_mining_reward`):

```rust
        // mod fetch_service
        #[tokio::test(flavor = "multi_thread")]
        async fn connect_to_node_get_info() {
            crate::connect_to_node_get_info::<FetchService>().await;
        }
```
```rust
        // mod state_service
        #[tokio::test(flavor = "multi_thread")]
        async fn connect_to_node_get_info() {
            crate::connect_to_node_get_info::<StateService>().await;
        }
```

Then update the module-doc deferred list: move `connect_to_node_get_info` out of
"Deferred" (it's covered), leaving `send_to_transparent` (heavy, P2),
`monitor_unverified_mempool` (P3), the zcashd matrix (round-2 P0), the
`test_vectors` builder (round-2 P1), and `get_mempool_info` (skipped).

---

## After applying

44 → 46 devtool entries (fetch + state). Rebuild the CI image first if not
already on `DEVTOOL_VERSION=d820388…` (the baked binary must have `wallet
get-info`). Delete this file once landed.
