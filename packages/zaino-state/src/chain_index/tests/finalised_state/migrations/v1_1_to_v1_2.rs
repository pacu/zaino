//! V1.1 to V1.2 migration tests.

use lmdb::{Cursor as _, Transaction as _, WriteFlags};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;
use zaino_common::network::ActivationHeights;
use zaino_common::{DatabaseConfig, Network, StorageConfig};

use crate::chain_index::finalised_state::capability::{
    BlockTransparentExt as _, CapabilityRequest, DbCore as _, DbRead as _, DbVersion,
    MigrationStatus,
};
use crate::chain_index::finalised_state::db::v1::DB_SCHEMA_V1_HASH;
use crate::chain_index::finalised_state::db::DbBackend;
use crate::chain_index::finalised_state::entry::StoredEntryFixed;
use crate::chain_index::finalised_state::ZainoDB;
use crate::chain_index::tests::init_tracing;
use crate::chain_index::tests::vectors::{
    build_mockchain_source, load_test_vectors, TestVectorData,
};
use crate::{BlockCacheConfig, Height, Outpoint, TxLocation, ZainoVersionedSerde as _};

const MIGRATION_SPENT_PROGRESS_KEY: &[u8] = b"_migration_spent_progress_1_2_0_next_height";

async fn wait_for_v1_2_migration_complete(
    zaino_db: &ZainoDB,
    timeout_duration: Duration,
) -> Result<(), tokio::time::error::Elapsed> {
    timeout(timeout_duration, async {
        loop {
            let metadata = zaino_db.get_metadata().await.unwrap();

            if metadata.version
                == (DbVersion {
                    major: 1,
                    minor: 2,
                    patch: 0,
                })
                && metadata.migration_status == MigrationStatus::Empty
                && zaino_db
                    .router()
                    .backend(CapabilityRequest::TransparentHistExt)
                    .is_ok()
            {
                assert_eq!(metadata.schema_hash, DB_SCHEMA_V1_HASH);
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

async fn downgrade_to_v1_1_and_clear_spent_index(db: &DbBackend) {
    let env = db.env();
    let metadata_db = db.metadata_db().unwrap();
    let spent_db = db.spent_db().unwrap();

    let mut metadata = db.get_metadata().await.unwrap();
    metadata.version = DbVersion {
        major: 1,
        minor: 1,
        patch: 0,
    };
    metadata.schema_hash = [0u8; 32];
    metadata.migration_status = MigrationStatus::Empty;

    {
        let mut txn = env.begin_rw_txn().unwrap();

        txn.clear_db(spent_db).unwrap();

        let metadata_key = b"metadata";
        let metadata_bytes = StoredEntryFixed::new(metadata_key, metadata)
            .to_bytes()
            .unwrap();

        txn.put(
            metadata_db,
            metadata_key,
            &metadata_bytes,
            WriteFlags::empty(),
        )
        .unwrap();

        txn.commit().unwrap();
    }

    env.sync(true).unwrap();
}

async fn downgrade_to_v1_1_and_delete_spent_tail(db: &DbBackend, resume_height: Height) {
    let env = db.env();
    let metadata_db = db.metadata_db().unwrap();
    let spent_db = db.spent_db().unwrap();

    let spent_keys_to_delete: Vec<Vec<u8>> = {
        let txn = env.begin_ro_txn().unwrap();
        let mut cursor = txn.open_ro_cursor(spent_db).unwrap();
        let mut spent_keys_to_delete = Vec::new();
        let mut kept_count = 0usize;

        for (outpoint_bytes, spent_bytes) in cursor.iter_start() {
            let spent_entry = StoredEntryFixed::<TxLocation>::from_bytes(spent_bytes).unwrap();

            assert!(
                spent_entry.verify(outpoint_bytes),
                "spent entry checksum mismatch before partial migration setup"
            );

            if spent_entry.inner().block_height() >= resume_height.0 {
                spent_keys_to_delete.push(outpoint_bytes.to_vec());
            } else {
                kept_count += 1;
            }
        }

        assert!(
            kept_count > 0,
            "partial migration setup did not keep any spent entries before resume height {}",
            resume_height.0
        );

        assert!(
            !spent_keys_to_delete.is_empty(),
            "partial migration setup did not find any spent entries to delete at or after resume height {}",
            resume_height.0
        );

        spent_keys_to_delete
    };

    let mut metadata = db.get_metadata().await.unwrap();
    metadata.version = DbVersion {
        major: 1,
        minor: 1,
        patch: 0,
    };
    metadata.schema_hash = [0u8; 32];
    metadata.migration_status = MigrationStatus::PartialBuidInProgress;

    {
        let mut txn = env.begin_rw_txn().unwrap();

        for spent_key in spent_keys_to_delete {
            txn.del(spent_db, &spent_key, None).unwrap();
        }

        let progress_entry = StoredEntryFixed::new(MIGRATION_SPENT_PROGRESS_KEY, resume_height);
        let progress_bytes = progress_entry.to_bytes().unwrap();

        txn.put(
            metadata_db,
            &MIGRATION_SPENT_PROGRESS_KEY,
            &progress_bytes,
            WriteFlags::empty(),
        )
        .unwrap();

        let metadata_key = b"metadata";
        let metadata_bytes = StoredEntryFixed::new(metadata_key, metadata)
            .to_bytes()
            .unwrap();

        txn.put(
            metadata_db,
            metadata_key,
            &metadata_bytes,
            WriteFlags::empty(),
        )
        .unwrap();

        txn.commit().unwrap();
    }

    env.sync(true).unwrap();
}

async fn assert_spent_index_matches_transparent_data(db: &DbBackend) {
    let env = db.env();
    let spent_db = db.spent_db().unwrap();

    let db_height = db.db_height().await.unwrap().unwrap();

    for height_raw in 0..=db_height.0 {
        let height = Height(height_raw);
        let transparent_tx_list = db.get_block_transparent(height).await.unwrap();

        let txn = env.begin_ro_txn().unwrap();

        for (tx_index, tx_opt) in transparent_tx_list.tx().iter().enumerate() {
            let Some(transparent_tx) = tx_opt else {
                continue;
            };

            let expected_tx_location = TxLocation::new(height.0, tx_index as u16);

            for input in transparent_tx.inputs() {
                if input.is_null_prevout() {
                    continue;
                }

                let outpoint = Outpoint::new(*input.prevout_txid(), input.prevout_index());
                let outpoint_bytes = outpoint.to_bytes().unwrap();

                let spent_bytes = txn.get(spent_db, &outpoint_bytes).unwrap_or_else(|error| {
                    panic!(
                        "missing spent entry for outpoint {:?} spent at height {}: {error}",
                        outpoint, height.0
                    )
                });

                let spent_entry = StoredEntryFixed::<TxLocation>::from_bytes(spent_bytes)
                    .unwrap_or_else(|error| {
                        panic!(
                            "corrupt spent entry for outpoint {:?} spent at height {}: {error}",
                            outpoint, height.0
                        )
                    });

                assert!(
                    spent_entry.verify(&outpoint_bytes),
                    "spent checksum mismatch for outpoint {:?} spent at height {}",
                    outpoint,
                    height.0
                );

                assert_eq!(
                    spent_entry.inner(),
                    &expected_tx_location,
                    "spent entry points to wrong TxLocation for outpoint {:?}",
                    outpoint
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn v1_1_to_v1_2_spent_index_backfill_from_old_version() {
    init_tracing();

    let TestVectorData { blocks, .. } = load_test_vectors().unwrap();

    let temp_dir: TempDir = tempfile::tempdir().unwrap();
    let db_path: PathBuf = temp_dir.path().to_path_buf();

    let v1_config = BlockCacheConfig {
        storage: StorageConfig {
            database: DatabaseConfig {
                path: db_path,
                ..Default::default()
            },
            ..Default::default()
        },
        db_version: 1,
        network: Network::Regtest(ActivationHeights::default()),
    };

    let source = build_mockchain_source(blocks.clone());

    // Build current v1 database, then make it look like a v1.1.0 database missing spent data.
    let db = DbBackend::spawn_v1(&v1_config).await.unwrap();

    crate::chain_index::tests::vectors::sync_db_with_blockdata(&db, blocks.clone(), None).await;

    db.wait_until_ready().await;

    downgrade_to_v1_1_and_clear_spent_index(&db).await;

    dbg!(db.shutdown().await.unwrap());

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Open through ZainoDB so the migration manager runs v1.1.0 -> v1.2.0.
    let zaino_db = ZainoDB::spawn(v1_config.clone(), source).await.unwrap();

    wait_for_v1_2_migration_complete(&zaino_db, Duration::new(30, 0))
        .await
        .expect("migration did not complete before timeout.");

    dbg!(zaino_db.status());

    let migrated_backend = zaino_db
        .router()
        .backend(CapabilityRequest::WriteCore)
        .unwrap();

    assert_spent_index_matches_transparent_data(&migrated_backend).await;

    dbg!(zaino_db.shutdown().await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn v1_1_to_v1_2_spent_index_migration_resumes_mid_build() {
    init_tracing();

    let TestVectorData { blocks, .. } = load_test_vectors().unwrap();

    let temp_dir: TempDir = tempfile::tempdir().unwrap();
    let db_path: PathBuf = temp_dir.path().to_path_buf();

    let v1_config = BlockCacheConfig {
        storage: StorageConfig {
            database: DatabaseConfig {
                path: db_path,
                ..Default::default()
            },
            ..Default::default()
        },
        db_version: 1,
        network: Network::Regtest(ActivationHeights::default()),
    };

    let source = build_mockchain_source(blocks.clone());

    // Build current v1 database with a complete spent index.
    let db = DbBackend::spawn_v1(&v1_config).await.unwrap();

    crate::chain_index::tests::vectors::sync_db_with_blockdata(&db, blocks.clone(), None).await;

    db.wait_until_ready().await;

    // Simulate an interrupted v1.1.0 -> v1.2.0 migration:
    // heights below resume_height are already migrated, and the spent tail must be rebuilt.
    let resume_height = Height(51);

    downgrade_to_v1_1_and_delete_spent_tail(&db, resume_height).await;

    dbg!(db.shutdown().await.unwrap());

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Open through ZainoDB so the migration resumes from the stored progress height.
    let zaino_db = ZainoDB::spawn(v1_config.clone(), source).await.unwrap();

    wait_for_v1_2_migration_complete(&zaino_db, Duration::new(30, 0))
        .await
        .expect("migration did not complete before timeout.");

    dbg!(zaino_db.status());

    let migrated_backend = zaino_db
        .router()
        .backend(CapabilityRequest::WriteCore)
        .unwrap();

    assert_spent_index_matches_transparent_data(&migrated_backend).await;

    dbg!(zaino_db.shutdown().await.unwrap());
}
