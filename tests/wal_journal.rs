#[path = "replication_support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use tidemq::wal::{Error, Journal, JournalOptions, inspect};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_records_share_a_durable_group_sync() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions {
        group_commit_delay: Duration::from_millis(25),
        ..JournalOptions::default()
    };
    let (journal, _, _) = Journal::open(&path, options.clone()).await.unwrap();
    let start = Arc::new(tokio::sync::Barrier::new(33));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0_u64..32 {
        let journal = journal.clone();
        let start = start.clone();
        tasks.spawn(async move {
            start.wait().await;
            journal
                .append(index.to_le_bytes().to_vec())
                .await
                .unwrap()
                .durable()
                .await
                .unwrap();
        });
    }
    start.wait().await;
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let statistics = journal.statistics();
    assert_eq!(statistics.written_records, 32);
    assert!(statistics.sync_operations < 32);
    assert_eq!(
        statistics.available_pending_bytes,
        options.max_pending_bytes as usize
    );
    journal.close().await.unwrap();
    let (_, report, records) = Journal::open(&path, options).await.unwrap();
    assert_eq!(report.records, 32);
    let mut values: Vec<_> = records
        .into_iter()
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
        .collect();
    values.sort_unstable();
    assert_eq!(values, (0..32).collect::<Vec<_>>());
}

#[tokio::test]
async fn inspection_does_not_mutate_and_reopen_repairs_only_an_incomplete_tail() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions::default();
    let (journal, _, _) = Journal::open(&path, options.clone()).await.unwrap();
    journal
        .append(b"durable".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let original = tokio::fs::metadata(&path).await.unwrap().len();
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .unwrap();
    file.write_all(b"TIDE").await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    let report = inspect(&path, &options).await.unwrap();
    assert_eq!(report.records, 1);
    assert_eq!(report.incomplete_tail_bytes, 4);
    assert_eq!(
        tokio::fs::metadata(&path).await.unwrap().len(),
        original + 4
    );
    let (journal, recovered, records) = Journal::open(&path, options).await.unwrap();
    assert_eq!(recovered.incomplete_tail_bytes, 4);
    assert_eq!(records, vec![b"durable".to_vec()]);
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), original);
    journal.close().await.unwrap();
}

#[tokio::test]
async fn complete_checksum_corruption_is_rejected_without_discarding_acknowledged_records() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions::default();
    let (journal, _, _) = Journal::open(&path, options.clone()).await.unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        journal
            .append(payload)
            .await
            .unwrap()
            .durable()
            .await
            .unwrap();
    }
    journal.close().await.unwrap();
    let length = tokio::fs::metadata(&path).await.unwrap().len();
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .await
        .unwrap();
    file.seek(std::io::SeekFrom::Start(88)).await.unwrap();
    file.write_all(b"X").await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    assert!(matches!(
        inspect(&path, &options).await,
        Err(Error::Corrupt { .. })
    ));
    assert!(matches!(
        Journal::open(&path, options).await,
        Err(Error::Corrupt { .. })
    ));
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), length);
}

#[tokio::test]
async fn corrupt_length_cannot_masquerade_as_an_incomplete_unacknowledged_tail() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions::default();
    let (journal, _, _) = Journal::open(&path, options.clone()).await.unwrap();
    journal
        .append(b"acknowledged".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let length = tokio::fs::metadata(&path).await.unwrap().len();
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .await
        .unwrap();
    file.seek(std::io::SeekFrom::Start(8)).await.unwrap();
    file.write_all(&4096_u64.to_le_bytes()).await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    assert!(matches!(
        Journal::open(&path, options).await,
        Err(Error::Corrupt { .. })
    ));
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), length);
}

#[tokio::test]
async fn partial_payload_is_repaired_but_an_impossible_length_is_not() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions::default();
    let (journal, _, _) = Journal::open(&path, options.clone()).await.unwrap();
    journal
        .append(vec![7; 128])
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .await
        .unwrap();
    file.set_len(100).await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    let (journal, report, recovered) = Journal::open(&path, options.clone()).await.unwrap();
    assert_eq!(report.incomplete_tail_bytes, 100);
    assert!(recovered.is_empty());
    journal.close().await.unwrap();
    let mut invalid = Vec::from(*b"TIDEWAL1");
    invalid.extend_from_slice(&u64::MAX.to_le_bytes());
    invalid.extend_from_slice(&[0; 72]);
    tokio::fs::write(&path, invalid).await.unwrap();
    assert!(matches!(
        Journal::open(&path, options).await,
        Err(Error::Corrupt { .. })
    ));
}

#[tokio::test]
async fn record_capacity_and_checkpoint_rewrite_are_bounded() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions {
        max_record_bytes: 1024,
        max_file_bytes: 4096,
        max_pending_bytes: 2048,
        group_commit_bytes: 1024,
        ..JournalOptions::default()
    };
    let (journal, _, _) = Journal::open(&path, options.clone()).await.unwrap();
    assert!(matches!(
        journal.append(vec![0; 1025]).await,
        Err(Error::Limit { .. })
    ));
    journal
        .append(b"obsolete".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.replace(vec![b"retained".to_vec()]).await.unwrap();
    assert_eq!(journal.statistics().checkpoint_rewrites, 1);
    journal
        .append(b"tail".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let (journal, _, records) = Journal::open(&path, options).await.unwrap();
    assert_eq!(records, vec![b"retained".to_vec(), b"tail".to_vec()]);
    journal.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_group_never_reports_durability_and_poisons_subsequent_writes() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions {
        max_record_bytes: 512,
        max_file_bytes: 600,
        max_pending_bytes: 1024,
        group_commit_bytes: 512,
        ..JournalOptions::default()
    };
    let (journal, _, _) = Journal::open(&path, options.clone()).await.unwrap();
    journal
        .append(vec![1; 512])
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    assert!(
        journal
            .append(vec![2; 512])
            .await
            .unwrap()
            .durable()
            .await
            .is_err()
    );
    assert!(
        journal
            .append(vec![3; 1])
            .await
            .unwrap()
            .durable()
            .await
            .is_err()
    );
    assert_eq!(journal.statistics().written_records, 1);
    assert_eq!(inspect(&path, &options).await.unwrap().records, 1);
}
