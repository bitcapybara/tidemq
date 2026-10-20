#[path = "replication_support/mod.rs"]
mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tidemq::wal::{Error, FRAME_HEADER_BYTES, JournalOptions, SegmentedJournal, inspect_segmented};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

const IDENTITY: [u8; 32] = [7; 32];

fn small_options() -> JournalOptions {
    JournalOptions {
        max_record_bytes: 512,
        max_file_bytes: 64 * 1024,
        max_pending_bytes: 4096,
        group_commit_bytes: 512,
        segment_target_bytes: 2048,
        max_segments: 32,
        max_checkpoint_bytes: 4096,
        max_retained_records: 64,
        control_reserve_bytes: 8192,
        ..JournalOptions::default()
    }
}

async fn active_segment(path: &Path, options: &JournalOptions) -> PathBuf {
    path.join(
        inspect_segmented(path, options, IDENTITY)
            .await
            .unwrap()
            .active_segment,
    )
}

async fn recover(path: &Path, options: JournalOptions) -> Vec<Vec<u8>> {
    let (journal, _, mut recovery) = SegmentedJournal::open(path, options, IDENTITY)
        .await
        .unwrap();
    let mut records = Vec::new();
    while let Some(record) = recovery.next().await.unwrap() {
        records.push(record.data);
    }
    journal.close().await.unwrap();
    records
}

async fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .unwrap();
    file.seek(std::io::SeekFrom::Start(offset)).await.unwrap();
    file.write_all(bytes).await.unwrap();
    file.sync_all().await.unwrap();
}

async fn assert_rejected_without_mutation(path: &Path, options: &JournalOptions) {
    let before = support::directory_contents(path).await;
    assert!(inspect_segmented(path, options, IDENTITY).await.is_err());
    assert_eq!(support::directory_contents(path).await, before);
    assert!(
        SegmentedJournal::open(path, options.clone(), IDENTITY)
            .await
            .is_err()
    );
    assert_eq!(support::directory_contents(path).await, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_records_share_a_durable_group_sync_across_bounded_segments() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions {
        group_commit_delay: Duration::from_millis(25),
        ..small_options()
    };
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
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
    assert!(
        !inspect_segmented(&path, &options, IDENTITY)
            .await
            .unwrap()
            .sealed_segments
            .is_empty()
    );
    let mut values: Vec<_> = recover(&path, options)
        .await
        .into_iter()
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
        .collect();
    values.sort_unstable();
    assert_eq!(values, (0..32).collect::<Vec<_>>());
}

#[tokio::test]
async fn inspection_never_mutates_and_only_the_active_partial_tail_is_repaired() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = small_options();
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    journal
        .append(b"durable".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let active = active_segment(&path, &options).await;
    let original = tokio::fs::metadata(&active).await.unwrap().len();
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&active)
        .await
        .unwrap();
    file.write_all(b"TIDE").await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    let before = support::directory_contents(&path).await;
    assert_eq!(
        inspect_segmented(&path, &options, IDENTITY)
            .await
            .unwrap()
            .incomplete_tail_bytes,
        4
    );
    assert_eq!(support::directory_contents(&path).await, before);
    let (journal, report, mut recovery) = SegmentedJournal::open(&path, options, IDENTITY)
        .await
        .unwrap();
    assert_eq!(report.incomplete_tail_bytes, 4);
    assert_eq!(recovery.next().await.unwrap().unwrap().data, b"durable");
    assert!(recovery.next().await.unwrap().is_none());
    assert_eq!(tokio::fs::metadata(active).await.unwrap().len(), original);
    journal.close().await.unwrap();
}

#[tokio::test]
async fn complete_active_payload_corruption_is_rejected_without_discarding_acknowledged_data() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = small_options();
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    journal
        .append(b"acknowledged".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let active = active_segment(&path, &options).await;
    let length = tokio::fs::metadata(&active).await.unwrap().len();
    overwrite(&active, length - 1, b"X").await;
    assert_rejected_without_mutation(&path, &options).await;
}

#[tokio::test]
async fn corrupt_record_length_cannot_masquerade_as_an_incomplete_tail() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = small_options();
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    let active = active_segment(&path, &options).await;
    let record_start = tokio::fs::metadata(&active).await.unwrap().len();
    journal
        .append(b"acknowledged".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    overwrite(&active, record_start + 8, &u64::MAX.to_le_bytes()).await;
    assert_rejected_without_mutation(&path, &options).await;
}

#[tokio::test]
async fn a_valid_active_header_with_partial_payload_is_the_only_discardable_body() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = small_options();
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    let active = active_segment(&path, &options).await;
    let record_start = tokio::fs::metadata(&active).await.unwrap().len();
    journal
        .append(vec![7; 128])
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let partial_bytes = FRAME_HEADER_BYTES as u64 + 10;
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&active)
        .await
        .unwrap();
    file.set_len(record_start + partial_bytes).await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);
    let (journal, report, mut recovery) = SegmentedJournal::open(&path, options, IDENTITY)
        .await
        .unwrap();
    assert_eq!(report.incomplete_tail_bytes, partial_bytes);
    assert!(recovery.next().await.unwrap().is_none());
    assert_eq!(
        tokio::fs::metadata(active).await.unwrap().len(),
        record_start
    );
    journal.close().await.unwrap();
}

#[tokio::test]
async fn sealed_segments_reject_partial_and_complete_corruption() {
    for partial in [true, false] {
        let directory = support::Directory::new().await;
        let path = directory.path().join("journal");
        let options = small_options();
        let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
            .await
            .unwrap();
        for value in 0..8 {
            journal
                .append(vec![value; 512])
                .await
                .unwrap()
                .durable()
                .await
                .unwrap();
        }
        journal.close().await.unwrap();
        let report = inspect_segmented(&path, &options, IDENTITY).await.unwrap();
        let sealed = path.join(report.sealed_segments.first().unwrap());
        let length = tokio::fs::metadata(&sealed).await.unwrap().len();
        if partial {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&sealed)
                .await
                .unwrap();
            file.set_len(length - 1).await.unwrap();
            file.sync_all().await.unwrap();
        } else {
            overwrite(&sealed, length - 1, &[255]).await;
        }
        assert_rejected_without_mutation(&path, &options).await;
    }
}

#[tokio::test]
async fn checkpoint_references_preserve_segment_bytes_and_exclude_superseded_records() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = small_options();
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    let mut ids = Vec::new();
    for value in 0..8 {
        let (id, durable) = journal.append_retained(vec![value; 512]).await.unwrap();
        durable.durable().await.unwrap();
        ids.push(id);
    }
    let before = support::directory_contents(&path).await;
    journal
        .checkpoint(b"control-at-barrier".to_vec(), vec![ids[3], ids[7]])
        .await
        .unwrap();
    let after = support::directory_contents(&path).await;
    let new_active = active_segment(&path, &options).await;
    let mut retained_segments = 0;
    for (file, bytes) in &after {
        if file.extension().is_some_and(|extension| extension == "wal") {
            if let Some(original) = before.get(file) {
                assert_eq!(
                    original, bytes,
                    "Checkpoint must not rewrite retained payloads"
                );
                retained_segments += 1;
            } else {
                assert_eq!(file, &new_active);
            }
        }
    }
    assert!(retained_segments >= 2);
    let (tail, durable) = journal.append_retained(vec![8; 512]).await.unwrap();
    durable.durable().await.unwrap();
    journal.close().await.unwrap();
    let (journal, _, mut recovery) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    assert_eq!(recovery.checkpoint(), b"control-at-barrier");
    let mut recovered = Vec::new();
    while let Some(record) = recovery.next().await.unwrap() {
        recovered.push((record.id.unwrap(), record.data));
    }
    assert_eq!(
        recovered,
        vec![
            (ids[3], vec![3; 512]),
            (ids[7], vec![7; 512]),
            (tail, vec![8; 512])
        ]
    );
    journal
        .checkpoint(b"new-barrier".to_vec(), vec![tail])
        .await
        .unwrap();
    journal.close().await.unwrap();
    assert_eq!(recover(&path, options).await, vec![vec![8; 512]]);
}

#[tokio::test]
async fn missing_or_corrupt_recovery_authority_never_initializes_or_cleans_orphans() {
    for target in [
        "missing-manifest",
        "corrupt-manifest",
        "missing-checkpoint",
        "corrupt-checkpoint",
        "missing-segment",
        "foreign-identity",
    ] {
        let directory = support::Directory::new().await;
        let path = directory.path().join("journal");
        let options = small_options();
        let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
            .await
            .unwrap();
        let (id, durable) = journal.append_retained(vec![1; 512]).await.unwrap();
        durable.durable().await.unwrap();
        journal
            .checkpoint(b"control".to_vec(), vec![id])
            .await
            .unwrap();
        journal.close().await.unwrap();
        let active = active_segment(&path, &options).await;
        let checkpoint = support::matching_files(&path, "checkpoint-", ".bin")
            .await
            .pop()
            .unwrap();
        let orphan = path.join(format!(
            "segment-{}.wal",
            tidemq_protocol::Id::random().unwrap()
        ));
        tokio::fs::write(&orphan, b"unreferenced").await.unwrap();
        match target {
            "missing-manifest" => tokio::fs::remove_file(path.join("manifest.bin"))
                .await
                .unwrap(),
            "corrupt-manifest" => overwrite(&path.join("manifest.bin"), 0, b"X").await,
            "missing-checkpoint" => tokio::fs::remove_file(checkpoint).await.unwrap(),
            "corrupt-checkpoint" => overwrite(&checkpoint, 0, b"X").await,
            "missing-segment" => tokio::fs::remove_file(active).await.unwrap(),
            "foreign-identity" => {
                let before = support::directory_contents(&path).await;
                assert!(
                    SegmentedJournal::open(&path, options.clone(), [8; 32])
                        .await
                        .is_err()
                );
                assert_eq!(support::directory_contents(&path).await, before);
                continue;
            }
            _ => unreachable!(),
        }
        assert_rejected_without_mutation(&path, &options).await;
        assert!(tokio::fs::try_exists(orphan).await.unwrap());
    }
}

#[tokio::test]
async fn valid_manifest_is_verified_before_unreferenced_files_are_reclaimed() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = small_options();
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    journal
        .append(b"acknowledged".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    let orphan = path.join(format!(
        "segment-{}.wal",
        tidemq_protocol::Id::random().unwrap()
    ));
    tokio::fs::write(&orphan, b"unreferenced").await.unwrap();
    let before = support::directory_contents(&path).await;
    inspect_segmented(&path, &options, IDENTITY).await.unwrap();
    assert_eq!(support::directory_contents(&path).await, before);
    let (journal, _, mut recovery) = SegmentedJournal::open(&path, options, IDENTITY)
        .await
        .unwrap();
    assert_eq!(
        recovery.next().await.unwrap().unwrap().data,
        b"acknowledged"
    );
    assert!(recovery.next().await.unwrap().is_none());
    journal.cleanup_orphans().await.unwrap();
    journal.close().await.unwrap();
    assert!(!tokio::fs::try_exists(orphan).await.unwrap());
}

#[tokio::test]
async fn an_existing_legacy_layout_is_rejected_without_deleting_its_data() {
    let directory = support::Directory::new().await;
    for old_file in ["raft.wal", "snapshot.bin"] {
        let path = directory.path().join(old_file);
        tokio::fs::create_dir(&path).await.unwrap();
        tokio::fs::write(path.join(old_file), b"legacy data")
            .await
            .unwrap();
        assert_rejected_without_mutation(&path, &small_options()).await;
    }
}

#[tokio::test]
async fn retained_record_limit_preserves_control_progress_and_checkpoint_can_release_capacity() {
    let directory = support::Directory::new().await;
    let path = directory.path().join("journal");
    let options = JournalOptions {
        max_retained_records: 2,
        ..small_options()
    };
    let (journal, _, _) = SegmentedJournal::open(&path, options.clone(), IDENTITY)
        .await
        .unwrap();
    let (first, durable) = journal.append_retained(vec![1; 128]).await.unwrap();
    durable.durable().await.unwrap();
    let (_, durable) = journal.append_retained(vec![2; 128]).await.unwrap();
    durable.durable().await.unwrap();
    let error = match journal.append_retained(vec![3; 128]).await {
        Ok((_, pending)) => pending.durable().await.unwrap_err(),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Limit { .. }));
    journal
        .append(b"latest-vote".to_vec())
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal
        .checkpoint(b"latest-vote".to_vec(), vec![first])
        .await
        .unwrap();
    journal
        .append_retained(vec![3; 128])
        .await
        .unwrap()
        .1
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    assert_eq!(
        recover(&path, options).await,
        vec![vec![1; 128], vec![3; 128]]
    );
}
