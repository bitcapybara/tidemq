use super::*;
use std::collections::BTreeMap;

fn options() -> JournalOptions {
    JournalOptions {
        max_record_bytes: 1024,
        max_file_bytes: 256 * 1024,
        max_pending_bytes: 8192,
        segment_target_bytes: 4096,
        max_checkpoint_bytes: 4096,
        control_reserve_bytes: 144 * 1024,
        ..JournalOptions::default()
    }
}
async fn opened(path: &Path) -> (SegmentedJournal, SegmentedInspection, Recovery) {
    SegmentedJournal::open(path, options(), [7; 32])
        .await
        .unwrap()
}
#[tokio::test]
async fn rolls_and_checkpoints_references_without_rewriting_payloads() {
    let dir = Directory::new();
    let (journal, _, mut recovery) = opened(dir.path()).await;
    assert!(recovery.next().await.unwrap().is_none());
    let mut ids = Vec::new();
    for value in 0..12u8 {
        let (id, durable) = journal.append_retained(vec![value; 900]).await.unwrap();
        durable.durable().await.unwrap();
        ids.push(id);
    }
    assert!(journal.statistics().segment_count >= 4);
    let before = directory_files(dir.path())
        .await
        .unwrap()
        .into_iter()
        .filter(|f| f.kind == SegmentedFileKind::Segment)
        .map(|f| {
            (
                f.name.clone(),
                std::fs::read(dir.path().join(f.name)).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let retained = ids.get(7..).unwrap().to_vec();
    journal
        .checkpoint(b"control".to_vec(), retained.clone())
        .await
        .unwrap();
    assert_eq!(journal.statistics().retained_records, 5);
    for (name, bytes) in before {
        if dir.path().join(&name).exists() {
            assert_eq!(std::fs::read(dir.path().join(name)).unwrap(), bytes);
        }
    }
    journal.close().await.unwrap();
    drop(journal);
    let (journal, _, mut recovery) = opened(dir.path()).await;
    assert_eq!(recovery.checkpoint(), b"control");
    for (position, id) in retained.into_iter().enumerate() {
        let record = recovery.next().await.unwrap().unwrap();
        assert_eq!(record.id, Some(id));
        assert_eq!(record.data, vec![(position + 7) as u8; 900]);
    }
    assert!(recovery.next().await.unwrap().is_none());
    journal.close().await.unwrap();
}
#[tokio::test]
async fn dropped_reference_is_not_revived_by_reused_raft_index_or_later_rotation() {
    let dir = Directory::new();
    let (journal, _, recovery) = opened(dir.path()).await;
    drop(recovery);
    let (old, durable) = journal.append_retained(b"old".to_vec()).await.unwrap();
    durable.durable().await.unwrap();
    journal
        .checkpoint(b"truncate".to_vec(), Vec::new())
        .await
        .unwrap();
    let (new, durable) = journal
        .append_retained(b"replacement".to_vec())
        .await
        .unwrap();
    durable.durable().await.unwrap();
    assert_ne!(old, new);
    journal.close().await.unwrap();
    drop(journal);
    let (journal, _, mut recovery) = opened(dir.path()).await;
    assert_eq!(recovery.checkpoint(), b"truncate");
    let record = recovery.next().await.unwrap().unwrap();
    assert_eq!(record.id, Some(new));
    assert_eq!(record.data, b"replacement");
    assert!(recovery.next().await.unwrap().is_none());
    journal.close().await.unwrap();
}
#[test]
fn default_and_inconsistent_segment_limits_are_checked() {
    JournalOptions::default().validate_segmented().unwrap();
    let value = JournalOptions {
        control_reserve_bytes: 0,
        ..JournalOptions::default()
    };
    assert!(value.validate_segmented().is_err());
    let value = JournalOptions {
        max_segments: 65,
        ..JournalOptions::default()
    };
    assert!(value.validate_segmented().is_err());
    assert_eq!(segmented_file_kind("raft.wal"), None);
    assert_eq!(segmented_file_kind("segment-../bad.wal"), None);
}

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tidemq-segment-unit-{}",
            uuid::Uuid::from_bytes(format::new_id().unwrap())
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn pressure_relocates_only_one_mixed_segment_and_never_replays_copied_history() {
    let dir = Directory::new();
    let options = JournalOptions {
        max_file_bytes: 96 * 1024,
        control_reserve_bytes: 60 * 1024,
        max_segments: 32,
        max_retained_records: 128,
        ..options()
    };
    let (journal, _, recovery) = SegmentedJournal::open(dir.path(), options.clone(), [7; 32])
        .await
        .unwrap();
    drop(recovery);
    let mut records = Vec::new();
    while journal.statistics().current_file_bytes < 32 * 1024 {
        let value = records.len() as u8;
        let (id, durable) = journal.append_retained(vec![value; 900]).await.unwrap();
        durable.durable().await.unwrap();
        records.push((id, value));
    }
    let sealed = inspect_segmented(dir.path(), &options, [7; 32])
        .await
        .unwrap()
        .sealed_segments;
    let before = sealed
        .iter()
        .map(|name| (name.clone(), std::fs::read(dir.path().join(name)).unwrap()))
        .collect::<BTreeMap<_, _>>();
    let retained = records
        .iter()
        .step_by(3)
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    journal
        .checkpoint(b"pressure".to_vec(), retained.clone())
        .await
        .unwrap();
    assert_eq!(journal.statistics().relocation_operations, 1);
    assert!(journal.statistics().relocated_bytes <= options.segment_target_bytes);
    assert_eq!(
        before
            .keys()
            .filter(|name| !dir.path().join(name).exists())
            .count(),
        1
    );
    for (name, bytes) in before {
        if dir.path().join(&name).exists() {
            assert_eq!(std::fs::read(dir.path().join(name)).unwrap(), bytes);
        }
    }
    let (new, durable) = journal.append_retained(b"new-tail".to_vec()).await.unwrap();
    durable.durable().await.unwrap();
    journal.close().await.unwrap();
    drop(journal);
    let (journal, _, mut recovery) = SegmentedJournal::open(dir.path(), options, [7; 32])
        .await
        .unwrap();
    assert_eq!(recovery.checkpoint(), b"pressure");
    for (id, value) in records.iter().step_by(3) {
        let record = recovery.next().await.unwrap().unwrap();
        assert_eq!(record.id, Some(*id));
        assert_eq!(record.data, vec![*value; 900]);
    }
    let record = recovery.next().await.unwrap().unwrap();
    assert_eq!(record.id, Some(new));
    assert_eq!(record.data, b"new-tail");
    assert!(recovery.next().await.unwrap().is_none());
    journal.close().await.unwrap();
}

#[test]
fn maximum_reference_checkpoint_and_large_control_state_fit_default_bound() {
    let references = (0..65_536)
        .map(|value| format::Location {
            id: RecordId(u64::MAX - value),
            segment: [255; 16],
            offset: u64::MAX,
            frame_bytes: u64::MAX,
            sequence: u64::MAX,
        })
        .collect();
    let checkpoint = format::Checkpoint {
        version: format::LAYOUT_VERSION,
        identity: [255; 32],
        replay_sequence: u64::MAX,
        control: vec![255; 2 * 1024 * 1024],
        references,
    };
    let encoded =
        disk::value_frame(&checkpoint, JournalOptions::default().max_checkpoint_bytes).unwrap();
    assert!(encoded.len() < JournalOptions::default().max_checkpoint_bytes);
}

#[tokio::test]
async fn partial_purge_of_reference_only_segment_does_not_revive_or_reject_dead_frames() {
    let dir = Directory::new();
    let small = JournalOptions {
        max_file_bytes: 96 * 1024,
        control_reserve_bytes: 60 * 1024,
        max_segments: 32,
        max_retained_records: 128,
        ..options()
    };
    let (journal, _, recovery) = SegmentedJournal::open(dir.path(), small.clone(), [7; 32])
        .await
        .unwrap();
    drop(recovery);
    let mut records = Vec::new();
    while journal.statistics().current_file_bytes < 32 * 1024 {
        let value = records.len() as u8;
        let (id, durable) = journal.append_retained(vec![value; 900]).await.unwrap();
        durable.durable().await.unwrap();
        records.push((id, value));
    }
    let retained = records
        .iter()
        .enumerate()
        .filter(|(position, _)| position % 3 != 2)
        .map(|(_, record)| record.0)
        .collect::<Vec<_>>();
    journal
        .checkpoint(b"first".to_vec(), retained.clone())
        .await
        .unwrap();
    assert_eq!(journal.statistics().relocation_operations, 1);
    let manifest: format::Manifest = disk::read_value(
        &dir.path().join(SEGMENTED_MANIFEST_FILE),
        format::MAX_MANIFEST_BYTES,
    )
    .await
    .unwrap();
    let relocated = manifest.sealed.iter().find(|s| s.reference_only).unwrap();
    let checkpoint: format::Checkpoint = disk::read_value(
        &dir.path()
            .join(format::checkpoint_name(manifest.checkpoint.unwrap().id)),
        small.max_checkpoint_bytes,
    )
    .await
    .unwrap();
    let copied = checkpoint
        .references
        .iter()
        .filter(|r| r.segment == relocated.id)
        .map(|r| r.id)
        .collect::<Vec<_>>();
    assert!(copied.len() >= 2);
    let dropped = *copied.first().unwrap();
    journal.close().await.unwrap();
    drop(journal);
    let low_pressure = JournalOptions {
        max_file_bytes: 128 * 1024,
        ..small
    };
    let (journal, _, mut recovery) =
        SegmentedJournal::open(dir.path(), low_pressure.clone(), [7; 32])
            .await
            .unwrap();
    while recovery.next().await.unwrap().is_some() {}
    let retained = retained
        .into_iter()
        .filter(|id| *id != dropped)
        .collect::<Vec<_>>();
    journal
        .checkpoint(b"second".to_vec(), retained.clone())
        .await
        .unwrap();
    assert_eq!(journal.statistics().relocation_operations, 0);
    assert!(dir.path().join(relocated.name()).exists());
    journal.close().await.unwrap();
    drop(journal);
    let (journal, _, mut recovery) = SegmentedJournal::open(dir.path(), low_pressure, [7; 32])
        .await
        .unwrap();
    assert_eq!(recovery.checkpoint(), b"second");
    let mut seen = Vec::new();
    while let Some(record) = recovery.next().await.unwrap() {
        seen.push(record.id.unwrap());
    }
    assert_eq!(seen, retained);
    assert!(!seen.contains(&dropped));
    journal.close().await.unwrap();
}
