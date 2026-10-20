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
