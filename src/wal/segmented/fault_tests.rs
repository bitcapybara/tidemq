use std::path::{Path, PathBuf};
use std::time::Duration;

use super::SegmentedJournal;
use crate::wal::JournalOptions;

const CHILD: &str = "wal::segmented::fault_tests::crash_boundary_child";
const IDENTITY: [u8; 32] = [42; 32];

pub(super) fn boundary(phase: &'static str) {
    let Ok(target) = std::env::var("TIDEMQ_WAL_TEST_PHASE") else {
        return;
    };
    if target != phase {
        return;
    }
    let arm = PathBuf::from(std::env::var_os("TIDEMQ_WAL_TEST_ARM").unwrap());
    if !arm.try_exists().unwrap() {
        return;
    }
    let ready = PathBuf::from(std::env::var_os("TIDEMQ_WAL_TEST_READY").unwrap());
    std::fs::write(&ready, phase).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(ready)
        .unwrap()
        .sync_all()
        .unwrap();
    loop {
        std::thread::park();
    }
}

fn options() -> JournalOptions {
    JournalOptions {
        max_record_bytes: 512,
        max_file_bytes: 64 * 1024,
        max_pending_bytes: 4096,
        group_commit_bytes: 512,
        segment_target_bytes: 1024,
        max_segments: 32,
        max_checkpoint_bytes: 4096,
        max_retained_records: 16,
        control_reserve_bytes: 8192,
        ..JournalOptions::default()
    }
}

fn relocation_options() -> JournalOptions {
    JournalOptions {
        max_record_bytes: 1024,
        max_file_bytes: 96 * 1024,
        max_pending_bytes: 4096,
        group_commit_bytes: 1024,
        segment_target_bytes: 4096,
        max_segments: 32,
        max_checkpoint_bytes: 4096,
        max_retained_records: 64,
        control_reserve_bytes: 60 * 1024,
        ..JournalOptions::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Only the parent fault test launches this process with its own private paths"]
async fn crash_boundary_child() {
    let path = PathBuf::from(std::env::var_os("TIDEMQ_WAL_TEST_PATH").unwrap());
    let arm = PathBuf::from(std::env::var_os("TIDEMQ_WAL_TEST_ARM").unwrap());
    let scenario = std::env::var("TIDEMQ_WAL_TEST_SCENARIO").unwrap();
    if scenario == "initialization" {
        tokio::fs::write(&arm, b"armed").await.unwrap();
    }
    let journal_options = if scenario == "relocation" {
        relocation_options()
    } else if scenario == "file-size" {
        JournalOptions {
            segment_target_bytes: 2048,
            ..options()
        }
    } else {
        options()
    };
    let (journal, _, _) = SegmentedJournal::open(&path, journal_options.clone(), IDENTITY)
        .await
        .unwrap();
    assert_ne!(
        scenario, "initialization",
        "Initialization did not reach its requested boundary"
    );
    if scenario == "relocation" {
        let mut ids = Vec::new();
        for value in 0..64 {
            let (id, durable) = journal.append_retained(vec![value; 900]).await.unwrap();
            durable.durable().await.unwrap();
            ids.push(id);
            if journal.statistics().current_file_bytes >= 32 * 1024 {
                break;
            }
        }
        assert!(journal.statistics().current_file_bytes >= 32 * 1024);
        let count_path = path.with_extension("acknowledged-count");
        tokio::fs::write(&count_path, ids.len().to_string())
            .await
            .unwrap();
        tokio::fs::OpenOptions::new()
            .write(true)
            .open(count_path)
            .await
            .unwrap()
            .sync_all()
            .await
            .unwrap();
        let retained = ids
            .into_iter()
            .enumerate()
            .filter_map(|(index, id)| (index % 3 != 0).then_some(id))
            .collect();
        tokio::fs::write(arm, b"armed").await.unwrap();
        journal
            .checkpoint(b"relocated-control".to_vec(), retained)
            .await
            .unwrap();
        panic!("The requested relocation boundary was not observed");
    }
    let (first, durable) = journal.append_retained(vec![1; 512]).await.unwrap();
    durable.durable().await.unwrap();
    if scenario == "file-size" {
        let inspection = super::inspect_segmented(&path, &journal_options, IDENTITY)
            .await
            .unwrap();
        let length = tokio::fs::metadata(path.join(inspection.active_segment))
            .await
            .unwrap()
            .len();
        let limit = libc::rlimit {
            rlim_cur: length + 64,
            rlim_max: length + 64,
        };
        // The limit affects only this disposable child and creates a real file write failure.
        unsafe {
            assert_ne!(libc::signal(libc::SIGXFSZ, libc::SIG_IGN), libc::SIG_ERR);
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &limit), 0);
        }
        let error = journal
            .append(vec![2; 512])
            .await
            .unwrap()
            .durable()
            .await
            .unwrap_err();
        assert!(matches!(error, crate::wal::Error::Writer { .. }));
        let stopped = match journal.append(b"control-after-error".to_vec()).await {
            Ok(pending) => pending.durable().await.is_err(),
            Err(_) => true,
        };
        assert!(
            stopped,
            "A failed file write must poison subsequent durability callbacks"
        );
        assert_eq!(journal.statistics().written_records, 1);
        tokio::fs::write(arm, b"armed").await.unwrap();
        boundary("file_size_error");
    } else if scenario == "rotation" {
        tokio::fs::write(arm, b"armed").await.unwrap();
        journal
            .append_retained(vec![2; 512])
            .await
            .unwrap()
            .1
            .durable()
            .await
            .unwrap();
    } else {
        assert_eq!(scenario, "checkpoint");
        let (second, durable) = journal.append_retained(vec![2; 512]).await.unwrap();
        durable.durable().await.unwrap();
        journal
            .checkpoint(b"old-control".to_vec(), vec![first, second])
            .await
            .unwrap();
        tokio::fs::write(arm, b"armed").await.unwrap();
        journal
            .checkpoint(b"new-control".to_vec(), vec![second])
            .await
            .unwrap();
    }
    panic!("The requested durable boundary was not observed");
}

#[cfg(unix)]
async fn kill_at_boundary(path: &Path, phase: &str, scenario: &str) {
    let ready = path.with_extension("ready");
    let arm = path.with_extension("armed");
    let log_path = path.with_extension("child.log");
    let output = std::fs::File::create(&log_path).unwrap();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .env("TIDEMQ_WAL_TEST_PATH", path)
        .env("TIDEMQ_WAL_TEST_READY", &ready)
        .env("TIDEMQ_WAL_TEST_ARM", arm)
        .env("TIDEMQ_WAL_TEST_PHASE", phase)
        .env("TIDEMQ_WAL_TEST_SCENARIO", scenario)
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let reached = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if tokio::fs::try_exists(&ready).await.unwrap() {
                assert_eq!(tokio::fs::read_to_string(&ready).await.unwrap(), phase);
                return true;
            }
            if child.try_wait().unwrap().is_some() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let status = if child.try_wait().unwrap().is_none() {
        child.kill().await.unwrap();
        child.wait().await.unwrap()
    } else {
        child.wait().await.unwrap()
    };
    assert!(
        matches!(reached, Ok(true)),
        "Child did not reach {phase}: {}",
        tokio::fs::read_to_string(log_path).await.unwrap()
    );
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(unix)]
async fn sigkill_at_each_publication_boundary_recovers_one_complete_authority() {
    let directory = std::env::temp_dir().join(format!(
        "tidemq-segment-boundaries-{}",
        tidemq_protocol::Id::random().unwrap()
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    let phases = [
        "segment_sync",
        "segment_directory_sync",
        "checkpoint_sync",
        "checkpoint_directory_sync",
        "manifest_temp_sync",
        "manifest_rename",
        "manifest_directory_sync",
        "gc_before_unlink",
        "gc_after_unlink",
        "gc_directory_sync",
    ];
    for phase in phases {
        let path = directory.join(phase);
        let rotation = phase.starts_with("segment_");
        kill_at_boundary(
            &path,
            phase,
            if rotation { "rotation" } else { "checkpoint" },
        )
        .await;
        let (journal, _, mut recovered) = SegmentedJournal::open(&path, options(), IDENTITY)
            .await
            .unwrap();
        let control = recovered.checkpoint().to_vec();
        let mut records = Vec::new();
        while let Some(record) = recovered.next().await.unwrap() {
            records.push(record.data);
        }
        if rotation {
            assert!(control.is_empty());
            assert!(
                records == vec![vec![1; 512]] || records == vec![vec![1; 512], vec![2; 512]],
                "An acknowledged record must survive {phase} exactly once"
            );
        } else if control == b"old-control" {
            assert_eq!(records, vec![vec![1; 512], vec![2; 512]], "{phase}");
        } else {
            assert_eq!(control, b"new-control", "{phase}");
            assert_eq!(records, vec![vec![2; 512]], "{phase}");
        }
        journal.close().await.unwrap();
    }
    tokio::fs::remove_dir_all(directory).await.unwrap();
}

async fn directory_contents(directory: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    let mut contents = std::collections::BTreeMap::new();
    let mut entries = tokio::fs::read_dir(directory).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        assert!(entry.file_type().await.unwrap().is_file());
        contents.insert(entry.path(), tokio::fs::read(entry.path()).await.unwrap());
    }
    contents
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(unix)]
async fn sigkill_during_initialization_never_reinterprets_unpublished_segments() {
    let directory = std::env::temp_dir().join(format!(
        "tidemq-segment-initialization-{}",
        tidemq_protocol::Id::random().unwrap()
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    for phase in [
        "segment_sync",
        "segment_directory_sync",
        "manifest_temp_sync",
        "manifest_rename",
        "manifest_directory_sync",
    ] {
        let path = directory.join(phase);
        kill_at_boundary(&path, phase, "initialization").await;
        if tokio::fs::try_exists(path.join("manifest.bin"))
            .await
            .unwrap()
        {
            let (journal, _, mut recovery) = SegmentedJournal::open(&path, options(), IDENTITY)
                .await
                .unwrap();
            assert!(recovery.checkpoint().is_empty());
            assert!(recovery.next().await.unwrap().is_none());
            journal.close().await.unwrap();
        } else {
            let before = directory_contents(&path).await;
            assert!(
                super::inspect_segmented(&path, &options(), IDENTITY)
                    .await
                    .is_err()
            );
            assert!(
                SegmentedJournal::open(&path, options(), IDENTITY)
                    .await
                    .is_err()
            );
            assert_eq!(directory_contents(&path).await, before);
        }
    }
    tokio::fs::remove_dir_all(directory).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(unix)]
async fn real_file_size_limit_never_acknowledges_the_failed_or_subsequent_write() {
    let directory = std::env::temp_dir().join(format!(
        "tidemq-segment-file-size-{}",
        tidemq_protocol::Id::random().unwrap()
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    let path = directory.join("journal");
    kill_at_boundary(&path, "file_size_error", "file-size").await;
    let options = JournalOptions {
        segment_target_bytes: 2048,
        ..options()
    };
    let (journal, report, mut recovery) = SegmentedJournal::open(&path, options, IDENTITY)
        .await
        .unwrap();
    assert!(report.incomplete_tail_bytes <= 64);
    assert_eq!(recovery.next().await.unwrap().unwrap().data, vec![1; 512]);
    assert!(recovery.next().await.unwrap().is_none());
    journal.close().await.unwrap();
    tokio::fs::remove_dir_all(directory).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(unix)]
async fn sigkill_during_bounded_relocation_keeps_acknowledged_payloads_and_checkpoint_authority() {
    let directory = std::env::temp_dir().join(format!(
        "tidemq-segment-relocation-{}",
        tidemq_protocol::Id::random().unwrap()
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    for phase in [
        "segment_sync",
        "segment_directory_sync",
        "relocation_copy_sync",
        "checkpoint_sync",
        "checkpoint_directory_sync",
        "manifest_temp_sync",
        "manifest_rename",
        "manifest_directory_sync",
        "gc_before_unlink",
        "gc_after_unlink",
        "gc_directory_sync",
    ] {
        let path = directory.join(phase);
        kill_at_boundary(&path, phase, "relocation").await;
        let count: u8 = tokio::fs::read_to_string(path.with_extension("acknowledged-count"))
            .await
            .unwrap()
            .parse()
            .unwrap();
        let (journal, _, mut recovery) =
            SegmentedJournal::open(&path, relocation_options(), IDENTITY)
                .await
                .unwrap();
        let control = recovery.checkpoint().to_vec();
        let mut actual = Vec::new();
        while let Some(record) = recovery.next().await.unwrap() {
            actual.push(record.data);
        }
        let expected = if control.is_empty() {
            (0..count).map(|value| vec![value; 900]).collect::<Vec<_>>()
        } else {
            assert_eq!(control, b"relocated-control");
            (0..count)
                .filter(|value| value % 3 != 0)
                .map(|value| vec![value; 900])
                .collect()
        };
        assert_eq!(actual, expected, "{phase}");
        journal.cleanup_orphans().await.unwrap();
        journal.close().await.unwrap();
    }
    tokio::fs::remove_dir_all(directory).await.unwrap();
}
