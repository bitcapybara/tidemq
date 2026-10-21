#[path = "s3_support/mod.rs"]
mod support;

use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use aws_sdk_s3::{config::Credentials, error::ProvideErrorMetadata, primitives::ByteStream};
use tidemq::{
    archive::{ColdReader, Pack, PackLimits},
    object_store::{Error, Limits, S3Store, Uploaded},
};
use tidemq_protocol::Id;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct LostResponseProxy {
    endpoint: String,
    dropped: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl LostResponseProxy {
    async fn start(target: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let target = target.strip_prefix("http://").unwrap().to_owned();
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let task = tokio::spawn(async move {
            let mut first = true;
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept(), if connections.len() < 8 => {
                        let (mut client, _) = accepted.unwrap();
                        let mut server = TcpStream::connect(&target).await.unwrap();
                        let lose = first;
                        first = false;
                        let observed = observed.clone();
                        connections.spawn(async move {
                            if lose {
                                let (mut client_read, _client_write) = client.into_split();
                                let (mut server_read, mut server_write) = server.into_split();
                                let forward = tokio::spawn(async move { tokio::io::copy(&mut client_read, &mut server_write).await });
                                let mut response = [0; 16384];
                                let count = server_read.read(&mut response).await.unwrap();
                                assert!(response[..count].starts_with(b"HTTP/1.1 200"));
                                observed.store(true, Ordering::Release);
                                forward.abort();
                                let _ = forward.await;
                            } else {
                                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                            }
                        });
                    }
                    finished = connections.join_next(), if !connections.is_empty() => { finished.unwrap().unwrap(); }
                }
            }
        });
        Self {
            endpoint,
            dropped,
            task,
        }
    }
}

impl Drop for LostResponseProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Fixture {
    process: Child,
    directory: PathBuf,
    endpoint: String,
    access: String,
    secret: String,
}

impl Fixture {
    async fn start() -> Self {
        let binary = std::env::var_os("TIDEMQ_TEST_RUSTFS_BIN")
            .expect("set TIDEMQ_TEST_RUSTFS_BIN to the verified RustFS 1.0.3 executable");
        let version = Command::new(&binary).arg("--version").output().unwrap();
        assert!(version.status.success());
        assert_eq!(
            String::from_utf8(version.stdout).unwrap().lines().next(),
            Some("rustfs 1.0.3")
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let directory = std::env::current_dir()
            .unwrap()
            .join("target/s3-rustfs")
            .join(Id::new_v7().unwrap().to_string());
        fs::create_dir_all(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::create_dir(directory.join("data")).unwrap();
        fs::create_dir(directory.join("logs")).unwrap();
        let access = format!("tmq{}", Id::random().unwrap().to_string().replace('-', ""));
        let secret = Id::random().unwrap().to_string().replace('-', "");
        fs::write(directory.join("access"), &access).unwrap();
        fs::write(directory.join("secret"), &secret).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in ["access", "secret"] {
                fs::set_permissions(directory.join(path), fs::Permissions::from_mode(0o600))
                    .unwrap();
            }
        }
        let log = fs::File::create(directory.join("server.log")).unwrap();
        let process = Command::new(&binary)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("RUSTFS_ACCESS_KEY_FILE", directory.join("access"))
            .env("RUSTFS_SECRET_KEY_FILE", directory.join("secret"))
            .env("RUSTFS_DURABILITY_MODE", "strict")
            .env("RUSTFS_NEW_BUCKET_DURABILITY_MODE", "inherit")
            .env("RUSTFS_CONSOLE_ENABLE", "false")
            .env("RUSTFS_OBS_ENDPOINT", "")
            .env("RUSTFS_LOG_LEVEL", "info")
            .env("RUSTFS_LOG_DIRECTORY", directory.join("logs"))
            .arg("server")
            .arg("--address")
            .arg(address.to_string())
            .arg("--region")
            .arg("us-east-1")
            .arg(directory.join("data"))
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let mut fixture = Self {
            process,
            directory,
            endpoint: format!("http://{address}"),
            access,
            secret,
        };
        for _ in 0..150 {
            assert!(
                fixture.process.try_wait().unwrap().is_none(),
                "isolated RustFS process exited during startup"
            );
            if let Ok(Ok(mut stream)) =
                tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(address)).await
            {
                stream.write_all(b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.unwrap();
                let mut response = [0; 1024];
                if let Ok(Ok(count)) =
                    tokio::time::timeout(Duration::from_millis(200), stream.read(&mut response))
                        .await
                    && response[..count].starts_with(b"HTTP/1.1 200")
                {
                    return fixture;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("isolated RustFS readiness timed out");
    }

    fn store(&self, pack: &Pack, limits: Limits, budget: tidemq::budget::MemoryBudget) -> S3Store {
        let mut config = support::config(&self.endpoint, pack.descriptor().identity.cluster_id);
        config.credentials =
            Credentials::new(&self.access, &self.secret, None, None, "TideMqRustFsTest");
        S3Store::new(config, limits, budget).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the verified RustFS 1.0.3 binary through TIDEMQ_TEST_RUSTFS_BIN"]
async fn actual_sdk_checks_immutable_ranges_corruption_multipart_and_abort() {
    let fixture = Fixture::start().await;
    let raw = support::raw_client(&fixture.endpoint, &fixture.access, &fixture.secret);
    raw.create_bucket()
        .bucket("tidemq-sdk-test")
        .send()
        .await
        .unwrap();
    let budget = support::budget();
    let pack = support::pack(&budget, 100, 5);
    let store = fixture.store(&pack, Limits::default(), budget.clone());
    assert_eq!(
        store.put_create_only(&pack).await.unwrap(),
        Uploaded::Created
    );
    assert_eq!(
        store.put_create_only(&pack).await.unwrap(),
        Uploaded::ExistingVerified
    );
    assert_eq!(
        store.head(pack.descriptor()).await.unwrap().total_bytes,
        pack.descriptor().total_bytes
    );
    let index = pack.descriptor().partitions.first().unwrap();
    let range = store
        .read_range(
            pack.descriptor(),
            index.byte_start,
            index.byte_end - index.byte_start,
        )
        .await
        .unwrap();
    assert_eq!(blake3::hash(&range).as_bytes(), &index.checksum);
    let reader = ColdReader::new(store.clone(), PackLimits::default(), 1024 * 1024, 4).unwrap();
    let records = reader
        .read_partition(pack.descriptor(), index.topic_id, index.partition_id)
        .await
        .unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records.first().unwrap().offset, 3);
    assert!(
        records
            .iter()
            .all(|record| record.key() == b"private-key" && record.body() == vec![b'x'; 100])
    );
    assert_eq!(
        reader
            .read_partition(pack.descriptor(), index.topic_id, index.partition_id)
            .await
            .unwrap(),
        records
    );
    assert!(reader.cached_bytes().await > 0);
    let mut foreign = pack.descriptor().clone();
    foreign.identity.cluster_id = Id::new_v7().unwrap();
    assert!(
        reader
            .read_partition(&foreign, index.topic_id, index.partition_id)
            .await
            .is_err()
    );
    foreign.identity = pack.descriptor().identity;
    foreign.identity.object_id = Id::new_v7().unwrap();
    assert!(
        reader
            .read_partition(&foreign, index.topic_id, index.partition_id)
            .await
            .is_err()
    );
    reader.clear().await;
    let bad = raw
        .put_object()
        .bucket("tidemq-sdk-test")
        .key("bad-checksum")
        .checksum_crc32("AAAAAA==")
        .body(ByteStream::from_static(b"checksum mismatch"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        bad.as_service_error().and_then(|error| error.code()),
        Some("BadDigest")
    );
    let key = store.object_key(pack.descriptor()).unwrap();
    let invalid_range = raw
        .get_object()
        .bucket("tidemq-sdk-test")
        .key(&key)
        .range("bytes=99999999-100000000")
        .send()
        .await
        .unwrap_err();
    assert_eq!(invalid_range.raw_response().unwrap().status().as_u16(), 416);
    let large = support::pack_in_cluster(
        &budget,
        pack.descriptor().identity.cluster_id,
        200 * 1024,
        60,
    );
    assert_eq!(
        store
            .start_multipart(large.clone())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap(),
        Uploaded::Created
    );
    store.verify_uploaded(large.descriptor()).await.unwrap();
    assert_eq!(
        store
            .start_multipart(large.clone())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap(),
        Uploaded::ExistingVerified
    );
    assert!(
        store
            .pending_uploads(None, 100)
            .await
            .unwrap()
            .uploads
            .is_empty()
    );
    let interrupted = support::pack_in_cluster(
        &budget,
        pack.descriptor().identity.cluster_id,
        200 * 1024,
        60,
    );
    let slow = fixture.store(
        &interrupted,
        Limits {
            bytes_per_second: 1024 * 1024,
            ..Limits::default()
        },
        budget.clone(),
    );
    let upload = slow.start_multipart(interrupted.clone()).await.unwrap();
    let interrupted_key = store.object_key(interrupted.descriptor()).unwrap();
    let mut part_observed = false;
    for _ in 0..100 {
        let pending = store.pending_uploads(None, 100).await.unwrap();
        if let Some(pending) = pending
            .uploads
            .iter()
            .find(|upload| upload.key == interrupted_key)
        {
            let parts = raw
                .list_parts()
                .bucket("tidemq-sdk-test")
                .key(&pending.key)
                .upload_id(&pending.upload_id)
                .send()
                .await
                .unwrap();
            if !parts.parts().is_empty() {
                part_observed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        part_observed,
        "multipart cancellation must follow an actual uploaded part"
    );
    upload.cancel().await.unwrap();
    assert!(
        store
            .pending_uploads(None, 100)
            .await
            .unwrap()
            .uploads
            .is_empty()
    );
    assert!(matches!(
        store.head(interrupted.descriptor()).await,
        Err(Error::Missing)
    ));
    let dropped = support::pack_in_cluster(&budget, pack.descriptor().identity.cluster_id, 100, 5);
    let before_drop = budget.available_data_bytes();
    drop(store.start_multipart(dropped.clone()).await.unwrap());
    for _ in 0..100 {
        if budget.available_data_bytes() == before_drop {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        budget.available_data_bytes(),
        before_drop,
        "dropped upload retains admission through cleanup"
    );
    assert!(
        store
            .pending_uploads(None, 100)
            .await
            .unwrap()
            .uploads
            .is_empty()
    );
    assert!(matches!(
        store.head(dropped.descriptor()).await,
        Err(Error::Missing)
    ));
    // Simulate a backend replacement preserving metadata; duplicate PUT must read the body.
    let head = raw
        .head_object()
        .bucket("tidemq-sdk-test")
        .key(&key)
        .send()
        .await
        .unwrap();
    let mut damaged = pack.bytes().to_vec();
    damaged[100] ^= 1;
    raw.put_object()
        .bucket("tidemq-sdk-test")
        .key(&key)
        .set_metadata(head.metadata().cloned())
        .body(ByteStream::from(damaged))
        .send()
        .await
        .unwrap();
    assert!(matches!(
        store.put_create_only(&pack).await,
        Err(Error::Conflict | Error::Corrupt { .. })
    ));
    let lost = support::pack_in_cluster(&budget, pack.descriptor().identity.cluster_id, 100, 5);
    let proxy = LostResponseProxy::start(&fixture.endpoint).await;
    let mut config = support::config(&proxy.endpoint, lost.descriptor().identity.cluster_id);
    config.credentials = Credentials::new(
        &fixture.access,
        &fixture.secret,
        None,
        None,
        "TideMqRustFsTest",
    );
    let fault_store = S3Store::new(config, Limits::default(), budget).unwrap();
    assert_eq!(
        fault_store.put_create_only(&lost).await.unwrap(),
        Uploaded::ExistingVerified
    );
    assert!(proxy.dropped.load(Ordering::Acquire));
    store.verify_uploaded(lost.descriptor()).await.unwrap();
    store.delete(lost.descriptor()).await.unwrap();
    store.delete(pack.descriptor()).await.unwrap();
    store.delete(pack.descriptor()).await.unwrap();
    assert!(matches!(
        store.head(pack.descriptor()).await,
        Err(Error::Missing)
    ));
    store.delete(large.descriptor()).await.unwrap();
    raw.delete_bucket()
        .bucket("tidemq-sdk-test")
        .send()
        .await
        .unwrap();
    fs::write("target/s3-sdk-evidence.json", "{\"rustfs\":\"1.0.3\",\"sdk\":\"aws-sdk-s3 1.152.0\",\"durability\":\"strict\",\"checks\":[\"conditional-put\",\"duplicate-body-reconciliation\",\"lost-put-response\",\"head\",\"range-206\",\"range-416\",\"bad-checksum\",\"cold-cache\",\"multipart-complete\",\"multipart-conditional-conflict\",\"multipart-cancel-abort\",\"delete\"],\"cloud_three_az\":false,\"hardware_power_loss\":false}\n").unwrap();
}
