#[path = "s3_support/mod.rs"]
mod support;

use std::time::Duration;

use tidemq::object_store::{Error, Limits, S3Store};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn fixture(
    headers: String,
    body: Vec<u8>,
    header_delay: Duration,
    body_delay: Duration,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 16384];
        let mut read = 0;
        loop {
            let count = socket.read(&mut request[read..]).await.unwrap();
            assert!(count > 0);
            read += count;
            if request[..read].windows(4).any(|value| value == b"\r\n\r\n") {
                break;
            }
            assert!(read < request.len());
        }
        tokio::time::sleep(header_delay).await;
        if socket.write_all(headers.as_bytes()).await.is_err() {
            return;
        }
        tokio::time::sleep(body_delay).await;
        let _ = socket.write_all(&body).await;
    });
    (format!("http://{address}"), task)
}

#[tokio::test]
async fn validates_status_range_checksum_and_whole_body_deadline() {
    let budget = support::budget();
    let pack = support::pack(&budget, 100, 2);
    for (status, corrupt, delay, expected) in [
        (200, false, Duration::ZERO, "status"),
        (206, true, Duration::ZERO, "checksum"),
        (206, false, Duration::from_secs(1), "body deadline"),
    ] {
        let mut body = pack.bytes().to_vec();
        if corrupt {
            body[100] ^= 1;
        }
        let (endpoint, task) = fixture(
            support::response_headers(&pack, status, 0, body.len()),
            body,
            Duration::ZERO,
            delay,
        )
        .await;
        let store = support::store(&endpoint, &pack, budget.clone(), Duration::from_millis(250));
        let result = store
            .read_range(pack.descriptor(), 0, pack.descriptor().total_bytes)
            .await;
        if expected == "body deadline" {
            assert!(matches!(
                result,
                Err(Error::Timeout {
                    operation: "get body"
                })
            ));
        } else {
            assert!(
                matches!(result, Err(Error::Corrupt { .. })),
                "expected {expected}, got {result:?}"
            );
        }
        task.abort();
    }
    let body = pack.bytes().to_vec();
    let (endpoint, task) = fixture(
        support::response_headers(&pack, 206, 0, body.len()),
        body,
        Duration::from_secs(1),
        Duration::ZERO,
    )
    .await;
    let store = support::store(&endpoint, &pack, budget, Duration::from_millis(250));
    assert!(matches!(
        store
            .read_range(pack.descriptor(), 0, pack.descriptor().total_bytes)
            .await,
        Err(Error::Timeout { .. })
    ));
    task.abort();
}

#[tokio::test]
async fn charges_received_clones_and_rejects_bad_ranges_before_io() {
    let budget = support::budget();
    let pack = support::pack(&budget, 100, 2);
    let before = budget.available_data_bytes();
    let body = pack.bytes().to_vec();
    let (endpoint, task) = fixture(
        support::response_headers(&pack, 206, 0, body.len()),
        body,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let store = support::store(&endpoint, &pack, budget.clone(), Duration::from_secs(3));
    assert!(matches!(
        store.read_range(pack.descriptor(), u64::MAX, 1).await,
        Err(Error::Range)
    ));
    assert!(matches!(
        store.read_range(pack.descriptor(), 0, 0).await,
        Err(Error::Range)
    ));
    let bytes = store
        .read_range(pack.descriptor(), 0, pack.descriptor().total_bytes)
        .await
        .unwrap();
    let clone = bytes.clone();
    drop(bytes);
    assert!(budget.available_data_bytes() < before);
    drop(clone);
    assert_eq!(budget.available_data_bytes(), before);
    task.await.unwrap();
}

#[tokio::test]
async fn concurrency_is_bounded_and_configuration_redacts_credentials() {
    let budget = support::budget();
    let pack = support::pack(&budget, 100, 2);
    let body = pack.bytes().to_vec();
    let (endpoint, task) = fixture(
        support::response_headers(&pack, 206, 0, body.len()),
        body,
        Duration::ZERO,
        Duration::from_millis(200),
    )
    .await;
    let config = support::config(&endpoint, pack.descriptor().identity.cluster_id);
    assert!(!format!("{config:?}").contains("test-secret-key"));
    let store = S3Store::new(
        config,
        Limits {
            max_in_flight: 1,
            ..Limits::default()
        },
        budget,
    )
    .unwrap();
    let first = store.clone();
    let descriptor = pack.descriptor().clone();
    let reader = tokio::spawn(async move {
        first
            .read_range(&descriptor, 0, descriptor.total_bytes)
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        store.head(pack.descriptor()).await,
        Err(Error::Overloaded)
    ));
    reader.await.unwrap().unwrap();
    task.await.unwrap();
    let mut registry = prometheus_client::registry::Registry::default();
    registry
        .sub_registry_with_prefix("s3")
        .register_collector(Box::new(store.metrics()));
    let mut output = String::new();
    prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();
    assert!(output.contains("s3_sdk_attempts_total 1"));
    assert!(!output.contains(&pack.descriptor().identity.object_id.to_string()));
}

#[tokio::test]
async fn saturated_data_memory_keeps_metadata_head_available() {
    let budget = support::budget();
    let pack = support::pack(&budget, 100, 2);
    let bytes = pack.bytes();
    let (endpoint, task) = fixture(
        support::response_headers(&pack, 200, 0, bytes.len()),
        bytes.to_vec(),
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let store = support::store(&endpoint, &pack, budget.clone(), Duration::from_secs(3));
    let available = u32::try_from(budget.available_data_bytes()).unwrap();
    let held = budget
        .try_reserve(tidemq_protocol::common::TrafficClass::Bulk, available)
        .unwrap();
    assert!(matches!(
        store.read_range(pack.descriptor(), 0, 1).await,
        Err(Error::Overloaded)
    ));
    assert!(store.head(pack.descriptor()).await.is_ok());
    assert_eq!(budget.available_data_bytes(), 0);
    drop(held);
    task.await.unwrap();
}

#[tokio::test]
async fn cached_reads_charge_each_record_index_until_its_views_are_dropped() {
    let budget = support::budget();
    let pack = support::pack(&budget, 100, 20);
    let before = budget.available_data_bytes();
    let index = pack.descriptor().partitions.first().unwrap();
    let body = pack
        .bytes()
        .get(index.byte_start as usize..index.byte_end as usize)
        .unwrap()
        .to_vec();
    let (endpoint, task) = fixture(
        support::response_headers(&pack, 206, index.byte_start as usize, body.len()),
        body,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let store = support::store(&endpoint, &pack, budget.clone(), Duration::from_secs(3));
    let reader = tidemq::archive::ColdReader::new(
        store,
        tidemq::archive::PackLimits::default(),
        1024 * 1024,
        4,
    )
    .unwrap();
    let first = reader
        .read_partition(pack.descriptor(), index.topic_id, index.partition_id)
        .await
        .unwrap();
    let after_first = budget.available_data_bytes();
    let second = reader
        .read_partition(pack.descriptor(), index.topic_id, index.partition_id)
        .await
        .unwrap();
    assert!(budget.available_data_bytes() < after_first);
    let retained = second.first().unwrap().clone();
    drop(first);
    drop(second);
    reader.clear().await;
    assert!(budget.available_data_bytes() < before);
    drop(retained);
    assert_eq!(budget.available_data_bytes(), before);
    task.await.unwrap();
}

#[tokio::test]
async fn decoder_admission_is_shared_across_cold_reader_instances() {
    let budget = support::budget();
    let pack = support::pack(&budget, 100, 20);
    let before = budget.available_data_bytes();
    let index = pack.descriptor().partitions.first().unwrap();
    let body = pack
        .bytes()
        .get(index.byte_start as usize..index.byte_end as usize)
        .unwrap()
        .to_vec();
    let (endpoint, task) = fixture(
        support::response_headers(&pack, 206, index.byte_start as usize, body.len()),
        body,
        Duration::ZERO,
        Duration::from_millis(500),
    )
    .await;
    let store = S3Store::new(
        support::config(&endpoint, pack.descriptor().identity.cluster_id),
        Limits {
            max_in_flight: 1,
            ..Limits::default()
        },
        budget.clone(),
    )
    .unwrap();
    let first = tidemq::archive::ColdReader::new(
        store.clone(),
        tidemq::archive::PackLimits::default(),
        0,
        0,
    )
    .unwrap();
    let second =
        tidemq::archive::ColdReader::new(store, tidemq::archive::PackLimits::default(), 0, 0)
            .unwrap();
    let descriptor = pack.descriptor().clone();
    let (topic, partition) = (index.topic_id, index.partition_id);
    let reading =
        tokio::spawn(async move { first.read_partition(&descriptor, topic, partition).await });
    for _ in 0..100 {
        if budget.available_data_bytes() < before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(budget.available_data_bytes() < before);
    assert!(matches!(
        second
            .read_partition(pack.descriptor(), topic, partition)
            .await,
        Err(tidemq::archive::Error::Overloaded)
    ));
    reading.await.unwrap().unwrap();
    task.await.unwrap();
    assert_eq!(budget.available_data_bytes(), before);
}
