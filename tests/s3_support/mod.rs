// Each integration target uses a different subset of these shared fixture helpers.
#![allow(dead_code)]

use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use bytes::Bytes;
use tidemq::{
    archive::{CommittedPartition, Pack, PackIdentity, PackLimits, Record},
    budget::MemoryBudget,
    object_store::{Limits, S3Config, S3Store},
};
use tidemq_protocol::{Id, state::OffsetRange};
use tidemq_transport::budget::MemoryLimits;

pub fn budget() -> MemoryBudget {
    MemoryBudget::new(MemoryLimits {
        total_bytes: 256 * 1024 * 1024,
        control_reserved_bytes: 8 * 1024 * 1024,
    })
    .unwrap()
}

pub fn pack(budget: &MemoryBudget, record_bytes: usize, count: usize) -> Pack {
    pack_in_cluster(budget, Id::random().unwrap(), record_bytes, count)
}

pub fn pack_in_cluster(
    budget: &MemoryBudget,
    cluster_id: Id,
    record_bytes: usize,
    count: usize,
) -> Pack {
    let records: Vec<_> = (0..count)
        .map(|offset| Record {
            offset: offset as u64 + 3,
            key: Bytes::from_static(b"private-key"),
            body: Bytes::from(vec![b'x'; record_bytes]),
        })
        .collect();
    let identity = PackIdentity {
        cluster_id,
        group_id: 9,
        object_id: Id::random().unwrap(),
    };
    Pack::build(
        identity,
        &[CommittedPartition {
            group_id: 9,
            topic_id: Id::random().unwrap(),
            partition_id: 4,
            offsets: OffsetRange::new(3, 3 + count as u64).unwrap(),
            committed_end: 3 + count as u64,
            records: &records,
        }],
        PackLimits::default(),
        budget,
    )
    .unwrap()
}

pub fn config(endpoint: &str, cluster_id: Id) -> S3Config {
    S3Config {
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        bucket: "tidemq-sdk-test".into(),
        prefix: "tidemq-test".into(),
        cluster_id,
        credentials: Credentials::new(
            "test-access-key",
            "test-secret-key",
            None,
            None,
            "TideMqTest",
        ),
        allow_http_loopback: true,
    }
}

pub fn store(endpoint: &str, pack: &Pack, budget: MemoryBudget, timeout: Duration) -> S3Store {
    S3Store::new(
        config(endpoint, pack.descriptor().identity.cluster_id),
        Limits {
            request_timeout: timeout,
            ..Limits::default()
        },
        budget,
    )
    .unwrap()
}

pub fn raw_client(endpoint: &str, access: &str, secret: &str) -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(access, secret, None, None, "TideMqTest"))
            .force_path_style(true)
            .build(),
    )
}

pub fn response_headers(pack: &Pack, status: u16, start: usize, length: usize) -> String {
    let descriptor = pack.descriptor();
    format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {length}\r\nContent-Range: bytes {start}-{}/{}\r\nx-amz-meta-tidemq-format: pack-v1\r\nx-amz-meta-tidemq-cluster: {}\r\nx-amz-meta-tidemq-group: {}\r\nx-amz-meta-tidemq-object: {}\r\nx-amz-meta-tidemq-blake3: {}\r\nConnection: close\r\n\r\n",
        start + length - 1,
        descriptor.total_bytes,
        descriptor.identity.cluster_id,
        descriptor.identity.group_id,
        descriptor.identity.object_id,
        blake3::Hash::from_bytes(descriptor.checksum).to_hex()
    )
}
