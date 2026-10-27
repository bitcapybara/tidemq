use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

pub const HEALTHY: u8 = 0;
pub const SLOW: u8 = 1;
pub const FAIL: u8 = 2;
pub const LARGE: u8 = 3;
pub const CHUNKED_LARGE: u8 = 4;

#[derive(Clone)]
pub struct Recorded {
    pub at: Instant,
    pub bytes: Bytes,
    pub instance: String,
}

pub struct Collector {
    pub address: SocketAddr,
    pub mode: Arc<AtomicU8>,
    pub requests: Arc<Mutex<Vec<Recorded>>>,
    task: Option<JoinHandle<()>>,
}

impl Collector {
    pub async fn new() -> Self {
        Self::bind("127.0.0.1:0".parse().unwrap()).await
    }
    pub async fn bind(address: SocketAddr) -> Self {
        let listener = TcpListener::bind(address).await.unwrap();
        let address = listener.local_addr().unwrap();
        let mode = Arc::new(AtomicU8::new(HEALTHY));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn(accept(listener, mode.clone(), requests.clone()));
        Self {
            address,
            mode,
            requests,
            task: Some(task),
        }
    }
    pub fn endpoint(&self) -> String {
        format!("http://{}/v1/traces", self.address)
    }
    pub fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::Relaxed);
    }
    pub fn spans(&self) -> Vec<Span> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .flat_map(|record| {
                ExportTraceServiceRequest::decode(record.bytes.clone())
                    .unwrap()
                    .resource_spans
            })
            .flat_map(|resource| resource.scope_spans)
            .flat_map(|scope| scope.spans)
            .collect()
    }
    pub fn assert_rate(&self, minimum: Duration) {
        let mut last = std::collections::BTreeMap::new();
        for record in self.requests.lock().unwrap().iter() {
            if let Some(previous) = last.insert(record.instance.clone(), record.at) {
                assert!(
                    record.at.duration_since(previous) >= minimum,
                    "One exporter exceeded its request interval"
                );
            }
        }
    }
    pub async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn accept(listener: TcpListener, mode: Arc<AtomicU8>, records: Arc<Mutex<Vec<Recorded>>>) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            result = listener.accept(), if tasks.len() < 16 => {
                let Ok((stream, _)) = result else { break; };
                tasks.spawn(serve(stream, mode.clone(), records.clone()));
            }
            _ = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
}

async fn serve(mut stream: TcpStream, mode: Arc<AtomicU8>, records: Arc<Mutex<Vec<Recorded>>>) {
    let request = tokio::time::timeout(Duration::from_secs(2), read(&mut stream))
        .await
        .unwrap();
    let Some(bytes) = request else {
        return;
    };
    {
        let mut records = records.lock().unwrap();
        let retained_bytes: usize = records.iter().map(|record| record.bytes.len()).sum();
        // Preserve the old maximum payload budget without dropping later business traces.
        assert!(
            retained_bytes.saturating_add(bytes.len()) <= 128 * 65_536,
            "Trace collector exceeded its retained payload budget"
        );
        let decoded = ExportTraceServiceRequest::decode(bytes.clone()).unwrap();
        let instance = decoded
            .resource_spans
            .first()
            .unwrap()
            .resource
            .as_ref()
            .unwrap()
            .attributes
            .iter()
            .find_map(|attribute| {
                match attribute
                    .value
                    .as_ref()
                    .and_then(|value| value.value.as_ref())
                {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            value,
                        ),
                    ) if attribute.key == "service.instance.id" => Some(value.clone()),
                    _ => None,
                }
            })
            .expect("Exporter resource must identify its one process instance");
        assert!(instance.len() <= 64);
        records.push(Recorded {
            at: Instant::now(),
            bytes,
            instance,
        });
    }
    let mode = mode.load(Ordering::Relaxed);
    if mode == SLOW {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let response: &[u8] = match mode {
        FAIL => b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 3600\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        LARGE => b"HTTP/1.1 200 OK\r\nContent-Length: 8192\r\nConnection: close\r\n\r\n",
        CHUNKED_LARGE => b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        _ => b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    };
    let _ = stream.write_all(response).await;
    if mode == LARGE {
        let _ = stream.write_all(&[42; 8192]).await;
    }
    if mode == CHUNKED_LARGE {
        let _ = stream.write_all(b"2000\r\n").await;
        let _ = stream.write_all(&[42; 8192]).await;
        let _ = stream.write_all(b"\r\n0\r\n\r\n").await;
    }
}

async fn read(stream: &mut TcpStream) -> Option<Bytes> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0_u8; 1];
        if header.len() >= 4096 || stream.read_exact(&mut byte).await.is_err() {
            return None;
        }
        header.push(byte[0]);
    }
    let header = std::str::from_utf8(&header).unwrap().to_ascii_lowercase();
    assert!(header.starts_with("post /v1/traces http/1.1\r\n"));
    assert!(header.contains("content-type: application/x-protobuf"));
    assert!(!header.contains("authorization:"));
    let length = header
        .lines()
        .find_map(|line| {
            line.strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    assert!(length <= 65_536);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.ok()?;
    Some(body.into())
}

pub async fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(8);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "Trace condition exceeded its deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
