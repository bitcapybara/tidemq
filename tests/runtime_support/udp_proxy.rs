use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Default)]
struct Counters {
    packets: AtomicU64,
    lost: AtomicU64,
    reordered: AtomicU64,
    over_mtu: AtomicU64,
}

#[derive(Debug)]
pub struct Snapshot {
    pub packets: u64,
    pub lost: u64,
    pub reordered: u64,
    pub over_mtu: u64,
}

struct Mtu {
    request: AtomicUsize,
    response: AtomicUsize,
}

pub struct Proxy {
    pub address: SocketAddr,
    backend: Arc<AtomicUsize>,
    counters: Arc<Counters>,
    mtu: Arc<Mtu>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

struct Pending {
    bytes: Vec<u8>,
    target: SocketAddr,
    since: Instant,
}

impl Proxy {
    /// A single SDK endpoint, two fixed backends, and one retained datagram in each direction.
    pub async fn start(backends: [SocketAddr; 2]) -> Self {
        let public = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let outgoing = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = public.local_addr().unwrap();
        let backend = Arc::new(AtomicUsize::new(0));
        let counters = Arc::new(Counters::default());
        let mtu = Arc::new(Mtu {
            request: AtomicUsize::new(1200),
            response: AtomicUsize::new(1200),
        });
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(
            public,
            outgoing,
            backends,
            backend.clone(),
            counters.clone(),
            mtu.clone(),
            stopped,
        ));
        Self {
            address,
            backend,
            counters,
            mtu,
            stop,
            task: Some(task),
        }
    }

    pub fn set_mtu(&self, mtu: usize) {
        self.mtu.request.store(mtu, Ordering::Release);
        self.mtu.response.store(mtu, Ordering::Release);
    }

    pub fn set_response_mtu(&self, mtu: usize) {
        self.mtu.response.store(mtu, Ordering::Release);
    }

    pub fn switch(&self) {
        self.backend.store(1, Ordering::Release);
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            packets: self.counters.packets.load(Ordering::Relaxed),
            lost: self.counters.lost.load(Ordering::Relaxed),
            reordered: self.counters.reordered.load(Ordering::Relaxed),
            over_mtu: self.counters.over_mtu.load(Ordering::Relaxed),
        }
    }

    pub async fn close(mut self) {
        let _ = self.stop.send(true);
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(1), &mut task).await {
                Ok(result) => result.unwrap().unwrap(),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    panic!("Bounded UDP proxy did not stop within its deadline");
                }
            }
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn run(
    public: UdpSocket,
    outgoing: UdpSocket,
    backends: [SocketAddr; 2],
    backend: Arc<AtomicUsize>,
    counters: Arc<Counters>,
    mtu: Arc<Mtu>,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut request = vec![0_u8; 65_536];
    let mut response = vec![0_u8; 65_536];
    let mut client = None;
    let mut pending_request = None;
    let mut pending_response = None;
    let mut timer = tokio::time::interval(Duration::from_millis(2));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = stop.changed() => return Ok(()),
            received = public.recv_from(&mut request) => {
                let (length, address) = received?;
                if client.is_some_and(|client| client != address) { continue; }
                client = Some(address);
                let target = backends[backend.load(Ordering::Acquire)];
                relay(&outgoing, &request[..length], target, &mut pending_request, &counters, mtu.request.load(Ordering::Acquire)).await?;
            }
            received = outgoing.recv_from(&mut response) => {
                let (length, address) = received?;
                if !backends.contains(&address) { continue; }
                if let Some(client) = client {
                    relay(&public, &response[..length], client, &mut pending_response, &counters, mtu.response.load(Ordering::Acquire)).await?;
                }
            }
            _ = timer.tick() => {
                flush(&outgoing, &mut pending_request).await?;
                flush(&public, &mut pending_response).await?;
            }
        }
    }
}

async fn relay(
    socket: &UdpSocket,
    bytes: &[u8],
    target: SocketAddr,
    pending: &mut Option<Pending>,
    counters: &Counters,
    mtu: usize,
) -> std::io::Result<()> {
    let number = counters.packets.fetch_add(1, Ordering::Relaxed) + 1;
    if bytes.len() > mtu {
        counters.over_mtu.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    if number.is_multiple_of(17) {
        counters.lost.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    if let Some(previous) = pending.take() {
        socket.send_to(bytes, target).await?;
        socket.send_to(&previous.bytes, previous.target).await?;
        counters.reordered.fetch_add(1, Ordering::Relaxed);
    } else if number.is_multiple_of(11) {
        *pending = Some(Pending {
            bytes: bytes.to_vec(),
            target,
            since: Instant::now(),
        });
    } else {
        socket.send_to(bytes, target).await?;
    }
    Ok(())
}

async fn flush(socket: &UdpSocket, pending: &mut Option<Pending>) -> std::io::Result<()> {
    if pending
        .as_ref()
        .is_some_and(|pending| pending.since.elapsed() >= Duration::from_millis(5))
        && let Some(pending) = pending.take()
    {
        socket.send_to(&pending.bytes, pending.target).await?;
    }
    Ok(())
}
