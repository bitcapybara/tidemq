use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{net::UdpSocket, sync::watch, task::JoinHandle};

#[derive(Clone, Copy, Default)]
pub enum Fault {
    #[default]
    Pass,
    FirstNewPeer,
    AllNewPeers,
}

#[derive(Clone, Debug)]
pub struct Peer {
    pub address: SocketAddr,
    pub forwarded: u64,
    pub dropped: u64,
}

#[derive(Default)]
struct State {
    fault: Fault,
    known: BTreeSet<SocketAddr>,
    previous: BTreeSet<SocketAddr>,
    peers: Vec<Peer>,
    latest: Option<SocketAddr>,
}

impl State {
    fn forward(&mut self, address: SocketAddr) -> bool {
        // Completed HTTP clients can still have datagrams queued when the next fault is armed.
        if self.previous.contains(&address) {
            return false;
        }
        self.known.insert(address);
        let index = self.peers.iter().position(|peer| peer.address == address);
        let index = index.unwrap_or_else(|| {
            self.peers.push(Peer {
                address,
                forwarded: 0,
                dropped: 0,
            });
            self.peers.len() - 1
        });
        let drop = match self.fault {
            Fault::Pass => false,
            Fault::FirstNewPeer => index == 0,
            Fault::AllNewPeers => true,
        };
        let peer = self.peers.get_mut(index).unwrap();
        if drop {
            peer.dropped += 1;
        } else {
            peer.forwarded += 1;
            self.latest = Some(address);
        }
        !drop
    }
}

pub struct Relay {
    pub address: SocketAddr,
    state: Arc<Mutex<State>>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl Relay {
    pub async fn start(backend: SocketAddr) -> Self {
        let public = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let outgoing = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        outgoing.connect(backend).await.unwrap();
        let address = public.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(public, outgoing, state.clone(), stopped));
        Self {
            address,
            state,
            stop,
            task: Some(task),
        }
    }

    pub fn arm(&self, fault: Fault) {
        let mut state = self.state.lock().unwrap();
        state.previous = state.known.clone();
        state.peers.clear();
        state.latest = None;
        state.fault = fault;
    }

    pub fn peers(&self) -> Vec<Peer> {
        self.state.lock().unwrap().peers.clone()
    }

    pub async fn close(mut self) {
        let _ = self.stop.send(true);
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(1), &mut task).await {
                Ok(result) => result.unwrap().unwrap(),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    panic!("Console UDP relay did not stop within its deadline");
                }
            }
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn run(
    public: UdpSocket,
    outgoing: UdpSocket,
    state: Arc<Mutex<State>>,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut request = vec![0_u8; 65_535];
    let mut response = vec![0_u8; 65_535];
    loop {
        tokio::select! {
            result = stop.changed() => {
                if result.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
            received = public.recv_from(&mut request) => {
                let (length, peer) = received?;
                let forward = state.lock().unwrap().forward(peer);
                if forward {
                    outgoing.send(&request[..length]).await?;
                }
            }
            received = outgoing.recv(&mut response) => {
                let length = received?;
                let peer = state.lock().unwrap().latest;
                if let Some(peer) = peer {
                    public.send_to(&response[..length], peer).await?;
                }
            }
        }
    }
}
