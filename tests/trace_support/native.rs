use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

pub struct Native {
    process: tokio::process::Child,
    directory: PathBuf,
    pub receiver: SocketAddr,
    pub metrics: SocketAddr,
}

impl Native {
    pub async fn start() -> Self {
        let binary = std::env::var_os("TIDEMQ_OTELCOL_BIN")
            .expect("Set TIDEMQ_OTELCOL_BIN to the verified official core 0.162.0 binary");
        let binary = PathBuf::from(binary);
        assert!(binary.is_absolute());
        let version = tokio::process::Command::new(&binary)
            .arg("--version")
            .output()
            .await
            .unwrap();
        assert!(version.status.success());
        assert!(
            String::from_utf8(version.stdout)
                .unwrap()
                .contains("0.162.0")
        );
        let receiver = address();
        let metrics = address();
        let directory = std::env::temp_dir().join(format!(
            "tidemq-collector-{}",
            tidemq_protocol::Id::random().unwrap()
        ));
        tokio::fs::create_dir(&directory).await.unwrap();
        let source = include_str!("../../deploy/observability/collector.yaml")
            .replace("127.0.0.1:4318", &receiver.to_string())
            .replace("port: 18888", &format!("port: {}", metrics.port()));
        let path = directory.join("collector.yaml");
        tokio::fs::write(&path, source).await.unwrap();
        assert!(
            tokio::process::Command::new(&binary)
                .args(["validate", "--config"])
                .arg(&path)
                .status()
                .await
                .unwrap()
                .success()
        );
        let process = tokio::process::Command::new(&binary)
            .env_clear()
            .env("GOMAXPROCS", "2")
            .env("GOMEMLIMIT", "192MiB")
            .args(["--config"])
            .arg(path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut native = Self {
            process,
            directory,
            receiver,
            metrics,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(native.process.try_wait().unwrap().is_none());
            if native.scrape().await.is_some() {
                break;
            }
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        native
    }

    pub async fn scrape(&self) -> Option<String> {
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut stream = tokio::net::TcpStream::connect(self.metrics).await.ok()?;
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .ok()?;
            let mut bytes = String::new();
            stream.take(262_144).read_to_string(&mut bytes).await.ok()?;
            Some(bytes)
        })
        .await
        .ok()
        .flatten()
    }

    pub async fn close(&mut self) {
        let pid = i32::try_from(self.process.id().unwrap()).unwrap();
        // SIGINT exercises the official Collector's normal shutdown path.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), self.process.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        for address in [self.receiver, self.metrics] {
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_reuseaddr(true).unwrap();
            socket.bind(address).unwrap();
        }
        tokio::fs::remove_dir_all(&self.directory).await.unwrap();
    }
}

fn address() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
