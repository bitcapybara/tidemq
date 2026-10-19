use aws_sdk_s3::config::Credentials;
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tidemq::object_store::{Limits, S3Store};
use tidemq_protocol::Id;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
pub struct RustFs {
    process: Child,
    directory: PathBuf,
    endpoint: String,
    access: String,
    secret: String,
}

impl RustFs {
    pub async fn start() -> Self {
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
            .join(Id::random().unwrap().to_string());
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

    pub fn store(
        &self,
        cluster_id: Id,
        limits: Limits,
        budget: tidemq::budget::MemoryBudget,
    ) -> S3Store {
        let mut config = super::s3::config(&self.endpoint, cluster_id);
        config.credentials =
            Credentials::new(&self.access, &self.secret, None, None, "TideMqRustFsTest");
        S3Store::new(config, limits, budget).unwrap()
    }
}

impl RustFs {
    pub async fn create_bucket(&self) {
        super::s3::raw_client(&self.endpoint, &self.access, &self.secret)
            .create_bucket()
            .bucket("tidemq-sdk-test")
            .send()
            .await
            .unwrap();
    }
}

impl Drop for RustFs {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = fs::remove_dir_all(&self.directory);
    }
}
