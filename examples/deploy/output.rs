use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tokio::{io::AsyncWriteExt, time::Instant};

pub struct Output(PathBuf);

impl Output {
    pub async fn create(path: &Path) -> Result<Self> {
        let mut builder = tokio::fs::DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        #[cfg(not(unix))]
        anyhow::bail!("Private deployment validation output requires Unix permissions");
        builder
            .create(path)
            .await
            .context("Create a new private output directory")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                tokio::fs::metadata(path).await?.permissions().mode() & 0o777 == 0o700,
                "Output directory must have mode 0700"
            );
        }
        Ok(Self(path.into()))
    }

    pub async fn write(&self, name: &'static str, value: &impl Serialize) -> Result<()> {
        ensure!(
            matches!(
                name,
                "waiting-fault.json" | "fault-observed.json" | "report.json"
            ),
            "Invalid output artifact name"
        );
        let mut bytes = serde_json::to_vec_pretty(value)?;
        ensure!(
            bytes.len() < 65_536,
            "JSON artifact exceeded its 64 KiB bound"
        );
        bytes.push(b'\n');
        let temporary = self.0.join(format!(".{name}.partial"));
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .await
            .context("Create private JSON artifact")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                file.metadata().await?.permissions().mode() & 0o777 == 0o600,
                "JSON artifacts must have mode 0600"
            );
        }
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        // Publishing a completed inode is atomic and never overwrites a host-created file.
        tokio::fs::hard_link(&temporary, self.0.join(name)).await?;
        tokio::fs::remove_file(temporary).await?;
        tokio::fs::File::open(&self.0).await?.sync_all().await?;
        Ok(())
    }

    pub async fn absent(&self, name: &'static str) -> Result<()> {
        ensure!(
            !self.marker(name).await?,
            "Host marker arrived before its authorized stage"
        );
        Ok(())
    }

    async fn marker(&self, name: &'static str) -> Result<bool> {
        ensure!(
            matches!(name, "fault-injected" | "recovered"),
            "Invalid host marker name"
        );
        match tokio::fs::symlink_metadata(self.0.join(name)).await {
            Ok(metadata) => {
                ensure!(
                    metadata.is_file() && metadata.len() <= 4096,
                    "Host marker must be a regular file no larger than 4 KiB"
                );
                Ok(true)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).context("Inspect host marker"),
        }
    }

    pub async fn wait(&self, name: &'static str, deadline: Instant) -> Result<()> {
        let until = deadline.min(
            Instant::now()
                .checked_add(Duration::from_secs(120))
                .context("Host wait deadline overflow")?,
        );
        loop {
            ensure!(
                Instant::now() < until,
                "Host marker wait exceeded its original deadline"
            );
            if self.marker(name).await? {
                ensure!(
                    Instant::now() < until,
                    "Host marker arrived after the original wait deadline"
                );
                return Ok(());
            }
            ensure!(
                Instant::now() < until,
                "Host marker wait exceeded its original deadline"
            );
            let next = Instant::now()
                .checked_add(Duration::from_millis(100))
                .context("Host poll deadline overflow")?;
            tokio::time::sleep_until(next.min(until)).await;
        }
    }
}

pub fn unix_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("System clock precedes the Unix epoch")?
            .as_millis(),
    )?)
}

pub fn request_deadline(total: Instant) -> Result<Instant> {
    ensure!(Instant::now() < total, "Validation work deadline exceeded");
    Ok(total.min(
        Instant::now()
            .checked_add(Duration::from_secs(10))
            .context("Request deadline overflow")?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn private_atomic_outputs_reject_reuse_and_oversized_or_symlink_markers() {
        let path = std::env::temp_dir().join(format!(
            "tidemq-deploy-output-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        let output = Output::create(&path).await.unwrap();
        assert!(Output::create(&path).await.is_err());
        output
            .write("waiting-fault.json", &serde_json::json!({"ready": true}))
            .await
            .unwrap();
        assert!(
            output
                .write("waiting-fault.json", &serde_json::json!({}))
                .await
                .is_err()
        );
        output.absent("fault-injected").await.unwrap();
        assert!(output.wait("fault-injected", Instant::now()).await.is_err());
        tokio::fs::write(path.join("fault-injected"), [])
            .await
            .unwrap();
        output
            .wait("fault-injected", Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(output.absent("fault-injected").await.is_err());
        assert!(
            output
                .write("report.json", &"x".repeat(65_536))
                .await
                .is_err()
        );
        tokio::fs::write(path.join("fault-injected"), vec![0; 4097])
            .await
            .unwrap();
        assert!(output.marker("fault-injected").await.is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                tokio::fs::metadata(&path)
                    .await
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                tokio::fs::metadata(path.join("waiting-fault.json"))
                    .await
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            std::os::unix::fs::symlink("waiting-fault.json", path.join("recovered")).unwrap();
            assert!(output.marker("recovered").await.is_err());
        }
        tokio::fs::remove_dir_all(path).await.unwrap();
    }
}
