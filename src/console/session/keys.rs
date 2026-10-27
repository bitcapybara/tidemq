use std::collections::BTreeSet;
use std::path::Path;

use aws_lc_rs::aead::{AES_256_GCM, RandomizedNonceKey};
use tokio::io::AsyncReadExt;
use zeroize::Zeroizing;

use crate::console::SessionConfig;
use crate::{Error, Result};

pub(super) async fn load(
    config: &SessionConfig,
) -> Result<(RandomizedNonceKey, Vec<RandomizedNonceKey>)> {
    let current = read(&config.key_file).await?;
    let mut seen = BTreeSet::from([*blake3::hash(current.as_slice()).as_bytes()]);
    let current = key(current.as_slice())?;
    let mut previous = Vec::new();
    for file in &config.previous_key_files {
        let bytes = read(file).await?;
        if !seen.insert(*blake3::hash(bytes.as_slice()).as_bytes()) {
            return Err(Error::Configuration {
                reason: "console session keys must contain distinct key material",
            });
        }
        previous.push(key(bytes.as_slice())?);
    }
    Ok((current, previous))
}

fn key(bytes: &[u8]) -> Result<RandomizedNonceKey> {
    RandomizedNonceKey::new(&AES_256_GCM, bytes).map_err(|_| Error::Configuration {
        reason: "console session encryption key could not be initialized",
    })
}

async fn read(path: &Path) -> Result<Zeroizing<Vec<u8>>> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|source| io_error("inspect console session key", path, source))?;
    if !metadata.is_file() {
        return Err(Error::Configuration {
            reason: "console session keys must use regular files",
        });
    }
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| io_error("open console session key", path, source))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|source| io_error("inspect console session key", path, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o137 != 0 {
            return Err(Error::Configuration {
                reason: "console session keys must restrict access to their owner and an optional read-only group",
            });
        }
    }
    if !metadata.is_file() || metadata.len() != 32 {
        return Err(Error::Configuration {
            reason: "console session keys must contain exactly 32 raw bytes in regular files",
        });
    }
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(33)
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| io_error("read console session key", path, source))?;
    if bytes.len() != 32 {
        return Err(Error::Configuration {
            reason: "console session keys must contain exactly 32 raw bytes",
        });
    }
    Ok(bytes)
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}
