use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use tokio::io::AsyncReadExt;

use super::ConsoleConfig;
use crate::{Error, Result};

const MAX_MANIFEST_BYTES: u64 = 1_048_576;
const MAX_PUBLIC_FILES: usize = 4_096;

pub(super) struct StaticFiles {
    root: PathBuf,
    public: HashSet<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestEntry {
    file: String,
    #[serde(default)]
    is_entry: bool,
    #[serde(default)]
    css: Vec<String>,
    #[serde(default)]
    assets: Vec<String>,
    #[serde(default)]
    imports: Vec<String>,
    #[serde(default)]
    dynamic_imports: Vec<String>,
}

impl StaticFiles {
    pub(super) async fn load(directory: &Path) -> Result<Self> {
        let root = tokio::fs::canonicalize(directory)
            .await
            .map_err(|source| file_error("locate console static directory", directory, source))?;
        let manifest = read_bounded(&root.join(".vite/manifest.json"), MAX_MANIFEST_BYTES).await?;
        let manifest: BTreeMap<String, ManifestEntry> =
            serde_json::from_slice(&manifest).map_err(|source| Error::Json { source })?;
        if manifest.is_empty()
            || manifest.len() > MAX_PUBLIC_FILES
            || !manifest.values().any(|entry| entry.is_entry)
        {
            return Err(invalid(
                "console static manifest must contain a bounded entry build",
            ));
        }
        let mut files = Self {
            root,
            public: HashSet::from(["index.html".to_owned()]),
        };
        for entry in manifest.values() {
            files.public.insert(entry.file.clone());
            files.public.extend(entry.css.iter().cloned());
            files.public.extend(entry.assets.iter().cloned());
            for import in entry.imports.iter().chain(&entry.dynamic_imports) {
                if !manifest.contains_key(import) {
                    return Err(invalid(
                        "console static manifest references a missing chunk",
                    ));
                }
            }
            if files.public.len() > MAX_PUBLIC_FILES {
                return Err(invalid(
                    "console static build exceeds its file count budget",
                ));
            }
        }
        for name in &files.public {
            files.resolve(name).await?;
        }
        Ok(files)
    }

    pub(super) fn contains(&self, name: &str) -> bool {
        self.public.contains(name)
    }

    pub(super) async fn resolve(&self, name: &str) -> Result<PathBuf> {
        if !self.contains(name)
            || !Path::new(name).components().all(|part| {
                matches!(part, Component::Normal(value) if !value.to_string_lossy().starts_with('.'))
            })
            || name.contains(['\\', '%'])
        {
            return Err(invalid("console static file must be a public build asset"));
        }
        let path = self.root.join(name);
        let canonical = tokio::fs::canonicalize(&path)
            .await
            .map_err(|source| file_error("locate console build asset", &path, source))?;
        if !canonical.starts_with(&self.root) {
            return Err(invalid("console build asset escapes its static directory"));
        }
        let metadata = tokio::fs::metadata(&canonical)
            .await
            .map_err(|source| file_error("inspect console build asset", &path, source))?;
        if !metadata.is_file() || metadata.len() == 0 {
            return Err(invalid(
                "console build assets must be nonempty regular files",
            ));
        }
        Ok(canonical)
    }
}

pub(super) async fn validate_resources(config: &ConsoleConfig) -> Result<StaticFiles> {
    validate_key(&config.session.key_file).await?;
    validate_roots(&config.cluster.trust_roots).await?;
    StaticFiles::load(&config.static_dir).await
}

async fn validate_key(path: &Path) -> Result<()> {
    let key = tokio::fs::File::open(path)
        .await
        .map_err(|source| file_error("open console session key", path, source))?;
    let metadata = key
        .metadata()
        .await
        .map_err(|source| file_error("inspect console session key", path, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o137 != 0 {
            return Err(invalid(
                "console session key must restrict access to its owner and an optional read-only group",
            ));
        }
    }
    if !metadata.is_file() || metadata.len() != 32 {
        return Err(invalid(
            "console session key must be a regular file containing exactly 32 raw bytes",
        ));
    }
    let mut bytes = Vec::new();
    key.take(33)
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| file_error("read console session key", path, source))?;
    if bytes.len() != 32 {
        return Err(invalid(
            "console session key must contain exactly 32 raw bytes",
        ));
    }
    Ok(())
}

async fn validate_roots(path: &Path) -> Result<()> {
    let roots = read_bounded(path, 65_536).await?;
    let certificates = reqwest::Certificate::from_pem_bundle(&roots).map_err(|source| {
        file_error(
            "parse console trust roots",
            path,
            std::io::Error::other(source),
        )
    })?;
    if certificates.is_empty() {
        return Err(invalid(
            "console cluster.trust_roots must contain a PEM certificate",
        ));
    }
    reqwest::Client::builder()
        .tls_certs_only(certificates)
        .build()
        .map_err(|source| {
            file_error(
                "validate console trust roots",
                path,
                std::io::Error::other(source),
            )
        })?;
    Ok(())
}

async fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| file_error("open console resource", path, source))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|source| file_error("inspect console resource", path, source))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(invalid("console resource must be a bounded regular file"));
    }
    let mut bytes = Vec::new();
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| file_error("read console resource", path, source))?;
    if bytes.len() as u64 > maximum {
        return Err(invalid("console resource exceeds its byte budget"));
    }
    Ok(bytes)
}

fn invalid(reason: &'static str) -> Error {
    Error::Configuration { reason }
}

fn file_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}
