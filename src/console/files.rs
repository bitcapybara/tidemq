use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use tokio::io::AsyncReadExt;

use super::ConsoleConfig;
use crate::{Error, Result};

const MAX_MANIFEST_BYTES: u64 = 1_048_576;
const MAX_PUBLIC_FILES: usize = 4_096;

#[derive(Clone)]
pub(super) struct StaticFiles {
    root: PathBuf,
    public: HashSet<String>,
    required: Arc<BTreeMap<String, Asset>>,
}

#[derive(Clone)]
pub(super) struct Asset {
    pub id: usize,
    pub name: String,
    pub length: u64,
    pub digest: blake3::Hash,
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
            required: Arc::default(),
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
        let index = read_bounded(&files.root.join("index.html"), MAX_MANIFEST_BYTES).await?;
        let index = std::str::from_utf8(&index)
            .map_err(|_| invalid("console entry page must be valid UTF-8"))?;
        let mut required = required_assets(&manifest)?;
        required.extend(html_assets(index)?);
        let mut assets = BTreeMap::new();
        for (id, name) in required.into_iter().enumerate() {
            let path = files.resolve(&name).await?;
            let (length, digest) = fingerprint(&path).await?;
            assets.insert(
                name.clone(),
                Asset {
                    id,
                    name,
                    length,
                    digest,
                },
            );
        }
        files.required = Arc::new(assets);
        Ok(files)
    }

    pub(super) fn contains(&self, name: &str) -> bool {
        self.public.contains(name)
    }

    pub(super) fn required(&self, name: &str) -> Option<&Asset> {
        self.required.get(name)
    }

    pub(super) fn required_count(&self) -> usize {
        self.required.len()
    }

    pub(super) async fn recheck(&self, asset: &Asset) -> Result<bool> {
        let path = self.resolve(&asset.name).await?;
        let (length, digest) = fingerprint(&path).await?;
        Ok(length == asset.length && digest == asset.digest)
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
        if self
            .required(name)
            .is_some_and(|asset| asset.length != metadata.len())
        {
            return Err(invalid("console build asset changed after startup"));
        }
        Ok(canonical)
    }
}

fn required_assets(
    manifest: &BTreeMap<String, ManifestEntry>,
) -> Result<std::collections::BTreeSet<String>> {
    let mut required = std::collections::BTreeSet::from(["index.html".to_owned()]);
    let mut visited = HashSet::new();
    let mut pending: Vec<_> = manifest
        .iter()
        .filter(|(_, entry)| entry.is_entry)
        .map(|(name, _)| name)
        .collect();
    while let Some(name) = pending.pop() {
        if !visited.insert(name) {
            continue;
        }
        let entry = manifest
            .get(name)
            .ok_or_else(|| invalid("console entry references a missing chunk"))?;
        required.insert(entry.file.clone());
        required.extend(entry.css.iter().cloned());
        pending.extend(entry.imports.iter().chain(&entry.dynamic_imports));
    }
    Ok(required)
}

fn html_assets(index: &str) -> Result<Vec<String>> {
    let mut assets = Vec::new();
    for tag in index
        .split('<')
        .filter_map(|part| part.split_once('>').map(|(tag, _)| tag))
    {
        let Some((kind, text)) = tag.split_once(char::is_whitespace) else {
            continue;
        };
        if !matches!(kind, "script" | "link") {
            continue;
        }
        let attributes = html_attributes(text)?;
        let reference = if kind == "script" {
            attributes.get("src")
        } else if attributes
            .get("rel")
            .is_some_and(|rel| matches!(*rel, "stylesheet" | "modulepreload"))
        {
            attributes.get("href")
        } else {
            None
        };
        if let Some(reference) = reference {
            let name = reference.strip_prefix('/').unwrap_or(reference);
            assets.push(name.strip_prefix("./").unwrap_or(name).to_owned());
        }
    }
    Ok(assets)
}

fn html_attributes(mut text: &str) -> Result<BTreeMap<&str, &str>> {
    let mut attributes = BTreeMap::new();
    while !text.trim().is_empty() {
        text = text.trim_start();
        let end = text
            .find(|ch: char| ch.is_whitespace() || ch == '=')
            .unwrap_or(text.len());
        let name = text
            .get(..end)
            .ok_or_else(|| invalid("invalid console entry attribute"))?;
        text = text
            .get(end..)
            .ok_or_else(|| invalid("invalid console entry attribute"))?
            .trim_start();
        let Some(value) = text.strip_prefix('=') else {
            continue;
        };
        text = value.trim_start();
        if let Some(quote) = text.chars().next().filter(|ch| matches!(ch, '\'' | '"')) {
            let value = text
                .get(quote.len_utf8()..)
                .ok_or_else(|| invalid("invalid console entry attribute"))?;
            let (value, rest) = value
                .split_once(quote)
                .ok_or_else(|| invalid("invalid console entry attribute"))?;
            attributes.insert(name, value);
            text = rest;
        } else {
            let end = text.find(char::is_whitespace).unwrap_or(text.len());
            let value = text
                .get(..end)
                .ok_or_else(|| invalid("invalid console entry attribute"))?;
            attributes.insert(name, value);
            text = text
                .get(end..)
                .ok_or_else(|| invalid("invalid console entry attribute"))?;
        }
    }
    Ok(attributes)
}

async fn fingerprint(path: &Path) -> Result<(u64, blake3::Hash)> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|source| file_error("open console build asset", path, source))?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 65_536];
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|source| file_error("read console build asset", path, source))?;
        if count == 0 {
            break;
        }
        length = length.saturating_add(count as u64);
        if length > 33_554_432 {
            return Err(invalid("console build asset exceeds its byte budget"));
        }
        let bytes = buffer
            .get(..count)
            .ok_or_else(|| invalid("console asset read exceeds its buffer"))?;
        hasher.update(bytes);
    }
    if length == 0 {
        return Err(invalid("console build assets must remain nonempty"));
    }
    Ok((length, hasher.finalize()))
}

#[cfg(test)]
pub(super) async fn validate_resources(config: &ConsoleConfig) -> Result<StaticFiles> {
    validate_key(&config.session.key_file).await?;
    validate_roots(&config.cluster.trust_roots).await?;
    StaticFiles::load(&config.static_dir).await
}

pub(super) async fn validate_remote_resources(config: &ConsoleConfig) -> Result<()> {
    validate_roots(&config.cluster.trust_roots).await
}

pub(super) async fn validate_embedded_resources(
    config: &super::EmbeddedConsoleConfig,
) -> Result<StaticFiles> {
    validate_key(&config.session.key_file).await?;
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

pub(super) async fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
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
