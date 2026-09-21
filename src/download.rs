use crate::archive::{Raw, atomic_json, file_digest, sync_directory, validate_gzip};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use reqwest::{StatusCode, blocking::Client, header::RETRY_AFTER};
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::Path,
    sync::Mutex,
    thread,
    time::{Duration, Instant, SystemTime},
};
use tempfile::NamedTempFile;

pub struct Downloader {
    client: Client,
    cooldown: Mutex<Instant>,
    base_url: String,
    pub attempts: usize,
    pub max_download_bytes: u64,
    pub max_expanded_bytes: u64,
    pub min_free_bytes: u64,
}

impl Downloader {
    pub fn new(
        base_url: &str,
        attempts: usize,
        max_download_bytes: u64,
        max_expanded_bytes: u64,
        min_free_bytes: u64,
    ) -> Result<Self> {
        let url = reqwest::Url::parse(base_url).context("invalid source base URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "source URL must use HTTP or HTTPS with a hostname"
        );
        Ok(Self {
            client: Client::builder()
                .https_only(url.scheme() == "https")
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(180))
                .user_agent(concat!("firstlight-gdelt/", env!("CARGO_PKG_VERSION")))
                .pool_max_idle_per_host(16)
                .build()?,
            cooldown: Mutex::new(Instant::now()),
            base_url: base_url.trim_end_matches('/').into(),
            attempts,
            max_download_bytes,
            max_expanded_bytes,
            min_free_bytes,
        })
    }

    pub fn fetch(&self, root: &Path, minute: DateTime<Utc>) -> Result<Option<Raw>> {
        let name = minute.format("%Y%m%d%H%M%S.webngrams.json.gz").to_string();
        let directory = root.join("raw").join(minute.format("%Y/%m/%d").to_string());
        fs::create_dir_all(&directory)?;
        let path = directory.join(&name);
        let metadata = path.with_extension("source.json");
        let source = format!("{}/{name}", self.base_url);
        if path.exists() && metadata.exists() {
            let cached: Result<Raw, _> = serde_json::from_reader(File::open(&metadata)?);
            if let Ok(raw) = cached
                && raw.source == source
                && raw.path == path.strip_prefix(root)?
                && raw.bytes == path.metadata()?.len()
                && raw.sha256 == file_digest(&path)?
            {
                return Ok(Some(raw));
            }
        }
        // An interrupted or corrupt object is retained for diagnosis.
        if path.exists() {
            let quarantine = directory.join(format!("{name}.unverified-{}", file_digest(&path)?));
            if !quarantine.exists() {
                fs::rename(&path, quarantine)?;
            }
        }
        let mut last_error = String::new();
        for attempt in 0..self.attempts {
            loop {
                let until = *self.cooldown.lock().expect("cooldown mutex");
                let wait = until.saturating_duration_since(Instant::now());
                if wait.is_zero() {
                    break;
                }
                thread::sleep(wait.min(Duration::from_secs(1)));
            }
            ensure!(
                fs2::available_space(root)?
                    >= self.min_free_bytes.saturating_add(self.max_download_bytes),
                "disk free-space reserve reached"
            );
            let response = self.client.get(&source).send();
            match response {
                Ok(response) if response.status() == StatusCode::NOT_FOUND => return Ok(None),
                Ok(response) if response.status() == StatusCode::OK => {
                    let save = (|| -> Result<Raw> {
                        if let Some(length) = response.content_length() {
                            ensure!(
                                length <= self.max_download_bytes,
                                "download exceeds byte limit"
                            );
                        }
                        let mut temp = NamedTempFile::new_in(&directory)?;
                        let bytes = std::io::copy(
                            &mut response.take(self.max_download_bytes + 1),
                            &mut temp,
                        )?;
                        ensure!(
                            bytes <= self.max_download_bytes,
                            "download exceeds byte limit"
                        );
                        temp.flush()?;
                        validate_gzip(temp.path(), self.max_expanded_bytes)
                            .context("invalid downloaded gzip")?;
                        let sha256 = file_digest(temp.path())?;
                        temp.as_file().sync_all()?;
                        temp.persist(&path)?;
                        sync_directory(&directory)?;
                        let raw = Raw {
                            path: path.strip_prefix(root)?.into(),
                            sha256,
                            bytes,
                            source: source.clone(),
                        };
                        atomic_json(&metadata, &raw)?;
                        Ok(raw)
                    })();
                    match save {
                        Ok(raw) => return Ok(Some(raw)),
                        Err(e) => last_error = format!("{e:#}"),
                    }
                }
                Ok(response) => {
                    let status = response.status();
                    if !(status.is_server_error()
                        || status == StatusCode::TOO_MANY_REQUESTS
                        || status == StatusCode::REQUEST_TIMEOUT)
                    {
                        bail!("HTTP {status} for {source}");
                    }
                    last_error = format!("HTTP {status}");
                    if status == StatusCode::TOO_MANY_REQUESTS
                        || status == StatusCode::SERVICE_UNAVAILABLE
                    {
                        let wait = bounded_retry_after(
                            response
                                .headers()
                                .get(RETRY_AFTER)
                                .and_then(|v| v.to_str().ok())
                                .and_then(retry_after)
                                .unwrap_or(Duration::from_secs(2u64.pow(attempt as u32))),
                        )?;
                        let mut until = self.cooldown.lock().expect("cooldown mutex");
                        *until = (*until).max(
                            Instant::now()
                                .checked_add(wait)
                                .context("Retry-After exceeds monotonic clock range")?,
                        );
                    }
                }
                Err(e) => last_error = format!("{:#}", anyhow::Error::new(e)),
            }
            if attempt + 1 < self.attempts {
                thread::sleep(Duration::from_millis(250 * 2u64.pow(attempt as u32)));
            }
        }
        bail!(
            "{source}: failed after {} attempts: {last_error}",
            self.attempts
        )
    }
}

fn retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|at| at.duration_since(SystemTime::now()).unwrap_or_default())
}

fn bounded_retry_after(wait: Duration) -> Result<Duration> {
    ensure!(
        wait <= Duration::from_secs(300),
        "server Retry-After exceeds the 300-second retry window; retry this collection later"
    );
    Ok(wait)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_after_seconds_and_dates() {
        assert_eq!(retry_after("12"), Some(Duration::from_secs(12)));
        assert_eq!(
            retry_after("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(Duration::ZERO)
        );
        assert_eq!(retry_after("invalid"), None);
        assert!(bounded_retry_after(retry_after("18446744073709551615").unwrap()).is_err());
        assert_eq!(
            bounded_retry_after(Duration::from_secs(300)).unwrap(),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn low_disk_reserve_fails_before_network_access() {
        let root = tempfile::TempDir::new().unwrap();
        let downloader = Downloader::new("http://127.0.0.1:1", 1, 1024, 8192, u64::MAX).unwrap();
        let minute = DateTime::parse_from_rfc3339("2025-03-16T00:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let error = downloader.fetch(root.path(), minute).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("disk free-space reserve reached")
        );
    }
}
