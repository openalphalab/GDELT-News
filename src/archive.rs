use anyhow::{Context, Result, bail, ensure};
use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use gdelt_type1::{
    ALGORITHM, Fragment, PIPELINE_VERSION, Recovery, TYPE2_ALGORITHM, Type2Options, reconstruct,
    reconstruct_type2,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};
use tempfile::NamedTempFile;
use unicode_segmentation::UnicodeSegmentation;

// Borrow ordinary JSON strings from the line buffer. Escaped strings still
// deserialize into owned storage, using serde's complete JSON validation.
#[derive(Deserialize)]
struct InputRecord<'a> {
    #[serde(borrow)]
    date: Cow<'a, str>,
    #[serde(borrow)]
    url: Cow<'a, str>,
    #[serde(borrow)]
    lang: Cow<'a, str>,
    #[serde(rename = "type")]
    kind: u8,
    pos: u8,
    #[serde(borrow)]
    pre: Cow<'a, str>,
    #[serde(borrow)]
    ngram: Cow<'a, str>,
    #[serde(borrow)]
    post: Cow<'a, str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Settings {
    pub types: Vec<u8>,
    pub type2: Type2Options,
    pub languages: Vec<String>,
    pub domains: Vec<String>,
    pub strip_artifacts: bool,
    pub quarantine_invalid_metadata: bool,
    pub max_expanded_bytes: u64,
    pub max_line_bytes: u64,
    pub max_fragments: usize,
    pub max_article_fragments: usize,
}

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn file_digest(path: &Path) -> Result<String> {
    let mut file = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("missing parent directory")?;
    fs::create_dir_all(parent)?;
    let mut temp = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temp, value)?;
    temp.write_all(b"\n")?;
    temp.as_file().sync_all()?;
    temp.persist(path)?;
    sync_directory(parent)?;
    Ok(())
}

pub fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub fn profile(settings: &Settings) -> String {
    digest(
        &serde_json::to_vec(&(
            PIPELINE_VERSION,
            ALGORITHM,
            TYPE2_ALGORITHM,
            unicode_segmentation::UNICODE_VERSION,
            unicode_normalization::UNICODE_VERSION,
            char::UNICODE_VERSION,
            settings,
        ))
        .expect("serializable settings"),
    )[..16]
        .to_owned()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Raw {
    #[serde(serialize_with = "portable_path")]
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
    pub source: String,
}

fn portable_path<S: Serializer>(
    path: &Path,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&path.to_string_lossy().replace('\\', "/"))
}

/// Local inputs are copied before recovery; original bytes are never rewritten.
pub fn import(root: &Path, source: &Path, max_bytes: u64) -> Result<Raw> {
    let metadata = source.metadata()?;
    ensure!(metadata.is_file(), "input must be a regular file");
    ensure!(metadata.len() <= max_bytes, "import exceeds byte limit");
    ensure!(
        fs2::available_space(root)? >= metadata.len().saturating_add(64 * 1024 * 1024),
        "insufficient disk space for import and reserve"
    );
    let parent = root.join("raw/imported");
    fs::create_dir_all(&parent)?;
    let mut temp = NamedTempFile::new_in(&parent)?;
    let bytes = std::io::copy(&mut File::open(source)?.take(max_bytes + 1), &mut temp)?;
    ensure!(bytes <= max_bytes, "import exceeds byte limit");
    temp.as_file().sync_all()?;
    let sha256 = file_digest(temp.path())?;
    let ext = if source.extension().is_some_and(|x| x == "gz") {
        "json.gz"
    } else {
        "json"
    };
    let path = parent.join(format!("{sha256}.{ext}"));
    if path.exists() {
        ensure!(
            file_digest(&path)? == sha256,
            "existing imported raw object is corrupt: {}",
            path.display()
        );
    } else {
        temp.persist(&path)?;
        sync_directory(&parent)?;
    }
    let raw = Raw {
        path: path.strip_prefix(root)?.to_path_buf(),
        sha256,
        bytes,
        source: source.canonicalize()?.display().to_string(),
    };
    atomic_json(&root.join(&raw.path).with_extension("source.json"), &raw)?;
    Ok(raw)
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Counts {
    pub lines: u64,
    pub type1: u64,
    pub type2: u64,
    pub type1_skipped: u64,
    pub type2_skipped: u64,
    pub other_type_skipped: u64,
    pub filtered: u64,
    pub selected_fragments: u64,
    pub articles: u64,
    pub type1_articles: u64,
    pub type2_articles: u64,
    pub ambiguous_articles: u64,
    pub articles_with_unmerged: u64,
    pub unmerged_fragments: u64,
    pub expanded_bytes: u64,
    pub best_effort_articles: u64,
    pub position_only_joins: u64,
    pub quarantined_metadata_records: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Quarantine {
    #[serde(serialize_with = "portable_path")]
    pub path: PathBuf,
    pub sha256: String,
    pub records: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub algorithm: String,
    pub profile: String,
    pub raw: Raw,
    #[serde(serialize_with = "portable_path")]
    pub output: PathBuf,
    pub output_sha256: String,
    pub counts: Counts,
    pub elapsed_seconds: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine: Option<Quarantine>,
}

#[derive(Serialize)]
struct Article<'a> {
    schema_version: u8,
    algorithm: &'static str,
    segmentation_type: u8,
    url: &'a str,
    date: &'a str,
    language: &'a str,
    source_domain: String,
    #[serde(serialize_with = "portable_path")]
    raw_path: &'a Path,
    raw_sha256: &'a str,
    // A single connected reconstruction still cannot certify completeness.
    completeness: &'static str,
    #[serde(flatten)]
    recovery: Recovery,
}

fn domain(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()?
        .host_str()
        .map(str::to_ascii_lowercase)
}

fn domain_matches(host: &str, filter: &str) -> bool {
    host == filter
        || host
            .strip_suffix(filter)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

pub fn process(root: &Path, raw: Raw, settings: &Settings) -> Result<(Counts, bool)> {
    let started = Instant::now();
    let profile = profile(settings);
    let directory = root.join("articles").join(&profile);
    fs::create_dir_all(&directory)?;
    let output = directory.join(format!("{}.articles.jsonl.gz", raw.sha256));
    let manifest_path = directory.join(format!("{}.manifest.json", raw.sha256));
    // Identity includes all output-affecting settings. Verify both objects
    // before accepting a checkpoint, including after an interrupted run.
    ensure!(
        file_digest(&root.join(&raw.path))? == raw.sha256,
        "raw checksum mismatch"
    );
    if manifest_path.exists()
        && output.exists()
        && let Ok(m) = serde_json::from_reader::<_, Manifest>(File::open(&manifest_path)?)
        && m.algorithm == PIPELINE_VERSION
        && m.profile == profile
        && m.raw.sha256 == raw.sha256
        && m.output_sha256 == file_digest(&output)?
        && m.quarantine
            .as_ref()
            .is_none_or(|q| file_digest(&root.join(&q.path)).is_ok_and(|sha| sha == q.sha256))
    {
        return Ok((m.counts, true));
    }
    atomic_json(&directory.join("settings.json"), settings)?;
    let file = BufReader::with_capacity(128 * 1024, File::open(root.join(&raw.path))?);
    let reader: Box<dyn Read> = if raw.path.extension().is_some_and(|e| e == "gz") {
        Box::new(MultiGzDecoder::new(file))
    } else {
        Box::new(file)
    };
    let mut reader = BufReader::with_capacity(128 * 1024, reader);
    let mut counts = Counts::default();
    let mut quarantine_temp = NamedTempFile::new_in(&directory)?;
    let mut quarantine_writer = GzEncoder::new(quarantine_temp.as_file_mut(), Compression::fast());
    // Separate observations of a changing URL and different languages.
    let mut groups: BTreeMap<(String, String, String, u8), Vec<Fragment>> = BTreeMap::new();
    let mut line = Vec::with_capacity(4096);
    loop {
        line.clear();
        let n = reader
            .by_ref()
            .take(settings.max_line_bytes + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        counts.lines += 1;
        ensure!(
            n as u64 <= settings.max_line_bytes,
            "line {} exceeds byte limit",
            counts.lines
        );
        counts.expanded_bytes += n as u64;
        ensure!(
            counts.expanded_bytes <= settings.max_expanded_bytes,
            "expanded input exceeds byte limit"
        );
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        // Fail the file visibly rather than silently deleting malformed records.
        let entry: InputRecord<'_> = serde_json::from_slice(&line)
            .with_context(|| format!("invalid record on line {}", counts.lines))?;
        match entry.kind {
            1 => counts.type1 += 1,
            2 => {
                counts.type2 += 1;
            }
            _ => {
                counts.other_type_skipped += 1;
                continue;
            }
        }
        if !settings.types.contains(&entry.kind) {
            if entry.kind == 1 {
                counts.type1_skipped += 1;
            } else {
                counts.type2_skipped += 1;
            }
            continue;
        }
        let mut reasons = Vec::new();
        if entry.url.trim().is_empty() {
            reasons.push("missing_url");
        }
        if entry.date.trim().is_empty() {
            reasons.push("missing_date");
        }
        if entry.lang.trim().is_empty() {
            reasons.push("missing_language");
        }
        if entry.pos > 90 || !entry.pos.is_multiple_of(10) {
            reasons.push("invalid_position");
        }
        if !reasons.is_empty() {
            ensure!(
                settings.quarantine_invalid_metadata,
                "invalid metadata on line {}: {}",
                counts.lines,
                reasons.join(",")
            );
            // Missing identity must never combine unrelated fragments into one article.
            // Preserve the record and its original line number for explicit repair.
            serde_json::to_writer(
                &mut quarantine_writer,
                &serde_json::json!({
                    "source_line": counts.lines, "reasons": reasons,
                    "record": serde_json::from_slice::<serde_json::Value>(&line)?
                }),
            )?;
            quarantine_writer.write_all(b"\n")?;
            counts.quarantined_metadata_records += 1;
            continue;
        }
        if !settings.languages.is_empty()
            && !settings
                .languages
                .iter()
                .any(|language| language == &entry.lang)
        {
            counts.filtered += 1;
            continue;
        }
        if !settings.domains.is_empty()
            && !domain(&entry.url).is_some_and(|host| {
                settings
                    .domains
                    .iter()
                    .any(|filter| domain_matches(&host, filter))
            })
        {
            counts.filtered += 1;
            continue;
        }
        let fragment = Fragment::from_context(
            entry.kind,
            entry.pos,
            &entry.pre,
            &entry.ngram,
            &entry.post,
            settings.strip_artifacts,
        );
        let tokens = if entry.kind == 2 {
            fragment.text.graphemes(true).count()
        } else {
            fragment.text.split_whitespace().count()
        };
        ensure!(
            tokens <= 256,
            "fragment on line {} exceeds the 256-token safety bound",
            counts.lines
        );
        ensure!(
            !fragment.text.is_empty(),
            "empty fragment on line {}",
            counts.lines
        );
        counts.selected_fragments += 1;
        ensure!(
            counts.selected_fragments as usize <= settings.max_fragments,
            "selected fragment limit exceeded"
        );
        let fragments = groups
            .entry((
                entry.url.into_owned(),
                entry.date.into_owned(),
                entry.lang.into_owned(),
                entry.kind,
            ))
            .or_default();
        ensure!(
            fragments.len() < settings.max_article_fragments,
            "article fragment limit exceeded on line {}",
            counts.lines
        );
        fragments.push(fragment);
    }
    quarantine_writer.finish()?;
    let quarantine = if counts.quarantined_metadata_records > 0 {
        quarantine_temp.as_file().sync_all()?;
        let sha256 = file_digest(quarantine_temp.path())?;
        let path = directory.join(format!("{}.quarantine.jsonl.gz", raw.sha256));
        quarantine_temp.persist(&path)?;
        sync_directory(&directory)?;
        Some(Quarantine {
            path: path.strip_prefix(root)?.into(),
            sha256,
            records: counts.quarantined_metadata_records,
        })
    } else {
        None
    };
    let mut temp = NamedTempFile::new_in(&directory)?;
    {
        let gz = GzEncoder::new(temp.as_file_mut(), Compression::fast());
        let mut writer = BufWriter::with_capacity(128 * 1024, gz);
        // Bounded batches avoid retaining all output alongside all input.
        let mut input = groups.into_iter();
        loop {
            let mut batch: Vec<_> = input.by_ref().take(64).collect();
            if batch.is_empty() {
                break;
            }
            let articles: Vec<_> = batch
                .par_iter_mut()
                .map(|((url, date, language, kind), fragments)| Article {
                    schema_version: 3,
                    algorithm: if *kind == 2 {
                        TYPE2_ALGORITHM
                    } else {
                        ALGORITHM
                    },
                    segmentation_type: *kind,
                    url,
                    date,
                    language,
                    source_domain: domain(url).unwrap_or_default(),
                    raw_path: &raw.path,
                    raw_sha256: &raw.sha256,
                    completeness: "not_verified",
                    recovery: if *kind == 2 {
                        reconstruct_type2(std::mem::take(fragments), settings.type2)
                    } else {
                        reconstruct(std::mem::take(fragments))
                    },
                })
                .collect();
            for article in articles {
                counts.articles += 1;
                if article.segmentation_type == 2 {
                    counts.type2_articles += 1;
                } else {
                    counts.type1_articles += 1;
                }
                counts.ambiguous_articles += u64::from(
                    article
                        .recovery
                        .type2_diagnostics
                        .as_ref()
                        .is_some_and(|d| d.stop_reason == "ambiguous_continuation"),
                );
                counts.articles_with_unmerged += u64::from(!article.recovery.unmerged.is_empty());
                if let Some(best) = article
                    .recovery
                    .type2_diagnostics
                    .as_ref()
                    .and_then(|d| d.best_effort.as_ref())
                {
                    counts.best_effort_articles += 1;
                    counts.position_only_joins += best.position_only_joins as u64;
                }
                counts.unmerged_fragments += article.recovery.unmerged.len() as u64;
                serde_json::to_writer(&mut writer, &article)?;
                writer.write_all(b"\n")?;
            }
        }
        writer.flush()?;
        writer.into_inner().map_err(|e| e.into_error())?.finish()?;
    }
    temp.as_file().sync_all()?;
    let output_sha256 = file_digest(temp.path())?;
    temp.persist(&output)?;
    sync_directory(&directory)?;
    let manifest = Manifest {
        algorithm: PIPELINE_VERSION.into(),
        profile,
        output: output.strip_prefix(root)?.into(),
        output_sha256,
        raw,
        counts,
        elapsed_seconds: started.elapsed().as_secs_f64(),
        quarantine,
    };
    atomic_json(&manifest_path, &manifest)?;
    Ok((manifest.counts, false))
}

pub fn validate_gzip(path: &Path, max_expanded: u64) -> Result<()> {
    let decoder = MultiGzDecoder::new(BufReader::new(File::open(path)?));
    let bytes = std::io::copy(&mut decoder.take(max_expanded + 1), &mut std::io::sink())?;
    if bytes > max_expanded {
        bail!("gzip exceeds expanded byte limit");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn borrowed_input_preserves_escaped_and_unescaped_json() {
        let plain = br#"{"type":2,"pos":0,"url":"https://example.com","date":"2025-01-01","lang":"ja","pre":"abc","ngram":"d","post":"ef"}"#;
        let row: InputRecord<'_> = serde_json::from_slice(plain).unwrap();
        assert!(matches!(row.pre, Cow::Borrowed("abc")));
        let escaped = br#"{"type":2,"pos":0,"url":"https://example.com","date":"2025-01-01","lang":"ja","pre":"a\n\"","ngram":"\u5b57","post":"\\end"}"#;
        let row: InputRecord<'_> = serde_json::from_slice(escaped).unwrap();
        assert_eq!(row.pre, "a\n\"");
        assert_eq!(row.ngram, "字");
        assert_eq!(row.post, "\\end");
        assert!(matches!(row.pre, Cow::Owned(_)));
    }
    #[test]
    fn hostname_boundaries() {
        assert!(domain_matches("news.bbc.co.uk", "bbc.co.uk"));
        assert!(!domain_matches("fakebbc.co.uk", "bbc.co.uk"));
        assert!(!domain_matches("bbc.co.uk.evil.test", "bbc.co.uk"));
    }
}
