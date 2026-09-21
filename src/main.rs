mod archive;
mod download;

use anyhow::{Context, Result, bail, ensure};
use archive::{Raw, Settings, atomic_json, process};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Timelike, Utc};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use serde::Serialize;
use std::{
    fs::{self, OpenOptions},
    path::PathBuf,
    sync::atomic::{AtomicI64, Ordering},
    thread,
    time::Instant,
};

#[derive(Parser)]
#[command(
    version,
    about = "Recover Type 1 and Type 2 GDELT news and retain exact raw source files"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(long, global = true, default_value = "data/gdelt-type1")]
    archive: PathBuf,
    /// Segmentation types to recover: 1, 2, or both.
    #[arg(long, global = true, value_delimiter = ',', default_value = "1,2")]
    types: Vec<u8>,
    /// Minimum Type 2 overlap in extended grapheme clusters (1..256).
    #[arg(long, global = true, default_value_t = 4)]
    type2_min_overlap: usize,
    /// Largest Type 2 decile jump per merge (0,10,...,90).
    #[arg(long, global = true, default_value_t = 10)]
    type2_max_position_gap: u8,
    /// Maximum Type 2 branch-search work per article; 0 disables it.
    #[arg(long, global = true, default_value_t = 20_000)]
    type2_search_budget: usize,
    /// Also produce one best-effort text, with estimated joins, from all Type 2 sections.
    #[arg(long, global = true)]
    type2_best_effort: bool,
    /// Minimum exact grapheme overlap for the optional estimated assembly (1..256).
    #[arg(long, global = true, default_value_t = 2)]
    type2_best_effort_min_overlap: usize,
    /// Comma-separated GDELT language codes; omitted means every selected language.
    #[arg(long, global = true, value_delimiter = ',')]
    language: Vec<String>,
    /// Exact hostnames, including subdomains; omitted means all sources.
    #[arg(long, global = true, value_delimiter = ',')]
    domain: Vec<String>,
    /// Retain the early Type 1 " / " artifact. Type 2 never applies this heuristic.
    #[arg(long, global = true)]
    keep_artifacts: bool,
    /// Preserve records with missing identity/invalid positions in a quarantine sidecar.
    #[arg(long, global = true)]
    quarantine_invalid_metadata: bool,
    /// Number of CPU threads; 0 uses available parallelism.
    #[arg(long, global = true, default_value_t = 0)]
    threads: usize,
    /// Maximum expanded size of one input (MiB).
    #[arg(long, global = true, default_value_t = 512)]
    max_expanded_mib: u64,
    /// Maximum number of selected fragments in one file.
    #[arg(long, global = true, default_value_t = 2_000_000)]
    max_fragments: usize,
    /// Maximum fragments in any single observation, preventing oversized assemblies.
    #[arg(long, global = true, default_value_t = 100_000)]
    max_article_fragments: usize,
}

#[derive(Subcommand)]
enum Command {
    /// Download and reconstruct every minute in an inclusive UTC interval.
    Collect {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        #[arg(long, default_value_t = 4)]
        downloads: usize,
        #[arg(long, default_value_t = 4)]
        attempts: usize,
        #[arg(long, default_value_t = 128)]
        max_download_mib: u64,
        #[arg(long, default_value_t = 2)]
        min_free_gib: u64,
        #[arg(
            long,
            default_value = "https://data.gdeltproject.org/gdeltv3/webngrams"
        )]
        base_url: String,
    },
    /// Import a local JSON/JSON.GZ file or a directory, then recover selected types.
    Reconstruct {
        #[arg(long)]
        input: PathBuf,
    },
}

#[derive(Default, Serialize)]
struct Summary {
    completed: u64,
    cached: u64,
    missing: u64,
    failed: u64,
    articles: u64,
    type1_articles: u64,
    type2_articles: u64,
    ambiguous_articles: u64,
    articles_with_unmerged: u64,
    best_effort_articles: u64,
    position_only_joins: u64,
    selected_fragments: u64,
    type2_skipped: u64,
    type1_skipped: u64,
    quarantined_metadata_records: u64,
    elapsed_seconds: f64,
}

fn timestamp(value: &str) -> Result<DateTime<Utc>> {
    let ts = if let Ok(ts) = DateTime::parse_from_rfc3339(value) {
        ts.with_timezone(&Utc)
    } else {
        NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S")
            .with_context(|| format!("use RFC3339 with timezone or YYYYMMDDHHMMSS: {value}"))?
            .and_utc()
    };
    ensure!(
        ts.second() == 0 && ts.nanosecond() == 0,
        "timestamps must be whole minutes"
    );
    Ok(ts)
}

fn handle(
    root: &std::path::Path,
    raw: Raw,
    settings: &Settings,
    summary: &mut Summary,
) -> Result<()> {
    let name = raw.path.display().to_string();
    let (counts, cached) = process(root, raw, settings)?;
    summary.completed += 1;
    summary.cached += u64::from(cached);
    summary.articles += counts.articles;
    summary.type1_articles += counts.type1_articles;
    summary.type2_articles += counts.type2_articles;
    summary.ambiguous_articles += counts.ambiguous_articles;
    summary.articles_with_unmerged += counts.articles_with_unmerged;
    summary.best_effort_articles += counts.best_effort_articles;
    summary.position_only_joins += counts.position_only_joins;
    summary.selected_fragments += counts.selected_fragments;
    summary.type2_skipped += counts.type2_skipped;
    summary.type1_skipped += counts.type1_skipped;
    summary.quarantined_metadata_records += counts.quarantined_metadata_records;
    if counts.quarantined_metadata_records > 0 {
        eprintln!(
            "{}: {} metadata-invalid records preserved in quarantine",
            name, counts.quarantined_metadata_records
        );
    }
    eprintln!(
        "{}: {} Type 1 and {} Type 2 observations, {} ambiguous{}",
        name,
        counts.type1_articles,
        counts.type2_articles,
        counts.ambiguous_articles,
        if cached { " (verified cache)" } else { "" }
    );
    Ok(())
}

fn run(cli: Cli) -> Result<()> {
    ensure!(
        cli.max_expanded_mib > 0 && cli.max_expanded_mib <= 65536,
        "max-expanded-mib must be 1..65536"
    );
    ensure!(cli.max_fragments > 0, "max-fragments must be positive");
    ensure!(
        cli.max_article_fragments > 0 && cli.max_article_fragments <= 2_000_000,
        "max-article-fragments must be 1..2000000"
    );
    ensure!(cli.threads <= 256, "threads must be 0..256");
    ensure!(
        cli.type2_search_budget <= 1_000_000,
        "type2-search-budget must be 0..1000000"
    );
    ensure!(
        (1..=256).contains(&cli.type2_best_effort_min_overlap),
        "type2-best-effort-min-overlap must be 1..256"
    );
    ensure!(
        !cli.types.is_empty() && cli.types.iter().all(|t| [1, 2].contains(t)),
        "types must be 1, 2, or 1,2"
    );
    ensure!(
        (1..=256).contains(&cli.type2_min_overlap),
        "type2-min-overlap must be 1..256"
    );
    ensure!(
        cli.type2_max_position_gap <= 90 && cli.type2_max_position_gap.is_multiple_of(10),
        "type2-max-position-gap must be 0,10,...,90"
    );
    fs::create_dir_all(&cli.archive)?;
    let root = cli.archive.canonicalize()?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(".lock"))?;
    lock.try_lock_exclusive()
        .context("another collector is using this archive")?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.threads)
        .build_global()?;
    let mut settings = Settings {
        types: cli.types,
        type2: gdelt_type1::Type2Options {
            min_overlap: cli.type2_min_overlap,
            max_position_gap: cli.type2_max_position_gap,
            search_budget: cli.type2_search_budget,
            best_effort: cli.type2_best_effort,
            best_effort_min_overlap: cli.type2_best_effort_min_overlap,
        },
        languages: cli.language,
        domains: cli
            .domain
            .into_iter()
            .map(|s| s.trim().trim_end_matches('.').to_ascii_lowercase())
            .collect(),
        strip_artifacts: !cli.keep_artifacts,
        quarantine_invalid_metadata: cli.quarantine_invalid_metadata,
        max_expanded_bytes: cli.max_expanded_mib * 1024 * 1024,
        max_line_bytes: 1024 * 1024,
        max_fragments: cli.max_fragments,
        max_article_fragments: cli.max_article_fragments,
    };
    settings.languages.sort();
    settings.types.sort();
    settings.types.dedup();
    settings.languages.dedup();
    settings.domains.sort();
    settings.domains.dedup();
    ensure!(
        settings.languages.iter().all(|s| !s.is_empty()),
        "empty language filter"
    );
    ensure!(
        settings
            .domains
            .iter()
            .all(|s| !s.is_empty() && !s.contains('/') && !s.contains(':')),
        "domain filters must be hostnames"
    );
    let started = Instant::now();
    let mut summary = Summary::default();
    let run_id = Utc::now().format("%Y%m%dT%H%M%S%.9fZ").to_string();
    let run_dir = root.join("runs").join(run_id);
    fs::create_dir_all(&run_dir)?;
    match cli.command {
        Command::Collect {
            start,
            end,
            downloads,
            attempts,
            max_download_mib,
            min_free_gib,
            base_url,
        } => {
            ensure!((1..=32).contains(&downloads), "downloads must be 1..32");
            ensure!((1..=8).contains(&attempts), "attempts must be 1..8");
            ensure!(
                (1..=4096).contains(&max_download_mib) && min_free_gib <= 65536,
                "invalid disk/download limits"
            );
            let start = timestamp(&start)?;
            let end = timestamp(&end)?;
            ensure!(end >= start, "end must be >= start");
            ensure!(
                start >= timestamp("20200101000000")?,
                "dataset begins in January 2020"
            );
            ensure!(end <= Utc::now(), "end must not be in the future");
            let downloader = download::Downloader::new(
                &base_url,
                attempts,
                max_download_mib * 1024 * 1024,
                settings.max_expanded_bytes,
                min_free_gib * 1024 * 1024 * 1024,
            )?;
            atomic_json(
                &run_dir.join("request.json"),
                &serde_json::json!({"start":start,"end":end,
                "downloads":downloads,"settings":settings,"profile":archive::profile(&settings),"base_url":base_url}),
            )?;
            let next = AtomicI64::new(0);
            let count = (end - start).num_minutes() + 1;
            // Only paths cross the bounded channel; one file is reconstructed
            // at a time while pooled downloads overlap CPU work.
            let (sender, receiver) = crossbeam_channel::bounded(downloads);
            thread::scope(|scope| -> Result<()> {
                for _ in 0..downloads {
                    let sender = sender.clone();
                    let downloader = &downloader;
                    let next = &next;
                    let root = &root;
                    scope.spawn(move || {
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= count {
                                break;
                            }
                            let minute = start + ChronoDuration::minutes(i);
                            let result = downloader.fetch(root, minute);
                            if sender.send((minute, result)).is_err() {
                                break;
                            }
                        }
                    });
                }
                drop(sender);
                for (minute, result) in receiver {
                    let key = minute.format("%Y%m%d%H%M%S").to_string();
                    let result = match result {
                        Ok(Some(raw)) => handle(&root, raw, &settings, &mut summary),
                        Ok(None) => {
                            summary.missing += 1;
                            atomic_json(
                                &run_dir.join(format!("{key}.json")),
                                &serde_json::json!({"status":"missing","minute":minute}),
                            )?;
                            continue;
                        }
                        Err(e) => Err(e),
                    };
                    if let Err(e) = result {
                        summary.failed += 1;
                        eprintln!("{key}: {e:#}");
                        atomic_json(
                            &run_dir.join(format!("{key}.json")),
                            &serde_json::json!({"status":"failed","minute":minute,"error":format!("{e:#}")}),
                        )?;
                    } else {
                        atomic_json(
                            &run_dir.join(format!("{key}.json")),
                            &serde_json::json!({"status":"complete","minute":minute}),
                        )?;
                    }
                }
                Ok(())
            })?;
        }
        Command::Reconstruct { input } => {
            let files = if input.is_dir() {
                let mut files = fs::read_dir(&input)?
                    .map(|e| e.map(|e| e.path()))
                    .collect::<std::io::Result<Vec<_>>>()?;
                files.retain(|p| {
                    p.is_file()
                        && p.file_name().is_some_and(|n| {
                            let n = n.to_string_lossy();
                            n.ends_with(".json.gz") || n.ends_with(".json")
                        })
                });
                files.sort();
                files
            } else {
                vec![input]
            };
            ensure!(!files.is_empty(), "no JSON/JSON.GZ input files found");
            for file in files {
                let result = archive::import(&root, &file, settings.max_expanded_bytes)
                    .and_then(|raw| handle(&root, raw, &settings, &mut summary));
                if let Err(e) = result {
                    summary.failed += 1;
                    eprintln!("{}: {e:#}", file.display());
                    let key = archive::digest(file.to_string_lossy().as_bytes());
                    atomic_json(
                        &run_dir.join(format!("{key}.json")),
                        &serde_json::json!({"status":"failed","input":file,"error":format!("{e:#}")}),
                    )?;
                }
            }
        }
    }
    summary.elapsed_seconds = started.elapsed().as_secs_f64();
    atomic_json(&run_dir.join("summary.json"), &summary)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    if summary.failed > 0 {
        bail!(
            "{} files failed; rerun the same command to retry",
            summary.failed
        );
    }
    Ok(())
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
