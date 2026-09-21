use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
    process::{Command, Output},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use tempfile::TempDir;

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gdelt-type1"))
        .arg("--archive")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}

fn record(kind: u8, url: &str, date: &str, pre: &str, word: &str, post: &str) -> String {
    json!({"type":kind,"url":url,"date":date,"lang":"en","pos":0,
        "pre":pre,"ngram":word,"post":post})
    .to_string()
        + "\n"
}

fn fixture() -> Vec<u8> {
    let mut text = record(
        1,
        "https://news.example.com/a",
        "2025-03-16T00:01:00Z",
        "",
        "Hello",
        "world",
    );
    text += &record(
        1,
        "https://news.example.com/a",
        "2025-03-16T00:01:00Z",
        "Hello",
        "world",
        "today.",
    );
    text += &record(
        1,
        "https://news.example.com/a",
        "2025-03-16T00:02:00Z",
        "",
        "Updated",
        "story",
    );
    text += &record(
        2,
        "https://news.example.com/type2",
        "2025-03-16T00:01:00Z",
        "",
        "字",
        "符",
    );
    text += &record(
        1,
        "https://fakeexample.com/a",
        "2025-03-16T00:01:00Z",
        "",
        "Another",
        "story",
    );
    text.into_bytes()
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::fast());
    gz.write_all(data).unwrap();
    gz.finish().unwrap()
}

fn outputs(root: &Path) -> Vec<std::path::PathBuf> {
    let mut files = vec![];
    for profile in fs::read_dir(root.join("articles")).unwrap() {
        for entry in fs::read_dir(profile.unwrap().path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "gz") {
                files.push(path);
            }
        }
    }
    files
}

fn articles(root: &Path) -> Vec<Value> {
    outputs(root)
        .iter()
        .flat_map(|p| {
            let mut text = String::new();
            MultiGzDecoder::new(fs::File::open(p).unwrap())
                .read_to_string(&mut text)
                .unwrap();
            text.lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn raw_preservation_types_revisions_filters_and_restart() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("input.json.gz");
    let bytes = gzip(&fixture());
    fs::write(&input, &bytes).unwrap();
    let root = temp.path().join("archive");
    let args = [
        "reconstruct",
        "--input",
        input.to_str().unwrap(),
        "--domain",
        "example.com",
        "--types",
        "1",
    ];
    let first = run(&root, &args);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let rows = articles(&root);
    assert_eq!(rows.len(), 2); // Different timestamps must not be fused.
    assert_eq!(rows[0]["text"], "Hello world today.");
    assert_eq!(rows[1]["text"], "Updated story");
    assert_eq!(
        fs::read(root.join(rows[0]["raw_path"].as_str().unwrap())).unwrap(),
        bytes
    );
    let second = run(&root, &args);
    let summary: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(summary["cached"], 1);
    assert_eq!(summary["type2_skipped"], 1);
    assert_eq!(outputs(&root).len(), 1);
    fs::write(&outputs(&root)[0], b"corruption").unwrap();
    assert!(run(&root, &args).status.success());
    assert_eq!(articles(&root), rows);
    let manifest = outputs(&root)[0]
        .parent()
        .unwrap()
        .read_dir()
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.to_string_lossy().ends_with(".manifest.json"))
        .unwrap();
    fs::write(manifest, b"{broken checkpoint").unwrap();
    assert!(run(&root, &args).status.success());
    assert_eq!(articles(&root), rows);
    let filtered = run(
        &root,
        &[
            "reconstruct",
            "--input",
            input.to_str().unwrap(),
            "--language",
            "fr",
        ],
    );
    assert!(filtered.status.success());
    assert_eq!(outputs(&root).len(), 2); // Config changes cannot reuse wrong outputs.
}

#[test]
fn rejects_corruption_and_malformed_input_without_completed_output() {
    for bytes in [b"not gzip".to_vec(), {
        let mut b = gzip(&fixture());
        b.truncate(b.len() - 5);
        b
    }] {
        let temp = TempDir::new().unwrap();
        let input = temp.path().join("bad.json.gz");
        fs::write(&input, bytes).unwrap();
        let root = temp.path().join("archive");
        let result = run(&root, &["reconstruct", "--input", input.to_str().unwrap()]);
        assert!(!result.status.success());
        assert!(outputs(&root).is_empty());
    }
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("bad.json");
    let mut data = fixture();
    data.extend_from_slice(b"{broken json}\n");
    fs::write(&input, data).unwrap();
    let root = temp.path().join("archive");
    assert!(
        !run(&root, &["reconstruct", "--input", input.to_str().unwrap()])
            .status
            .success()
    );
    assert!(outputs(&root).is_empty());
}

struct Server {
    url: String,
    count: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}
impl Server {
    fn new(responses: Vec<(u16, Vec<u8>)>) -> Self {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let url = format!("http://{}", server.server_addr());
        let stop = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let stop2 = stop.clone();
        let count2 = count.clone();
        let handle = thread::spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                if let Ok(Some(request)) = server.recv_timeout(Duration::from_millis(10)) {
                    let i = count2
                        .fetch_add(1, Ordering::Relaxed)
                        .min(responses.len() - 1);
                    let (status, body) = &responses[i];
                    let response = tiny_http::Response::from_data(body.clone())
                        .with_status_code(*status)
                        .with_header(tiny_http::Header::from_bytes("Retry-After", "0").unwrap());
                    let _ = request.respond(response);
                }
            }
        });
        Self {
            url,
            count,
            stop,
            handle: Some(handle),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap();
    }
}

fn collect(root: &Path, server: &Server) -> Output {
    run(
        root,
        &[
            "collect",
            "--start",
            "20250316000100",
            "--end",
            "20250316000100",
            "--base-url",
            &server.url,
            "--attempts",
            "2",
            "--min-free-gib",
            "0",
            "--downloads",
            "2",
        ],
    )
}

#[test]
fn download_retry_resume_and_corrupt_cache_repair() {
    let server = Server::new(vec![(429, vec![]), (200, gzip(&fixture()))]);
    let temp = TempDir::new().unwrap();
    let result = collect(temp.path(), &server);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(server.count.load(Ordering::Relaxed), 2);
    assert!(collect(temp.path(), &server).status.success());
    assert_eq!(server.count.load(Ordering::Relaxed), 2);
    let raw = temp
        .path()
        .join(articles(temp.path())[0]["raw_path"].as_str().unwrap());
    fs::write(&raw, b"corrupt raw").unwrap();
    assert!(collect(temp.path(), &server).status.success());
    assert_eq!(server.count.load(Ordering::Relaxed), 3);
}

#[test]
fn distinguishes_missing_denied_and_invalid_downloads() {
    for (status, body, success, expected, requests) in [
        (404, vec![], true, "missing", 1),
        (403, vec![], false, "failed", 1),
        (206, gzip(&fixture()), false, "failed", 1),
        (200, b"not a gzip".to_vec(), false, "failed", 2),
    ] {
        let server = Server::new(vec![(status, body)]);
        let temp = TempDir::new().unwrap();
        let result = collect(temp.path(), &server);
        assert_eq!(result.status.success(), success);
        let summary: Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(summary[expected], 1);
        assert_eq!(server.count.load(Ordering::Relaxed), requests);
    }
}

#[test]
fn validates_time_boundaries_and_fragment_limits() {
    let temp = TempDir::new().unwrap();
    let invalid = run(
        temp.path(),
        &[
            "collect",
            "--start",
            "2025-03-16T00:01:01Z",
            "--end",
            "20250316000100",
        ],
    );
    assert!(!invalid.status.success());
    let input = temp.path().join("input.json");
    fs::write(&input, fixture()).unwrap();
    let limited = run(
        &temp.path().join("limited"),
        &[
            "reconstruct",
            "--input",
            input.to_str().unwrap(),
            "--max-fragments",
            "1",
        ],
    );
    assert!(!limited.status.success());
}

#[test]
fn type2_pipeline_selection_separation_and_option_checkpoints() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("mixed.json.gz");
    let url = "https://example.com/story";
    let date = "2025-03-16T00:01:00Z";
    let text = record(2, url, date, "前文", "甲", "乙丙丁")
        + &record(2, url, date, "甲乙", "丙", "丁后文")
        + &record(1, url, date, "A", "different", "version");
    let raw = gzip(text.as_bytes());
    fs::write(&input, &raw).unwrap();
    let root = temp.path().join("type2");
    let args = [
        "reconstruct",
        "--input",
        input.to_str().unwrap(),
        "--types",
        "2",
    ];
    let result = run(&root, &args);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let summary: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(summary["type2_articles"], 1);
    assert_eq!(summary["type1_skipped"], 1);
    let rows = articles(&root);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["segmentation_type"], 2);
    assert_eq!(rows[0]["schema_version"], 3);
    assert_eq!(rows[0]["text"], "前文甲乙丙丁后文");
    assert_eq!(rows[0]["type2_diagnostics"]["minimum_overlap_used"], 4);
    assert_eq!(
        fs::read(root.join(rows[0]["raw_path"].as_str().unwrap())).unwrap(),
        raw
    );
    let resumed: Value = serde_json::from_slice(&run(&root, &args).stdout).unwrap();
    assert_eq!(resumed["cached"], 1);
    let mut strict = args.to_vec();
    strict.extend(["--type2-min-overlap", "8"]);
    assert!(run(&root, &strict).status.success());
    assert_eq!(outputs(&root).len(), 2);
    assert!(articles(&root).iter().any(|a| a["text"] == "前文甲乙丙丁"));
    let mut no_search = args.to_vec();
    no_search.extend(["--type2-search-budget", "0"]);
    assert!(run(&root, &no_search).status.success());
    assert_eq!(outputs(&root).len(), 3);
    assert!(
        articles(&root)
            .iter()
            .any(|a| a["type2_diagnostics"]["search_budget"] == 0)
    );
    let both = temp.path().join("both");
    assert!(
        run(&both, &["reconstruct", "--input", input.to_str().unwrap()])
            .status
            .success()
    );
    let rows = articles(&both);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["text"], "A different version");
    assert_eq!(rows[1]["text"], "前文甲乙丙丁后文");
}

#[test]
fn type2_token_limit_counts_graphemes_and_cli_rejects_invalid_options() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("too_long.json");
    let text = record(
        2,
        "https://example.com",
        "2025-03-16T00:01:00Z",
        &"界".repeat(256),
        "长",
        "",
    );
    fs::write(&input, text).unwrap();
    let args = ["reconstruct", "--input", input.to_str().unwrap()];
    assert!(!run(&temp.path().join("limit"), &args).status.success());
    // Invalid options must fail independently of the oversized-input guard.
    fs::write(&input, fixture()).unwrap();
    assert!(run(&temp.path().join("valid"), &args).status.success());
    for option in [
        ["--types", "0"],
        ["--type2-min-overlap", "0"],
        ["--type2-max-position-gap", "11"],
        ["--type2-search-budget", "1000001"],
        ["--type2-best-effort-min-overlap", "0"],
    ] {
        let mut invalid = args.to_vec();
        invalid.extend(option);
        assert!(!run(temp.path(), &invalid).status.success());
    }
}

#[test]
fn best_effort_export_is_optional_and_keeps_conservative_evidence() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("split.json");
    let raw = record(
        2,
        "https://example.com/story",
        "2025-03-16T00:01:00Z",
        "first",
        "ABCD",
        "",
    ) + &record(
        2,
        "https://example.com/story",
        "2025-03-16T00:01:00Z",
        "different",
        "EFGH",
        "",
    );
    fs::write(&input, raw).unwrap();
    let root = temp.path().join("archive");
    let args = [
        "reconstruct",
        "--input",
        input.to_str().unwrap(),
        "--types",
        "2",
    ];
    assert!(run(&root, &args).status.success());
    assert!(
        articles(&root)[0]["type2_diagnostics"]
            .get("best_effort")
            .is_none()
    );
    let mut joined = args.to_vec();
    joined.push("--type2-best-effort");
    assert!(run(&root, &joined).status.success());
    assert_eq!(outputs(&root).len(), 2);
    let rows = articles(&root);
    let row = rows
        .iter()
        .find(|r| r["type2_diagnostics"].get("best_effort").is_some())
        .unwrap();
    assert_eq!(row["text"], "firstABCD");
    assert_eq!(row["unmerged"].as_array().unwrap().len(), 1);
    assert_eq!(
        row["type2_diagnostics"]["best_effort"]["text"],
        "firstABCD\n\ndifferentEFGH"
    );
    assert_eq!(
        row["type2_diagnostics"]["best_effort"]["ordering"],
        "estimated"
    );
    let summary: Value = serde_json::from_slice(&run(&root, &joined).stdout).unwrap();
    assert_eq!(summary["cached"], 1);
    assert_eq!(summary["best_effort_articles"], 1);
    assert_eq!(summary["position_only_joins"], 1);
    assert_eq!(summary["articles_with_unmerged"], 1);
}

#[test]
fn import_and_expansion_and_per_article_limits_fail_without_completed_output() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("oversized.json");
    fs::write(&input, vec![b' '; 2 * 1024 * 1024]).unwrap();
    let root = temp.path().join("raw_limit");
    let r = run(
        &root,
        &[
            "reconstruct",
            "--input",
            input.to_str().unwrap(),
            "--max-expanded-mib",
            "1",
        ],
    );
    assert!(!r.status.success());
    assert!(String::from_utf8_lossy(&r.stderr).contains("import exceeds byte limit"));
    assert!(!root.join("raw/imported").exists());
    let compressed = temp.path().join("large.json.gz");
    fs::write(&compressed, gzip(&fixture().repeat(2000))).unwrap();
    let root = temp.path().join("expanded_limit");
    let r = run(
        &root,
        &[
            "reconstruct",
            "--input",
            compressed.to_str().unwrap(),
            "--max-expanded-mib",
            "1",
        ],
    );
    assert!(!r.status.success());
    assert!(String::from_utf8_lossy(&r.stderr).contains("expanded input exceeds byte limit"));
    assert!(outputs(&root).is_empty());
    fs::write(&input, fixture()).unwrap();
    let root = temp.path().join("article_limit");
    let r = run(
        &root,
        &[
            "reconstruct",
            "--input",
            input.to_str().unwrap(),
            "--max-article-fragments",
            "1",
        ],
    );
    assert!(!r.status.success());
    assert!(String::from_utf8_lossy(&r.stderr).contains("article fragment limit exceeded"));
    assert!(outputs(&root).is_empty());
}

#[test]
fn multi_member_gzip_and_worker_counts_produce_identical_results() {
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("members.json.gz");
    let source = fixture();
    let mut zipped = gzip(&source[..source.len() / 2]);
    zipped.extend(gzip(&source[source.len() / 2..]));
    fs::write(&input, zipped).unwrap();
    let one = temp.path().join("one");
    let four = temp.path().join("four");
    for (root, threads) in [(&one, "1"), (&four, "4")] {
        assert!(
            run(
                root,
                &[
                    "reconstruct",
                    "--input",
                    input.to_str().unwrap(),
                    "--threads",
                    threads,
                    "--type2-best-effort"
                ]
            )
            .status
            .success()
        );
    }
    assert_eq!(articles(&one).len(), 4);
    assert_eq!(articles(&one), articles(&four));
}

#[test]
fn a_forced_stop_releases_the_archive_lock_and_collection_resumes() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("archive");
    let stalled = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let url = format!("http://{}", stalled.server_addr());
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_gdelt-type1"))
            .arg("--archive")
            .arg(&root)
            .args([
                "collect",
                "--start",
                "20250316000100",
                "--end",
                "20250316000100",
                "--base-url",
                &url,
                "--attempts",
                "1",
                "--downloads",
                "1",
                "--min-free-gib",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let request = stalled
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .expect("collector did not request source");
    let input = temp.path().join("input.json");
    fs::write(&input, fixture()).unwrap();
    let conflict = run(&root, &["reconstruct", "--input", input.to_str().unwrap()]);
    assert!(!conflict.status.success());
    assert!(
        String::from_utf8_lossy(&conflict.stderr)
            .contains("another collector is using this archive")
    );
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    drop(request);
    let good = Server::new(vec![(200, gzip(&fixture()))]);
    assert!(collect(&root, &good).status.success());
    assert_eq!(articles(&root).len(), 4);
    assert!(collect(&root, &good).status.success());
    assert_eq!(good.count.load(Ordering::Relaxed), 1);
}
