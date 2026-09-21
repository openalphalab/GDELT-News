use flate2::{Compression, write::GzEncoder};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, io::Write, path::Path, process::Command};
use tempfile::TempDir;

fn fixture(root: &Path, valid_best_effort: bool) -> std::path::PathBuf {
    let hash = "a".repeat(64);
    let common = json!({"schema_version":3,"url":"https://example.org/story","date":"2025-03-16T00:01:00Z",
        "raw_path":"raw/sample.json.gz","raw_sha256":hash,"completeness":"not_verified"});
    let mut a = common.clone();
    a.as_object_mut().unwrap().extend(
        json!({"segmentation_type":1,"algorithm":"type1-trie-v1","language":"en",
        "text":"Main story","input_fragments":3,"merged_fragments":1,
        "unmerged":[{"pos":20,"text":"Another section"},{"pos":20,"text":"section ends"}]})
        .as_object()
        .unwrap()
        .clone(),
    );
    let mut b = common;
    b.as_object_mut().unwrap().extend(json!({"segmentation_type":2,"algorithm":"type2-adaptive-overlap-v3","language":"zh",
        "text":"新闻报道","input_fragments":2,"merged_fragments":1,"unmerged":[{"pos":40,"text":"其他材料"}],
        "type2_diagnostics":{"search_status":"not_needed","best_effort":{
            "text":if valid_best_effort {"新闻报道\n\n其他材料"} else {"新闻报道"}, "ordering":"estimated",
            "position_only_joins":1,"comparison_budget_exhausted":false}}}).as_object().unwrap().clone());
    let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
    for row in [a, b] {
        writeln!(&mut gzip, "{row}").unwrap();
    }
    let bytes = gzip.finish().unwrap();
    let file = root.join(format!("{hash}.articles.jsonl.gz"));
    fs::write(&file, &bytes).unwrap();
    fs::write(
        root.join(format!("{hash}.manifest.json")),
        json!({"output_sha256":format!("{:x}",Sha256::digest(&bytes)),
        "raw":{"sha256":hash,"path":"raw/sample.json.gz"},"profile":"test",
        "counts":{"articles":2,"selected_fragments":5}})
        .to_string(),
    )
    .unwrap();
    file
}

fn export(input: &Path, out: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_gdelt-export"))
        .arg("--articles")
        .arg(input)
        .arg("--output-directory")
        .arg(out)
        .args(extra)
        .output()
        .unwrap()
}

#[test]
fn exports_both_types_and_refuses_to_overwrite() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(tmp.path(), true);
    let out = tmp.path().join("export");
    let result = export(&input, &out, &[]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let bytes = fs::read(out.join("observations.jsonl")).unwrap();
    let rows: Vec<Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).unwrap())
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["meta"]["type1"], 1);
    assert_eq!(rows[0]["meta"]["type2"], 1);
    assert_eq!(rows[1]["text"], "Main story\n\nAnother section ends");
    assert_eq!(rows[2]["text"], "新闻报道\n\n其他材料");
    assert!(!export(&input, &out, &[]).status.success());
    assert_eq!(fs::read(out.join("observations.jsonl")).unwrap(), bytes);
}

#[test]
fn checksum_and_lost_evidence_fail_without_publishing() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(tmp.path(), false);
    let out = tmp.path().join("export");
    let result = export(&input, &out, &[]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("lost residual evidence"));
    assert!(!out.exists());
    fs::write(&input, "corrupted").unwrap();
    let result = export(&input, &out, &[]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("checksum mismatch"));
    assert!(!out.exists());
}

#[test]
fn invalid_export_limit_fails_before_publishing() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(tmp.path(), true);
    let out = tmp.path().join("export");
    assert!(
        !export(&input, &out, &["--max-expanded-mib", "0"])
            .status
            .success()
    );
    assert!(!out.exists());
}
