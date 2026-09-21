"""Compare single-thread reconstruction on identical, real Type 1 fragments.

Requires the upstream checkout (pinned revision in NOTICE.md) and its tqdm
dependency. Network/download, JSON parsing, storage and cleanup are excluded
from both timed kernels. Rust additionally retains unused fragments.
"""
import argparse
import gzip
import hashlib
import importlib.util
import json
import platform
import statistics
import subprocess
import tempfile
import time
from collections import defaultdict
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--upstream", required=True, type=Path)
    parser.add_argument("--rust-benchmark", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("benchmark-results.json"))
    parser.add_argument("--max-articles", type=int, default=0,
                        help="Evenly spaced deterministic sample; 0 benchmarks every article")
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("wordmatch", args.upstream / "src/gdeltnews/wordmatch.py")
    upstream = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(upstream)
    groups = defaultdict(list)
    opener = gzip.open if args.input.suffix == ".gz" else open
    with opener(args.input, "rt", encoding="utf-8") as source:
        for line in source:
            row = json.loads(line)
            if row["type"] != 1:
                continue
            f = upstream._transform_entry(row)
            groups[(row["url"], row["date"], row["lang"])].append({"pos": f["pos"], "text": f["sentence"]})
    ordered = [sorted(groups[key], key=lambda f: f["pos"]) for key in sorted(groups)]
    total_articles = len(ordered)
    if args.max_articles > 0 and len(ordered) > args.max_articles:
        ordered = [ordered[i * total_articles // args.max_articles] for i in range(args.max_articles)]
    prepared = [([f["text"] for f in group], [f["pos"] for f in group]) for group in ordered]
    python_seconds = []
    for _ in range(3):
        started = time.perf_counter()
        texts = [upstream.reconstruct_sentence(fragments, positions) for fragments, positions in prepared]
        python_seconds.append(time.perf_counter() - started)
        print(f"Python run {len(python_seconds)}: {python_seconds[-1]:.3f}s", flush=True)
    checksum = hashlib.sha256()
    for text in texts:
        checksum.update(text.encode("utf-8") + b"\0")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "fragments.json"
        path.write_text(json.dumps(ordered, ensure_ascii=False), encoding="utf-8")
        result = subprocess.run([str(args.rust_benchmark.resolve()), str(path)], check=True, capture_output=True, text=True)
    rust = json.loads(result.stdout)
    assert checksum.hexdigest() == rust["text_sha256"], "Rust/upstream text mismatch"
    report = {
        "input": args.input.name,
        "input_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(),
        "upstream_revision": subprocess.check_output(["git", "-C", str(args.upstream), "rev-parse", "HEAD"], text=True).strip(),
        "platform": platform.platform(), "python_version": platform.python_version(),
        "articles": len(ordered), "total_source_articles": total_articles,
        "sampling": "all" if len(ordered) == total_articles else "evenly spaced URL-sorted groups",
        "fragments": sum(map(len, ordered)),
        "python_seconds": python_seconds, "rust_seconds": rust["seconds"],
        "median_kernel_speedup": statistics.median(python_seconds) / statistics.median(rust["seconds"]),
        "identical_core_text": True, "text_sha256": checksum.hexdigest(),
        "scope": "Single-thread recovery only; three warm runs; no network, parsing, disk or upstream cleanup. Rust retains unmerged fragments.",
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
