"""Compare a combined run with independent Type 1/Type 2 baseline outputs."""
import argparse
import gzip
import json
from pathlib import Path


def read(path, kind):
    rows = {}
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for row in map(json.loads, stream):
            if row["segmentation_type"] != kind:
                continue
            key = (row["url"], row["date"], row["language"])
            assert key not in rows, ("duplicate observation", key)
            rows[key] = row
    assert rows, ("empty baseline or combined output", path, kind)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--type1-baseline", type=Path, required=True)
    parser.add_argument("--type2-baseline", type=Path, required=True)
    parser.add_argument("--combined", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    old = read(args.type1_baseline, 1)
    current = read(args.combined, 1)
    assert old.keys() == current.keys(), "Type 1 observations changed"
    for key in old:
        for field in ("text", "input_fragments", "merged_fragments", "unmerged", "raw_sha256"):
            assert old[key][field] == current[key][field], (key, field)
    standalone = read(args.type2_baseline, 2)
    combined = read(args.combined, 2)
    assert standalone == combined, "Type 2 standalone/combined outputs differ"
    report = {
        "combined_profile": args.combined.parent.name,
        "type1_regression_matches": len(old),
        "type2_standalone_combined_matches": len(standalone),
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
