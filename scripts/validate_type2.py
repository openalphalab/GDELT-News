"""Independently check a Type 2 source/output pair; no publisher text required.

This verifies evidence accounting and substring provenance, not reconstruction
accuracy or original-article completeness. Uses only Python's standard library.
"""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import unicodedata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--articles", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = defaultdict(list)
    with gzip.open(args.raw, "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            if row["type"] == 2:
                key = (row["url"], row["date"], row["lang"])
                text = unicodedata.normalize("NFC", row["pre"] + row["ngram"] + row["post"])
                source[key].append((row["pos"], text))
    for fragments in source.values():
        fragments.sort(key=lambda fragment: fragment[0])
    languages = Counter()
    stops = Counter()
    searches = Counter()
    seen = set()
    merged = unused = characters = 0
    segment_count = segment_characters = search_steps = 0
    best_effort_count = 0
    estimated_joins = Counter()
    sha256 = hashlib.sha256(args.raw.read_bytes()).hexdigest()
    with gzip.open(args.articles, "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            if row["segmentation_type"] != 2:
                continue
            key = (row["url"], row["date"], row["language"])
            assert key not in seen, ("duplicate observation", key)
            seen.add(key)
            ordered = source[key]
            fragments = Counter(ordered)
            assert row["raw_sha256"] == sha256
            assert sum(fragments.values()) == row["input_fragments"]
            leftovers = Counter((f["pos"], f["text"]) for f in row["unmerged"])
            assert all(fragments[f] >= n for f, n in leftovers.items())
            placed = fragments - leftovers
            assert sum(placed.values()) == row["merged_fragments"]
            assert all(text in row["text"] for _, text in placed)
            assert row["input_fragments"] == row["merged_fragments"] + len(row["unmerged"])
            assert row["completeness"] == "not_verified"
            diagnostics = row["type2_diagnostics"]
            indices = diagnostics["primary_fragment_indices"]
            assert len(indices) == row["merged_fragments"]
            assert all(isinstance(i, int) and 0 <= i < len(ordered) for i in indices)
            assert Counter(ordered[i] for i in indices) == placed
            assigned = list(indices)
            residual = []
            for segment in diagnostics["segments"]:
                ids = segment["fragment_indices"]
                assert ids and all(isinstance(i, int) and 0 <= i < len(ordered) for i in ids)
                assert all(ordered[i][1] in segment["text"] for i in ids)
                assert segment["min_pos"] == min(ordered[i][0] for i in ids)
                assert segment["max_pos"] == max(ordered[i][0] for i in ids)
                assigned.extend(ids)
                residual.extend(ids)
                segment_count += 1
                segment_characters += len(segment["text"])
            assert sorted(assigned) == list(range(len(ordered))), "Missing or duplicated fragment IDs"
            assert Counter(ordered[i] for i in residual) == leftovers
            if best := diagnostics.get("best_effort"):
                assert all(text in best["text"] for _, text in fragments), "Best-effort text lost evidence"
                assert sorted(j["section"] for j in best["joins"]) == list(range(len(diagnostics["segments"]) + 1))
                methods = Counter(j["method"] for j in best["joins"])
                assert methods["seed"] == 1
                assert methods["position_only"] == best["position_only_joins"]
                assert all(j["method"] in {"seed", "position_only", "overlap_estimate", "contained"} for j in best["joins"])
                estimated_joins.update(methods)
                best_effort_count += 1
            assert 0 <= diagnostics["search_steps"] <= diagnostics["search_budget"]
            searches[diagnostics["search_status"]] += 1
            search_steps += diagnostics["search_steps"]
            languages[row["language"]] += 1
            stops[diagnostics["stop_reason"]] += 1
            merged += row["merged_fragments"]
            unused += len(row["unmerged"])
            characters += len(row["text"])
    assert seen == set(source), "Use unfiltered Type 2 outputs to validate the entire source"
    report = {
        "raw_sha256": sha256,
        "articles_sha256": hashlib.sha256(args.articles.read_bytes()).hexdigest(),
        "type2_observations": len(seen), "language_observations": dict(languages),
        "input_fragments": merged + unused, "merged_fragments": merged,
        "unmerged_fragments": unused, "recovered_codepoints": characters,
        "residual_segments": segment_count, "residual_segment_codepoints": segment_characters,
        "search_statuses": dict(searches), "search_steps": search_steps,
        "stop_reasons": dict(stops), "all_fragments_accounted_for": True,
        "all_placed_fragments_present_in_text": True,
        "all_fragments_assigned_to_exactly_one_section": True,
        "all_segment_fragments_present_in_segment_text": True,
        "best_effort_observations": best_effort_count,
        "best_effort_join_methods": dict(estimated_joins),
        "all_best_effort_fragments_present": True if best_effort_count else None,
        "scope": "Source evidence checks; not a publisher-ground-truth accuracy measurement",
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
