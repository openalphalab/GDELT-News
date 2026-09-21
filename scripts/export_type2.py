"""Export readable Type 2 observations and a problem list from collector output."""
import argparse
import gzip
import json
from pathlib import Path


def literal(text):
    fence = "```"
    while fence in text:
        fence += "`"
    return [fence + "text", text, fence]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--articles", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args()
    with gzip.open(args.articles, "rt", encoding="utf8") as stream:
        rows = [r for r in map(json.loads, stream) if r["segmentation_type"] == 2]
    assert rows and all("best_effort" in r["type2_diagnostics"] for r in rows), "Run with --type2-best-effort first"
    args.output_directory.mkdir(parents=True, exist_ok=True)
    problems = [(i, r) for i, r in enumerate(rows, 1) if r["unmerged"]]
    exports = []
    report = [f"# {len(rows)} observations: one best-effort text each", "",
              "Each observation has one text containing all its supplied fragments. Overlap-based joins use exact grapheme matches; remaining joins use estimated source-position order and paragraph breaks. Original publisher order and completeness are not verified.", ""]
    problem_report = [f"# {len(problems)} observations with uncertain continuity", "",
                      "All now have a best-effort combined text. The counts below describe the conservative reconstruction, before estimated joins.", "",
                      "| Observation | Language | Main / total fragments | Overlap joins | Contained sections | Position-only joins | Source |",
                      "| --- | --- | ---: | ---: | ---: | ---: | --- |"]
    problem_texts = [f"# {len(problems)} problematic observations: combined text", "",
                     "These texts include estimated joins. All supplied source fragments remain present; source order is not verified.", ""]
    for i, r in enumerate(rows, 1):
        best = r["type2_diagnostics"]["best_effort"]
        export = {k: r[k] for k in ["url", "date", "language", "raw_path", "raw_sha256", "input_fragments", "completeness"]}
        export.update(text=best["text"], assembly="best_effort", ordering=best["ordering"],
                      joins=best["joins"], position_only_joins=best["position_only_joins"],
                      conservative_text=r["text"], conservative_merged_fragments=r["merged_fragments"])
        exports.append(export)
        section = [f"## Observation {i}", "", f"Source: <{r['url']}>", "",
                   f"Language: {r['language']} | Observed: {r['date']} | Source fragments: {r['input_fragments']:,} | Ordering: {best['ordering']} | Position-only joins: {best['position_only_joins']}", ""]
        section += literal(best["text"]) + [""]
        report += section
        if r["unmerged"]:
            problem_texts += section
            methods = [j["method"] for j in best["joins"]]
            problem_report.append(f"| {i} | {r['language']} | {r['merged_fragments']} / {r['input_fragments']} | {methods.count('overlap_estimate')} | {methods.count('contained')} | {best['position_only_joins']} | <{r['url'].replace('|', '%7C')}> |")
    out = args.output_directory
    (out / f"{len(rows)}-best-effort-observations.md").write_text("\n".join(report), encoding="utf8")
    (out / f"{len(rows)}-best-effort-observations.json").write_text(json.dumps(exports, ensure_ascii=False, indent=2) + "\n", encoding="utf8")
    (out / f"{len(problems)}-problematic-observations.md").write_text("\n".join(problem_report) + "\n", encoding="utf8")
    (out / f"{len(problems)}-problematic-best-effort.md").write_text("\n".join(problem_texts), encoding="utf8")
    (out / f"{len(problems)}-problematic-best-effort.json").write_text(json.dumps([exports[i-1] for i, _ in problems], ensure_ascii=False, indent=2) + "\n", encoding="utf8")
    print(json.dumps({"observations": len(rows), "problematic_observations": len(problems), "output_directory": str(out.resolve())}))


if __name__ == "__main__":
    main()
