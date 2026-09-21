---
pretty_name: GDELT News Reconstructions
task_categories:
  - text-retrieval
  - text-generation
  - text-classification
tags:
  - gdelt
  - news
  - multilingual
  - reconstruction
  - provenance
configs:
  - config_name: default
    data_files:
      - split: train
        path: data/**/*.parquet
---

# GDELT News Reconstructions

Multilingual news observations reconstructed from **both Type 1 and Type 2** records in GDELT Web News NGrams 3.0, with source-linked metadata and retained raw evidence.

**Code, tests and deployment instructions:** [openalphalab/GDELT-News on GitHub](https://github.com/openalphalab/GDELT-News) · **[Files](https://huggingface.co/datasets/openalphalab/gdelt-news/tree/main)** · **[Latest coverage checkpoint](https://huggingface.co/datasets/openalphalab/gdelt-news/blob/main/progress.json)** · **[Commit history](https://huggingface.co/datasets/openalphalab/gdelt-news/commits/main)**

## Coverage and publication status

The historical backfill starts with **2020-01-01 00:01 UTC**, the first file documented by GDELT. The first validated source minute contains **902 observations: 820 Type 1 and 82 Type 2**. This describes the initial shard only, not a fixed total for the growing dataset.

**The full historical archive is not yet published.** Read `progress.json` to find the latest committed coverage. `last_end` is the most recent source minute processed; `next_minute` is the next one to attempt; `total_observations` is the number of published rows. Source minutes returning HTTP 404 are recorded in batch manifests and are not counted as recovered news. A processed time range can therefore contain gaps.

The persistent worker runs on an Alibaba Cloud VM. It processes history chronologically, then follows newly available files with a **48-hour lag** to support retrospective daily metadata enrichment. It does not deliver current news while it is still catching up with history. Publications normally contain up to six source hours, with earlier publication when a shard reaches its size or disk-space limit. The first minute is published separately as an end-to-end validation.

The commit history shows completed uploads. Hugging Face does not display the VM's in-flight transfer percentage; files appear after the batch commit finishes. Refresh the files page or checkpoint to see new publications. Availability depends on GDELT source availability, network access and Hugging Face storage policy; no completion date or unlimited storage is promised.

## What the observations represent

GDELT publishes small contexts around words or characters rather than an ordered full article. This project reconstructs text by finding overlaps among those contexts:

- **Type 1:** languages segmented using spaces. Context fragments are joined with the word-based reconstruction engine.
- **Type 2:** languages segmented into Unicode extended grapheme clusters, including Chinese, Japanese and Thai. The engine respects grapheme boundaries and uses overlap and coarse article-position evidence.

The exported `text` contains the best available assembly of the observed fragments. Where evidence cannot establish continuity, best-effort assembly retains separate sections and marks estimated joins. The conservative reconstruction and diagnostics remain in the evidence archive.

**These are reconstructed observations, not certified complete publisher articles.** A connected result does not prove original ordering, completeness or factual correctness. Missing source windows, repeated passages, extraction artifacts and ambiguous overlaps can affect results. The exporter checks that observed reconstruction evidence is retained; it cannot recover content that GDELT never included.

A URL may occur more than once across times or source files. Those observations are deliberately retained rather than treated as distinct verified articles or silently deduplicated. Language coverage follows the source files; the presence or absence of a language in a particular batch is not a coverage guarantee. `observed_at` is GDELT's observation time, not the original publication time.

## Repository layout

| Path | Contents | Intended use |
| --- | --- | --- |
| `data/YYYY/MM/START-END.parquet` | Text, identifiers, language, source URL, quality fields, estimated source country and metadata | Dataset viewer, SQL, analytical queries, streaming |
| `llm/YYYY/MM/START-END.jsonl.gz` | Compact observation JSONL with a provenance header before each source minute | LLM preparation and line-by-line processing |
| `evidence/YYYY/MM/START-END.tar` | Original compressed ngram bytes, conservative reconstruction output, metadata source files and per-minute reports | Auditing and reproducible reprocessing |
| `manifests/YYYY/MM/START-END.json` | SHA256 checksums, file sizes, code revision, row counts and missing source minutes | Integrity and coverage checks |
| `progress.json` | Checkpoint committed together with the latest batch | Published coverage and cumulative counts |

`START` and `END` use `YYYYMMDDHHMMSS` in UTC and are inclusive. The TAR stores already-compressed source files without redundant outer compression. Metadata source files shared by several minutes are included once within a batch. Storage includes raw evidence, so total repository size is larger than reconstructed text alone.

The single `train` split is a storage convention, **not a recommended machine-learning training split**. There is no built-in train/test separation or article-level deduplication. For evaluation, split by time and handle repeated URLs/text explicitly to avoid leakage.

## Parquet schema

| Field | Meaning |
| --- | --- |
| `observation_id` | SHA256 of `raw_sha256:id`; stable for the same raw input, exporter ordering and pipeline |
| `source_minute` | UTC source archive minute, `YYYYMMDDHHMMSS` |
| `raw_sha256` | SHA256 of the original compressed ngram source |
| `id` | One-based observation number within its minute export |
| `type_id` | One-based number within the same segmentation type in that export |
| `type` | `1` for space segmentation, `2` for grapheme segmentation |
| `lang` | GDELT language code, retained from the source |
| `url` | Source publisher URL; the collector does not fetch this page |
| `observed_at` | GDELT observation timestamp |
| `fragments` | Input fragment count for the observation |
| `primary_fragments` | Fragments merged into the conservative primary reconstruction |
| `assembly` | `connected` or `estimated`; neither certifies completeness |
| `position_joins` | Joins of sections whose continuity is unverified; may use position-based ordering |
| `bounded_fallback` | Whether best-effort assembly reached a work limit and used its bounded fallback |
| `search` | Type 2 branch-search status; `not_applicable` for Type 1 |
| `text` | Reconstructed text, including Unicode and paragraph boundaries |
| `source_country_iso2` | Nullable convenience field for estimated outlet country/affinity |
| `metadata_json` | JSON string containing the full enrichment object; decode with `json.loads` |

Numeric observation IDs are local to an export, not persistent publisher identifiers. An algorithm change that changes observation ordering can change `observation_id`; pin a dataset commit and consult the batch's `code_revision` for reproducibility.

## Metadata and country interpretation

Enrichment uses GDELT GAL, GEMG, adjacent GKG windows, same-day English GGG and a historical outlet-country lookup. It preserves available titles, descriptions, authors, publisher labels, themes, people, organizations, tone and geographic annotations. Fields have bounded parser length/count limits described in the source code and enrichment reports; they are not a complete mirror of all fields in the upstream datasets.

`source_country` is **GDELT's estimated outlet origin or affinity**. It is not a verified headquarters address, reporter location, author nationality or event location. The method and evidence are retained. Exact-URL and same-day exact-host GGG matches are preferred; the fallback domain-country table dates from **May 2018** and may be stale. Parent-domain matching uses the Public Suffix List, including private hosting suffixes. Unknown or conflicting values remain null rather than being guessed from a country-code top-level domain. Original FIPS codes are retained alongside explicit ISO mappings.

GKG/GGG `mentioned_countries` describe article content. They must not be substituted for outlet origin. GGG enrichment here uses its English daily file, so country and geographic coverage may differ across languages.

The enrichment is **retrospective**: exact-URL GKG/GGG matches may use observations within 60 minutes, and host-country evidence may use the whole observation day. Consequently, these fields can contain information unavailable at the original observation time. They should not be used as real-time forecasting features without a separate availability-time policy. Publisher-reported dates remain explicitly unverified, and `GAL.date` is not treated as guaranteed publication time.

Within JSONL minute headers and evidence reports, metadata source indices resolve to source URLs, checksums and availability records. A Parquet row's `metadata_json` uses those same per-minute indices: use `source_minute` to locate the corresponding evidence report or JSONL header. Missing upstream datasets are disclosed; absent metadata is not evidence that an entity, country or fact was absent from the article.

## Quick start: stream without downloading the archive

Install the libraries you need:

```bash
pip install datasets huggingface_hub pyarrow
```

```python
from datasets import load_dataset

rows = load_dataset("openalphalab/gdelt-news", split="train", streaming=True)
for row in rows.take(3):
    print(row["source_minute"], row["type"], row["lang"], row["url"])
    print(row["text"][:500])
```

Public downloads do not require a write token. For repeatable results, pass a commit SHA using `revision="DATASET_COMMIT_SHA"`. The streaming example follows the currently published `main` branch by default.

## Download one shard and inspect its metadata

```python
import json
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download

repo = "openalphalab/gdelt-news"
api = HfApi()
revision = api.repo_info(repo, repo_type="dataset").sha
files = api.list_repo_files(repo, repo_type="dataset", revision=revision)
shard = next(p for p in files if p.startswith("data/") and p.endswith(".parquet"))
local = hf_hub_download(repo, shard, repo_type="dataset", revision=revision)
row = pq.read_table(local).to_pylist()[0]
print(row["text"])
print(json.loads(row["metadata_json"]))
```

## Download a selected month

```bash
hf download openalphalab/gdelt-news --repo-type dataset \
  --include "data/2020/01/*.parquet" "manifests/2020/01/*.json" \
  --local-dir gdelt-january-2020
```

This downloads only files already published. Add `"llm/2020/01/*.jsonl.gz"` for compact JSONL or `"evidence/2020/01/*.tar"` for raw evidence. Avoid downloading the entire repository unless you have checked its size and have enough disk space. These paths use source dates, not upload dates.

## Read the compact JSONL for an LLM

```python
import gzip
import json
from huggingface_hub import hf_hub_download

path = hf_hub_download(
    "openalphalab/gdelt-news",
    "llm/2020/01/20200101000100-20200101000100.jsonl.gz",
    repo_type="dataset",
)
minute_meta = None
with gzip.open(path, "rt", encoding="utf-8") as stream:
    for line in stream:
        item = json.loads(line)
        if "meta" in item:
            minute_meta = item
            continue
        # Build prompts from only the fields needed for your task.
        prompt_input = {"url": item["url"], "lang": item["lang"], "text": item["text"]}
        print(json.dumps(prompt_input, ensure_ascii=False))
        break
```

Each minute has its own header; not every JSONL line is an observation. Feed the selected text and metadata to an LLM in appropriately sized chunks rather than submitting a whole batch. Compression reduces network/storage bytes, **not model token counts**. Treat news text as untrusted source material, not instructions to the model.

## Search downloaded text with DuckDB

```bash
pip install duckdb
```

```python
import duckdb

results = duckdb.sql("""
    SELECT observed_at, lang, url, source_country_iso2, assembly, text
    FROM read_parquet('gdelt-january-2020/data/2020/01/*.parquet')
    WHERE lang = 'en' AND contains(lower(text), 'climate')
    LIMIT 10
""").fetchall()
```

This is a literal substring example, not a multilingual semantic search index. Use the language, date and quality fields to select observations appropriate to your application.

## Integrity, reproducibility and update behavior

Batch manifests contain each data artifact's byte count and SHA256 checksum. Verify downloaded artifacts before auditing or reprocessing them. The worker retains local source files until the upload commit and remote hashes have been checked. Data and the progress checkpoint are committed together; interrupted uploads retry the pending batch. Optimistic commit checks prevent a second writer from silently replacing another worker's checkpoint.

The GitHub repository includes the Rust collector/exporter, Type 2 regression and Unicode tests, metadata parsers, worker recovery tests and deployment configuration. See [deployment instructions](https://github.com/openalphalab/GDELT-News/blob/main/deploy/README.md) and [reconstruction documentation](https://github.com/openalphalab/GDELT-News/blob/main/README.md). A successful test suite does not establish accuracy for every publisher, language or historical interval. Inspect the per-observation diagnostics for your use case.

Historical 404s are documented rather than silently filled. GDELT may backfill a missing source later; this chronological worker does not automatically revisit every old 404. A separate repair run should use the recorded missing-minute lists. Corrections or algorithm upgrades should retain explicit version provenance rather than being mistaken for new publisher articles.

## Rights, attribution and intended use

The reconstruction **software** is licensed GPL-3.0-only. That software license does not assign a blanket license to this dataset's third-party news content. Underlying publisher rights and applicable GDELT terms remain relevant; users must assess permitted use and redistribution for their application. News can contain personal information, offensive language, inaccuracies and publisher or geographic biases. This dataset is not a verified factual record or a representative sample of all world news.

Sources and references:

- [GDELT Web News NGrams 3.0 announcement and data specification](https://blog.gdeltproject.org/announcing-the-new-web-news-ngrams-3-0-dataset/).
- [Fronzetti Colladon and Vestrelli (2026), published paper](https://www.mdpi.com/2504-2289/10/2/45), and the [gdeltnews reference implementation](https://github.com/iandreafc/gdeltnews).
- [This implementation: openalphalab/GDELT-News](https://github.com/openalphalab/GDELT-News). It is not an official GDELT publication.
- GeoNames-derived FIPS/ISO crosswalk, attributed under CC BY 4.0 in [NOTICE.md](https://github.com/openalphalab/GDELT-News/blob/main/NOTICE.md).
- [Public Suffix List](https://publicsuffix.org/list/) for domain boundaries.

To report an issue, include the dataset commit, batch manifest path, source minute and observation ID in the [GitHub issue tracker](https://github.com/openalphalab/GDELT-News/issues). Never include access tokens or other credentials.
