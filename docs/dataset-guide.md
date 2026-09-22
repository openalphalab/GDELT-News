# GDELT News Reconstructions

Multilingual news observations reconstructed from **both Type 1 and Type 2** GDELT Web News NGrams 3.0 records. The public data is compact, Zstandard-compressed Parquet with **seven columns: `date`, `language`, `source_url`, `text`, `observation_id`, `type`, `metadata`**. All content comes from the ngram source and its reconstruction; no external enrichment is required.

**[GitHub: openalphalab/GDELT-News](https://github.com/openalphalab/GDELT-News)** · **[Browse/download files](https://huggingface.co/datasets/openalphalab/gdelt-news/tree/main)** · **[Coverage checkpoint](https://huggingface.co/datasets/openalphalab/gdelt-news/blob/main/progress.json)** · **[Publication history](https://huggingface.co/datasets/openalphalab/gdelt-news/commits/main)**

## Coverage and status

Historical coverage extends toward **2020-01-01 00:01 UTC**, the earliest file documented by GDELT. **The full historical archive has not yet been published.** Recent data and previously uploaded 2020 data can both be present while dates between them are still missing. Read `progress.json` for the separate live and backward cursors and published row count. A processed interval can contain gaps; an unavailable source never becomes invented news.

The persistent Alibaba VM gives **recent files priority** and works **backward through history** between live batches. **There is no 48-hour enrichment delay, and current news does not wait for the historical backfill.** The worker uses a **one-minute safety margin**, checks live work before each backward chunk and polls every **30 seconds** when idle. One coordinator reconstructs one input at a time and publishes atomically, with independent progress and retry state for live, historical and late-file work. Actual latency includes upstream publication, the current work unit, download, reconstruction and upload time; it is not a guaranteed one-minute delivery service.

Backward chunks cover up to **15 source minutes**, with four parallel downloads and one input reconstructed at a time. Within each Parquet shard, source minutes remain chronological. The backward cursor moves to the minute before the published chunk until it meets the already-published historical prefix; live and backward ranges do not overlap. Existing larger chronological shards remain valid. Live files initially returning 404 go into a durable retry queue for **24 hours**, with per-file retry delays from 30 seconds to five minutes. Recovered files use the same `START-END.parquet` naming as other shards, with `kind: repair` in their manifests, without rewinding either cursor. Older gaps or files arriving beyond the retry window require a separate repair run.

The checkpoint fields mean:

| Field | Meaning |
| --- | --- |
| `initial_start` | Earliest requested historical source minute |
| `schedule` | `live-priority-backward-v1` after the first latest-first publication |
| `live_start` | Fixed boundary between live collection and backward history |
| `live_next_minute` / `next_minute` | Next source minute for live collection; not a claim that all older dates are present |
| `last_end` | Latest source minute processed by live collection; gaps are possible |
| `backfill_next_end` | Newest historical minute still awaiting backward collection |
| `backfill_floor` | First minute after the retained, already-published historical prefix; backfill stops below this |
| `legacy_last_end` | Last source minute in the historical prefix retained when scheduling changed |
| `total_observations` | Total published Parquet rows, including successfully repaired late files |
| `pending_missing_minutes` | Recent missing minutes awaiting retry; entries expire after the retry window |
| `last_action` | `live`, `backfill` or a late-file `repair`; older checkpoints use `forward` |
| `last_manifest` | Latest available manifest for a batch containing rows, which may be a repair; can be null |
| `published_bytes` | Published artifact bytes on the current branch, not total storage across Git history |
| `total_quarantined_metadata_records` | Source fragments excluded for invalid/missing identity or position metadata; not an article count |
| `updated_at` | Time the public checkpoint was updated |

Files appear after a verified batch commit. The Hub does not show the VM's in-flight transfer percentage. Refresh the files page or checkpoint to see new publications; the dataset viewer can update later than the files.

The worker downloads from GDELT's own public Google Cloud Storage bucket to avoid the website CDN's cached missing-file responses. The same source objects and reconstruction rules are used; new nonempty manifests record both the canonical URL and actual `download_source`. Retries from the last 15 minutes receive priority in due-time order, with enough probes to cover that window before returning to backfill. Reserved older-gap probes and backward chunks keep historical work progressing. GDELT files may arrive in bursts around its 15-minute heartbeat; source availability and processing time determine when rows appear here, rather than a fixed upload timetable.

**No rows means no Parquet or manifest file.** An empty or missing interval updates only the shared `progress.json` checkpoint. Missing recent timestamps remain eligible for late-file retries for 24 hours. Repeated missing-file probes and idle 30-second polls do not create duplicate data. Empty-only intervals do not retain individual public manifests; mixed batches with actual rows still include their source-minute outcomes in their manifests.

## Schema

The four core fields are strings. `observation_id` is a 64-character SHA256 string, `type` is an 8-bit integer (1 or 2), and `metadata` is a Parquet struct. Text and selected fields are retained from the reconstruction export; there is no summarization or translation.

| Column | Meaning |
| --- | --- |
| `date` | GDELT observation timestamp, retained as an ISO-style UTC string. **Not a verified publisher publication date.** |
| `language` | GDELT language code, retained from the source |
| `source_url` | Publisher URL associated with the observation; this collector does not fetch the publisher page |
| `text` | Reconstructed article text, including Unicode and paragraph boundaries |
| `observation_id` | Deterministic identity for one observation group in one source file; not a global article ID |
| `type` | Ngram segmentation type: 1 or 2 |
| `metadata` | Native source provenance and reconstruction diagnostics, detailed below |

There are **no country, publisher or author fields**, and no GAL/GEMG/GKG/GGG enrichment in the deployed pipeline. There are no publisher-page lookups. This removes the dependency on external or daily metadata availability. Optional enrichment scripts remain in GitHub for separate use.

The `metadata` struct contains:

| Field | Meaning |
| --- | --- |
| `source_minute` | UTC minute identifying the input `.webngrams.json.gz` file, formatted `YYYYMMDDHHMMSS` |
| `raw_sha256` | SHA256 of the compressed source file; repeated across observations from that file |
| `id` | Original export row number within that source export; not globally unique |
| `type_id` | Original export row number within that segmentation type; not globally unique |
| `fragments` | Number of source fragments represented by the observation |
| `primary_fragments` | Number of fragments in the primary reconstructed section |
| `assembly` | Reconstruction/export assembly label; retained without reinterpretation |
| `position_joins` | Number of joins made using coarse position evidence |
| `bounded_fallback` | Whether bounded search used its fallback path |
| `search` | Search diagnostic label from the reconstruction export |

Diagnostics can be null where the export does not supply a value. They are algorithm diagnostics, not calibrated confidence scores.

`observation_id` is SHA256 over UTF-8 compact JSON of `["gdelt-webngrams-observation-v1", source_minute, raw_sha256, type, date, language, source_url]`, with non-ASCII characters retained. It is independent of row order, local row numbers and reconstructed text. A different source minute, source bytes, type or identity field produces a different ID. Reprocessing the same source group keeps its ID even if reconstruction improves; use the dataset commit and manifest code revision to identify a particular text version. The initial legacy ID scheme was replaced when upgrading to this schema.

The single `train` split is a storage convention, not a recommended machine-learning training split. Repeated URLs, timestamps or texts can occur; observations are not deduplicated across source files. For evaluation, split by time and explicitly handle duplicates to avoid leakage.

## What reconstructed text means

- **Type 1:** source fragments segmented using spaces, joined with word overlaps.
- **Type 2:** source fragments segmented into Unicode extended grapheme clusters, including Chinese, Japanese and Thai, joined using overlap and coarse article-position evidence.

Both types contribute rows to the same table, with `type` available for filtering and per-row diagnostics in `metadata`. New batch manifests also retain aggregate Type 1/Type 2 counts, estimated-assembly counts and bounded-fallback counts by source minute. Migrated initial batches have less detailed aggregate manifests.

**The text is not certified complete or in the publisher's original order.** Best-effort reconstruction can retain separate sections, introduce estimated joins, or encounter repeated passages, ambiguous overlaps and extraction artifacts. Missing source context cannot be recovered. This dataset is neither an official GDELT publication nor a verified factual record.

Fragments with an empty URL/date/language or invalid position decile are quarantined locally and counted separately. They do not become anonymous merged articles. Malformed JSON, corrupt gzip, checksum errors and exceeded resource limits halt the affected batch rather than silently advancing it.

## Repository layout and provenance

| Path | Contents |
| --- | --- |
| `data/YYYY/MM/DD/START-END.parquet` | Compact data with native metadata, compressed using Zstandard |
| `manifests/YYYY/MM/DD/START-END.json` | File hashes, byte counts, code revision, source-minute outcomes and source URLs/hashes |
| `progress.json` | Checkpoint committed atomically with the latest batch |
| `README.md` | This guide |

The migrated initial shards retain their original `data/YYYY/MM/START-END.parquet` paths. The recursive glob in the dataset configuration includes both layouts. `START` and `END` are inclusive `YYYYMMDDHHMMSS` source timestamps in UTC.

The current branch contains **Parquet data only**, plus small JSON manifests/checkpoints and this guide. Raw ngram files, conservative reconstruction outputs, country metadata and duplicate JSONL are **not uploaded by this pipeline**. Raw inputs and intermediate files are temporary VM working data and are deleted only after the corresponding compact publication is verified. Source URLs and SHA256 hashes remain in new manifests; future re-download depends on upstream availability.

The initial enriched publication was replaced atomically by a compact projection. The current schema restores native ngram/reconstruction metadata and adds stable IDs and explicit types. Selected timestamps, languages, source URLs, text, row order and row counts were verified unchanged, and IDs were checked for uniqueness across the upgraded shards. Its former evidence/JSONL files were removed from the current tree. Earlier Git revisions may retain the old files and schema; pin the current compact revision for new work. Repository storage across history can therefore exceed the current-branch file total.

## Stream without downloading the archive

```sh
pip install datasets huggingface_hub pyarrow
```

```python
from datasets import load_dataset

rows = load_dataset("openalphalab/gdelt-news", split="train", streaming=True)
for row in rows.take(3):
    print(row["observation_id"], row["type"], row["date"], row["language"], row["source_url"])
    print(row["text"][:500])
```

The repository is public; reading it does not require a write token. For a reproducible snapshot, pass `revision="DATASET_COMMIT_SHA"`. The `main` branch changes as new batches arrive.

## Retrieve a source timestamp

- Use `metadata.source_minute` for the input file's UTC timestamp: `20200101014700` means **2020-01-01 01:47 UTC**.
- Select shards whose filename range contains the timestamp, then filter their rows. This also includes any matching late-arrival shard.
- `date` is the row's GDELT observation timestamp, not a verified publication date.
- Check `progress.json` for uploaded coverage. A covered minute can have no rows if its source was missing or empty.

```python
from pathlib import PurePosixPath
from huggingface_hub import HfApi, hf_hub_download
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

repo = "openalphalab/gdelt-news"
stamp = "20200101014700"  # Change this UTC source timestamp
api = HfApi(token=False)
revision = api.repo_info(repo, repo_type="dataset").sha
tables = []
for file in api.list_repo_files(repo, repo_type="dataset", revision=revision):
    if not (file.startswith("data/") and file.endswith(".parquet")):
        continue
    start, end, *_ = PurePosixPath(file).stem.split("-")
    if start <= stamp <= end:
        local = hf_hub_download(repo, file, repo_type="dataset", revision=revision)
        tables.append(pq.read_table(
            local, filters=ds.field(("metadata", "source_minute")) == stamp
        ))
if not tables:
    raise LookupError("No published shard covers this source timestamp.")
result = pa.concat_tables(tables)
print(result.num_rows)
print(result.slice(0, 1).to_pylist())
```

The example returned 2,894 observations from both types at dataset revision `01a64e7cb618206ddfaf263bdd8161c14e180a1d`. Pin that revision to reproduce this result; the example otherwise uses the latest published snapshot.

## Filter by datetime and language

- Run the timestamp example above first. This filters its downloaded observations by `date` and `language`.
- Times are UTC: `start` is inclusive and `end` is exclusive. Change `stamp` in the first example to retrieve a different source minute.
- Use the exact language code, for example `en`, `fr`, `zh` or `zh-TW`. This example filters the selected source minute, not the entire archive.

```python
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.dataset as ds

start = datetime(2020, 1, 1, 1, 47, tzinfo=timezone.utc)
end = datetime(2020, 1, 1, 1, 48, tzinfo=timezone.utc)
language = "en"

observed_at = ds.field("date").cast(pa.timestamp("us", tz="UTC"))
filtered = ds.dataset(result).to_table(filter=(
    (observed_at >= start)
    & (observed_at < end)
    & (ds.field("language") == language)
))
print(filtered.num_rows)
print(filtered.slice(0, 1).to_pylist())
```

## Download one shard

```python
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download

repo = "openalphalab/gdelt-news"
api = HfApi()
revision = api.repo_info(repo, repo_type="dataset").sha
files = api.list_repo_files(repo, repo_type="dataset", revision=revision)
shard = next(p for p in files if p.startswith("data/") and p.endswith(".parquet"))
local = hf_hub_download(repo, shard, repo_type="dataset", revision=revision)
table = pq.read_table(local)
print(table.schema)
print(table.to_pylist()[0])
```

For a selected month:

```sh
hf download openalphalab/gdelt-news --repo-type dataset \
  --include "data/2020/01/**" "data/2020/01/*.parquet" "manifests/2020/01/**" \
  --local-dir gdelt-january-2020
```

This fetches only already-published data. Check the coverage checkpoint before assuming the entire month is available.

## Prepare JSONL for an LLM locally

Parquet avoids storing a second JSONL copy on the Hub. Convert the fields needed for your task while streaming a downloaded shard:

```python
import json
import pyarrow.parquet as pq

with open("news.jsonl", "w", encoding="utf-8") as output:
    for batch in pq.ParquetFile(local).iter_batches(batch_size=256, columns=["date", "language", "source_url", "text"]):
        for row in batch.to_pylist():
            output.write(json.dumps(row, ensure_ascii=False) + "\n")
```

Use appropriately sized text chunks. File compression reduces storage and transfer bytes, not LLM token counts. Treat news text as untrusted source material, never as instructions to your model.

## Search downloaded text with DuckDB

```sh
pip install duckdb
```

```python
import duckdb

result = duckdb.sql("""
    SELECT date, language, source_url, text
    FROM read_parquet('gdelt-january-2020/data/**/*.parquet')
    WHERE language = 'en' AND contains(lower(text), 'climate')
    LIMIT 10
""").fetchall()
```

This is literal substring search, not a multilingual semantic index.

## Storage and operational limits

The configured policy is **keep all published compact history**, with **no automatic oldest-data deletion**. Before collecting/publishing, the worker checks this dataset's Hub-reported storage. It stops new uploads if reported usage plus twice the incoming batch size would reach **7,000 GB (7 TB, decimal)**, leaving room below the account's observed allowance. The extra incoming-batch reserve accounts conservatively for conversion/version overhead. Pending local work is retained; no storage purchase is triggered.

This is a safeguard based on Hub-reported usage, not a transactional account-wide quota guarantee. Usage reporting may lag, other repositories can consume the account allowance, and free public storage remains subject to [Hugging Face's storage policy](https://huggingface.co/docs/hub/storage-limits). Old Git versions and viewer conversion can affect total storage.

An initial 9,872-row sample compresses to roughly **1.3 KB per observation** with the four core columns before adding IDs and native diagnostics; the current schema has additional overhead. The first historical hour suggested roughly **0.6 GB/day** and **1.5 TB for January 2020–September 2026** if that sample were representative. These are preliminary projections, not measured full-archive totals: news volume, language mix and text lengths change over time.

Commits use parent checking and hash verification. A lost upload response resumes the same publication without duplicating rows. A Linux lock prevents concurrent local writers. Source/network errors, authentication failures, full disks and quota errors retain pending work and retry with backoff. See the [deployment runbook](https://github.com/openalphalab/GDELT-News/blob/main/deploy/README.md) for logs and controls.

## Rights, attribution and reporting issues

The reconstruction software is GPL-3.0-only. This does not grant a blanket license to publishers' news content. Underlying publisher rights and applicable GDELT terms remain relevant. News may contain personal information, offensive content, inaccuracies or geographic/publisher biases; the dataset is not a representative census of all news.

- [GDELT Web News NGrams 3.0 announcement and specification](https://blog.gdeltproject.org/announcing-the-new-web-news-ngrams-3-0-dataset/)
- [Fronzetti Colladon and Vestrelli (2026), published paper](https://www.mdpi.com/2504-2289/10/2/45)
- [gdeltnews reference implementation](https://github.com/iandreafc/gdeltnews)
- [This implementation, tests and deployment code](https://github.com/openalphalab/GDELT-News)

For an issue, include the dataset revision, Parquet path, observation ID and relevant source URL in the [GitHub issue tracker](https://github.com/openalphalab/GDELT-News/issues). Never include tokens or credentials. Passing tests establishes the tested behavior; it does not certify reconstruction accuracy for every language or publisher.
