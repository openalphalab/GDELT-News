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
  - parquet
configs:
  - config_name: default
    data_files:
      - split: train
        path: data/**/*.parquet
---

# GDELT News Reconstructions

- **Content:** multilingual news text reconstructed from GDELT Web News NGrams 3.0, including **Type 1 and Type 2**.
- **Format:** Zstandard-compressed **Parquet only**, with small manifests and a coverage checkpoint.
- **Columns:** `date`, `language`, `source_url`, `text`, `observation_id`, `type`, `metadata`.
- **Metadata:** source-file timestamp, source checksum and reconstruction diagnostics. Observation IDs are stable for the same source group. No country, publisher, author or external enrichment.
- **Coverage:** historical backfill starts at **2020-01-01 00:01 UTC** and is still in progress. See [progress.json](https://huggingface.co/datasets/openalphalab/gdelt-news/blob/main/progress.json) for published coverage and row counts; gaps are possible.
- **Updates:** no 48-hour enrichment delay. After backfill catches up, the worker polls every **30 seconds** with a **one-minute safety margin**; upstream and processing time add latency.
- **Retrieve a timestamp:** for `20200101014700` (**2020-01-01 01:47 UTC**), select Parquet filenames whose `START-END` range covers it, then filter `metadata.source_minute == "20200101014700"`. [Python example below](#retrieve-a-timestamp-with-python).
- **Dates:** `date` is GDELT's observation time; `metadata.source_minute` identifies the input file. Neither is a verified publication date.
- **Download:** [browse Parquet files](https://huggingface.co/datasets/openalphalab/gdelt-news/tree/main/data), or use the [streaming and download examples](https://github.com/openalphalab/GDELT-News/blob/main/docs/dataset-guide.md#stream-without-downloading-the-archive). Public downloads need no token.
- **Quality:** reconstruction is best effort; text may be incomplete, reordered or repeated. Both types retain diagnostics. Publisher content rights still apply.
- **Retention:** keep published history; stop new uploads before the configured **7 TB** guard. No automatic oldest-data deletion.
- **Code and details:** [GitHub repository](https://github.com/openalphalab/GDELT-News) · [Full dataset guide](https://github.com/openalphalab/GDELT-News/blob/main/docs/dataset-guide.md) · [GDELT source specification](https://blog.gdeltproject.org/announcing-the-new-web-news-ngrams-3-0-dataset/).

## Retrieve a timestamp with Python

- Change `stamp` to the UTC source timestamp you need.
- Downloads only matching shards and includes both reconstruction types. Only already-published timestamps are available.

```bash
pip install huggingface_hub pyarrow
```

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
