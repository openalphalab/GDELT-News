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
- **Retrieve a timestamp:** for `20200101014700` (**2020-01-01 01:47 UTC**), select Parquet filenames whose `START-END` range covers it, then filter `metadata.source_minute == "20200101014700"`. [Copy the Python example](https://github.com/openalphalab/GDELT-News/blob/main/docs/dataset-guide.md#retrieve-a-source-timestamp).
- **Dates:** `date` is GDELT's observation time; `metadata.source_minute` identifies the input file. Neither is a verified publication date.
- **Download:** [browse Parquet files](https://huggingface.co/datasets/openalphalab/gdelt-news/tree/main/data), or use the [streaming and download examples](https://github.com/openalphalab/GDELT-News/blob/main/docs/dataset-guide.md#stream-without-downloading-the-archive). Public downloads need no token.
- **Quality:** reconstruction is best effort; text may be incomplete, reordered or repeated. Both types retain diagnostics. Publisher content rights still apply.
- **Retention:** keep published history; stop new uploads before the configured **7 TB** guard. No automatic oldest-data deletion.
- **Code and details:** [GitHub repository](https://github.com/openalphalab/GDELT-News) · [Full dataset guide](https://github.com/openalphalab/GDELT-News/blob/main/docs/dataset-guide.md) · [GDELT source specification](https://blog.gdeltproject.org/announcing-the-new-web-news-ngrams-3-0-dataset/).
