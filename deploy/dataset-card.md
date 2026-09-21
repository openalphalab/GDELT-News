---
pretty_name: GDELT News Reconstructions
task_categories:
  - text-retrieval
  - text-generation
tags:
  - gdelt
  - news
  - multilingual
  - reconstruction
configs:
  - config_name: default
    data_files:
      - split: train
        path: data/**/*.parquet
---

# GDELT News Reconstructions

Type 1 (space-delimited) and Type 2 (character/grapheme) observations reconstructed
from [GDELT Web News NGrams 3.0](https://blog.gdeltproject.org/announcing-the-new-web-news-ngrams-3-0-dataset/).
The backfill starts at 2020-01-01 00:01 UTC. See `progress.json` for the actual
published coverage; later dates are not present until the backfill reaches them.
After catching up, the worker follows new files with a 48-hour lag for retrospective
daily metadata enrichment. Source 404s are explicitly listed in batch manifests.

## Files

- `data/YYYY/MM/*.parquet`: searchable text, language, URL, observation timestamp,
  reconstruction quality, source hash, estimated source country and `metadata_json`.
- `llm/YYYY/MM/*.jsonl.gz`: compact JSONL with a metadata header before each minute's
  observations. Headers include source provenance; text is unchanged from the exporter.
- `evidence/YYYY/MM/*.tar`: exact original compressed ngram files, conservative
  reconstruction output and source metadata files. Already-compressed files are
  stored without redundant outer compression.
- `manifests/YYYY/MM/*.json`: byte counts, SHA256 hashes, missing minutes and code revision.

```python
from datasets import load_dataset
data = load_dataset("openalphalab/gdelt-news", split="train", streaming=True)
row = next(iter(data))
print(row["url"], row["text"])
```

## Interpretation and limitations

These are reconstructed observations, not certified complete publisher articles.
`assembly=estimated` and `position_joins` identify uncertain joins; a connected
result does not prove completeness or original ordering. The conservative evidence
is retained alongside best-effort text. Repeated URLs at different times remain
separate observations. `observed_at` is GDELT observation time, not publication time.

Source country is GDELT's estimated outlet origin/affinity, not verified headquarters,
author nationality or event location. Unknowns remain null. Metadata records its
source and method; the historical domain lookup dates from May 2018. GKG/GGG joins
are retrospective and can use later observations, making them unsuitable as
unqualified real-time features. Unavailable source datasets are disclosed.

The software is [GPL-3.0-only](https://github.com/openalphalab/GDELT-News).
That software license does not grant rights to third-party news text; underlying
publisher rights and GDELT source terms continue to apply. GeoNames-derived country
codes are attributed in the project's NOTICE.md. No model-based text generation
or publisher-page scraping is used in reconstruction.
