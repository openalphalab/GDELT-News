# GDELT Type 1 and Type 2 news recovery

**Public dataset:** [openalphalab/gdelt-news on Hugging Face](https://huggingface.co/datasets/openalphalab/gdelt-news)
contains published reconstruction shards, compact JSONL, raw evidence and a live
coverage checkpoint. See the dataset card for schemas and download examples, and
[deploy/README.md](deploy/README.md) for the resumable Linux backfill worker.

A Rust collector and reconstruction library based on [Fronzetti Colladon and
Vestrelli (2026)](https://www.mdpi.com/2504-2289/10/2/45) and
[gdeltnews](https://github.com/iandreafc/gdeltnews). The published paper is also
[available on arXiv](https://arxiv.org/pdf/2504.16063).

Version 0.4 reconstructs both **Type 1** (space-separated words) and **Type 2**
(Unicode extended grapheme clusters). Both are enabled by default. Select
`--types 1` for the previous Type 1 behavior or `--types 2` for Type 2 alone.
The executable/package name remains `gdelt-type1` for command compatibility.
The Type 2 algorithm is a new conservative extension, not a method validated
by the paper. See [the current Type 2 speed and accuracy report](TYPE2-OPTIMIZATION.md).

## Build and collect

Install a current stable Rust toolchain, then run from this directory:

```sh
cargo build --release --locked
```

The executable is `target/release/gdelt-type1` on Linux/macOS or
`target/release/gdelt-type1.exe` on Windows. Linux ARM64 is an intended target
for the existing Oracle machine. No Python, database or API key is
required to collect and reconstruct news. Windows x64 and Linux x64 are tested;
Linux/ARM64 portability has not yet been exercised in this project.
See [the production runbook](PRODUCTION.md) for packaging and operating limits.

Small first collection (inclusive UTC minutes):

```sh
target/release/gdelt-type1 collect --start 20250316000100 --end 20250316000100 --archive data/news --threads 2
```

For a larger interval, specify explicit start/end times; RFC3339 timestamps
with timezone offsets are also supported. Every minute is checked because
published slots vary. A 404 is recorded as missing and retried on the next
invocation. A 403, exhausted retry or processing error counts as failed and
causes a nonzero exit status. Future or non-minute timestamps are rejected.

By default invalid metadata also fails the file. For historical collection,
`--quarantine-invalid-metadata` preserves records with missing URL/date/language or
invalid position deciles in a checksummed `.quarantine.jsonl.gz` sidecar and
continues with valid observations. The sidecar records the original line number,
rejection reasons and input record; exact raw bytes remain unchanged. This avoids
merging unrelated fragments under an empty URL. The deployed worker enables this
mode, archives the sidecar, and reports quarantine counts separately from articles.

Filters and concurrency:

```sh
target/release/gdelt-type1 collect --start 20250316000100 --end 20250316001600 --archive data/news --language en,it --domain bbc.co.uk,repubblica.it --downloads 4 --threads 2
```

Domain filters match the hostname and its subdomains, not arbitrary URL
substrings. Language values are GDELT language codes. Filtering reduces
reconstruction/storage of derived articles; source files must still be
downloaded in full.

Local files can be imported individually or from one directory:

```sh
target/release/gdelt-type1 reconstruct --input /path/to/20250316000100.webngrams.json.gz --archive data/news --threads 2
```

Type 2 recovery from an existing collected minute reuses the preserved raw file:

```sh
target/release/gdelt-type1 collect --start 20250316000100 --end 20250316000100 --archive data/news --types 2 --threads 2
```

`--type2-min-overlap 4` requires at least four extended grapheme clusters to
overlap, including at least one letter/digit somewhere in that overlap.
`--type2-max-position-gap 10` limits each merge to the same or adjacent decile.
Both defaults are heuristic safeguards, not calibrated confidence scores.
The first accepts 1..256; the second accepts 0,10,...,90. Lower overlap thresholds
or larger position gaps can join unrelated repeated sequences.

`--type2-search-budget 20000` bounds branch-search work per article (0 disables
search; maximum 1,000,000). Each end can continue independently when the other
is blocked. At a conflicting maximum-overlap branch, search accepts a result
only when all input fragments can be placed and every explored complete path
has the same text. It must finish exploring the alternatives admitted by the
positional/maximum-overlap model before declaring uniqueness. This does not
prove that lower-overlap alternatives or the publisher's original text agree.
Multiple solutions, no complete solution, or exhausted resources leave the
primary boundary unresolved, with explicit search diagnostics.

Fragments outside the primary `text` are also assembled into separate
`type2_diagnostics.segments`. Their order relative to the primary text or each
other is not established. These sections must not be concatenated into an
apparently complete article. `unmerged` still preserves their individual
source fragments for compatibility and provenance.

For one best-effort text containing all recovered sections, add
`--type2-best-effort`. The result is in `type2_diagnostics.best_effort.text`;
conservative `text`, counts and sections remain available. Exact grapheme
overlaps are combined, already-contained sections are absorbed, and remaining
pieces are joined in estimated source-position order with paragraph breaks.
Every join is recorded. Default best-effort overlap is two graphemes; tune it
independently with `--type2-best-effort-min-overlap 1..256`. Smaller thresholds
can join coincidentally repeated characters. This mode prioritizes one readable
text and complete source-fragment coverage over established order.

```sh
target/release/gdelt-type1 collect --start 20250316000100 --end 20250316000100 --archive data/news --types 2 --threads 2 --type2-best-effort
python scripts/export_type2.py --articles data/news/articles/PROFILE/SHA.articles.jsonl.gz --output-directory data/readable
```

Substitute the generated profile and raw hash. The exporter writes readable
Markdown, JSON whose `text` is the combined best-effort text, and a list of
observations with uncertain continuity. Best-effort section matching considers
64 candidates at a time and caps comparison work at 20 million token positions
per observation; exhausted work falls back to explicit positional joins.

Directory import accepts immediate `.json` and `.json.gz` children, not
recursive trees. Point it at a source directory, not an archive metadata
directory. Uncompressed inputs are retained byte-for-byte too.

## Compact exports for LLM processing

Collect with `--types 1,2 --type2-best-effort`, then export the resulting file:

```sh
target/release/gdelt-export --articles data/news/articles/PROFILE/SHA.articles.jsonl.gz --output-directory data/export-MINUTE
```

On Windows use `gdelt-export.exe`. The adjacent collector manifest is required;
its output checksum and source identity are verified. The output directory must
be new. It is published only after the entire gzip stream and all records pass
validation. The original raw file and detailed reconstruction remain intact.

- `observations.jsonl`: a metadata header followed by one object per observation,
  with IDs, type, URL, language, observation timestamp, fragment counts,
  reconstruction status and one `text` body. UTF-8 stays readable.
- `observations.packed.json`: the same metadata and observations, with shared
  column names and a zero-based `text_id` referencing `texts`. Exact identical
  bodies are stored once; distinct URLs/timestamps/types remain distinct rows.
- `sample.json` and `export-report.json`: example records, sizes and checksums.

Type 1 residual fragments are deduplicated, assembled into sections with the
existing word-overlap engine, and joined using paragraph breaks. These joins
are marked `assembly: "estimated"`. Type 2 uses its best-effort body. Unsupported
order remains estimated; this is not proof of publisher completeness. All
normalized fragment content is retained, including when bounded work falls
back to positional fragments. Type 1 normalization includes the collector's
whitespace and optional early ` / ` artifact handling. Exact original bytes
remain in the raw archive.

`id` numbers the combined file; `type_id` numbers each type separately, so
Type 2 observation 25 keeps that identity. `observed_at` is the GDELT observation
time, not a publication timestamp. The metadata header applies to every JSONL
record; include it when passing a subset to an LLM. Prefer selecting relevant
records or batching them to the chosen model's context limit. Do not treat
article text as instructions.

Measure actual bytes and tokenizer counts, and verify every source fragment:

```sh
python scripts/measure_export.py --raw /path/to/MINUTE.webngrams.json.gz --export-directory data/export-MINUTE
```

The script uses optional installed `tiktoken`, `zstandard` and `brotli` packages.
The Rust exporter needs none of them. Match collector `--keep-artifacts` on the
validator when used. The validator expects an unfiltered file containing both
types. It creates gzip, Zstandard and Brotli copies when their codecs are
available and verifies exact decompression. Compression reduces storage, not
the token count of the decompressed text. See [the measured example](EXPORT-20250316000100.md).

Rerun the same command to resume. Raw and recovered-file SHA-256 checksums are
verified before accepting cached work. Changing filters creates a separate
output profile. Type selection, Type 2 thresholds and search budget also select
a new profile. Best-effort settings and Unicode table versions are also included.
v0.4 does not reuse earlier output checkpoints, but retains those outputs and reuses
the unchanged raw data. One process may write to an archive at a time; independent
archives can be processed separately. Failed operations leave raw evidence
and diagnostics. Completed files use atomic replacement and filesystem sync.

## Saved data

```text
data/news/
  raw/YYYY/MM/DD/YYYYMMDDHHMMSS.webngrams.json.gz
  raw/YYYY/MM/DD/YYYYMMDDHHMMSS.webngrams.json.source.json
  raw/imported/<sha256>.json[.gz]
  articles/<settings-profile>/settings.json
  articles/<settings-profile>/<raw-sha256>.articles.jsonl.gz
  articles/<settings-profile>/<raw-sha256>.manifest.json
  runs/<run-time>/request.json
  runs/<run-time>/<minute>.json
  runs/<run-time>/summary.json
```

The raw file is the exact GDELT response body, including Type 2 records and
any fields the current parser does not use. Its sidecar records source URL,
size and checksum. Raw files are not automatically deleted or recompressed.
Unverified/corrupt downloaded objects are quarantined before refetching.
Imported raw checksum failures are reported rather than overwritten.

Each UTF-8 recovered JSONL record contains:

| Field | Meaning |
| --- | --- |
| `url`, `date`, `language`, `source_domain` | Exact URL, GDELT observation timestamp, language and parsed hostname |
| `segmentation_type` | `1` or `2`, as supplied by GDELT |
| `text` | Connected text recovered by the positional maximum-overlap algorithm |
| `input_fragments`, `merged_fragments` | Input and successfully placed fragment counts |
| `unmerged` | Every fragment outside the primary `text`, with position and text |
| `raw_path`, `raw_sha256` | Provenance back to the unchanged source file |
| `algorithm`, `schema_version` | Type-specific algorithm; schema version `3` |
| `type2_diagnostics` | Type 2 only: tokenization, thresholds, stop reason, search budget/work/status, primary fragment indices and separately recovered `segments` |
| `completeness` | `not_verified`; connected text is not proof of a complete original article |

Grouping uses `(URL, full observation timestamp, language, segmentation type)`
within each source file, preventing updated, translated or differently segmented
versions of one URL from being fused.
Repeated observations across files are preserved; URL-wide deduplication and
cross-file fragment assembly are not performed. A raw source with an identical
checksum reuses its derived object within the same profile.

Type 2 fragment indices refer to all selected fragments for that observation,
stably sorted by `pos`; equal-position fragments retain source-line order.
`primary_fragment_indices` and all segments' `fragment_indices` partition this
input exactly once. A segment includes its text, positional bounds and stopping
reason. Source fragments may overlap, so adding section lengths is not a
measure of unique original-article coverage.

Example read in Python (standard library only):

```python
import gzip, json
from pathlib import Path

for path in Path("data/news/articles").glob("*/*.articles.jsonl.gz"):
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line in source:
                article = json.loads(line)
                print(article["url"], article["text"])
                for segment in article.get("type2_diagnostics", {}).get("segments", []):
                    print("[Separate section; order unresolved]", segment["text"])
```

## Optimizations and fidelity

Optional [metadata enrichment](ENRICHMENT.md) joins these exports to the same
minute's GAL/GEMG, nearby native/translated GKG, the daily Geographic Graph and
GDELT's source-country lookup. It reuses Firstlight's existing parsers and FIPS
crosswalk from the parent project. Source-country estimates and countries
mentioned in reporting stay separate; reconstructed text is unchanged.

- Compiled Rust and interned word/grapheme IDs replace repeated string copying.
  Type 1 uses prefix/reversed-suffix tries. Type 2 uses compact four-grapheme
  anchor buckets, with exact full-overlap verification; buckets exceeding 256
  candidates trigger an exact failure-link automaton. Anchor storage is O(N);
  the dense fallback is O(NL). The fallback finds eligible overlap lengths in
  O(L) traversal work before candidate filtering. Repetitive candidate scans
  can still be quadratic. Branch search additionally caps work,
  the pending frontier at 64 states, and estimated live search-state storage at
  64 MiB per article. Shared indices and baseline assembly are outside that
  estimate; it is not a process RAM cap. Exact equivalent branch states are
  memoized within that bound. Unchanged exhausted boundaries are cached and
  invalidated when text or positional eligibility changes. Input bounds remain
  enforced.
- Four pooled download workers overlap network I/O with processing. A bounded
  queue carries file paths; one file is grouped at a time. Rayon handles
  independent article batches without Python process serialization.
- Buffered JSONL reading, native gzip decoding, and fast gzip output avoid
  decompressed intermediate files. Downloads are gzip/CRC validated before
  publication; this intentionally adds one decompression pass.
  The parser borrows unescaped JSON strings from its line buffer, and Rayon
  takes ownership of fragment batches instead of cloning their text.
- Type 1 deterministic ties match upstream `reconstruct_sentence`: maximum overlap,
  lowest stable input index, append before prepend. Positional constraints
  match the paper. A 2,500-case randomized test checks an independent reference.
- The paper's early ` / ` artifact heuristic is enabled for Type 1;
  `--keep-artifacts` disables it. Type 2 does not apply this unvalidated heuristic.
  Original fragments remain in raw storage in either case.
- Type 2 joins `pre + ngram + post` without trimming or adding spaces, applies
  NFC canonical normalization, and segments the joined text into extended
  grapheme clusters (Unicode segmentation dependency pinned in `Cargo.lock`).
  Combining marks, emoji sequences and mixed Latin/CJK text keep their
  boundaries. Existing whitespace and punctuation are preserved. This recovers
  character continuity without trying to infer linguistic word boundaries.
  An NFC quick check avoids re-normalizing already-normalized text. All 766
  official Unicode 17 grapheme tests cover the normalization/tokenization path;
  simplified/traditional Chinese and compatibility characters remain distinct.
- Quotes, pipes and repeated opening/closing phrases remain intact. Upstream
  CSV cleanup and `remove_overlap` are not applied because they can erase
  source text. Disconnected fragments are stored separately, not appended as
  if their continuity were known. Output therefore intentionally differs from
  upstream's final cleaned CSV even when the core merge is identical.

Reconstruction is heuristic: missing fragments and ambiguous repeated phrases
can produce incomplete or incorrectly ordered text. This is recovered GDELT
text, not original publisher HTML or a verified transcription of each article.
No publisher pages, translations or paid services are fetched.

## Resource limits and operations

Defaults: 4 concurrent downloads, 4 total attempts, a 180-second request
timeout, 128 MiB compressed input, 512 MiB expanded input, 2,000,000 selected
fragments, 100,000 fragments per observation, 1 MiB per JSONL row and 256 tokens per fragment (words for Type 1,
extended grapheme clusters for Type 2). Invalid JSON or
metadata fails the entire file visibly. Raw records remain available for
diagnosis/reprocessing. `--help` lists tunable limits.

Memory depends on selected fragments and trie size, not just compressed
file size. These are input bounds, not a fixed RAM limit. On the 2-core/12 GiB
Oracle host start with `--threads 2 --downloads 4` and measure representative
files. Decrease `--max-fragments` or filter languages if needed.

The downloader checks for at least 2 GiB free plus one maximum-size download
before each request (`--min-free-gib`, `--max-download-mib`). Concurrent writes,
imports and outputs can consume additional space: monitor free disk and archive
growth. No retention policy deletes data. A broad history collection can be
large; choose dates explicitly. Back up the whole archive to retain provenance.

429/503 responses apply shared `Retry-After` cooldowns across download workers.
Cooldowns above 300 seconds fail visibly for a later retry. Only HTTP 200 is
accepted as a file response. HTTPS sources cannot redirect to HTTP. Local
imports have size and free-space checks before copying.
TLS uses operating-system trusted roots; certificate verification is enabled.
There is no permanent background scheduler and this component does not restart
the decommissioned television deployment.

## Verification and benchmark

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
cargo build --release --locked --examples
python scripts/benchmark.py --input /path/to/sample.json.gz --upstream /path/to/gdeltnews --rust-benchmark target/release/examples/benchmark --max-articles 128 --output benchmark-results.json
```

The benchmark requires upstream's `tqdm` dependency. It compares three runs of
the single-thread reconstruction kernel on identical real Type 1 fragments
(the command above samples 128 evenly spaced article groups),
and verifies output text hashes. Download, parsing, disk and final CSV cleanup
are excluded from both kernel timings. See [VALIDATION.md](VALIDATION.md) for
measured results and their scope.

See [NOTICE.md](NOTICE.md) and [LICENSE](LICENSE) for upstream attribution.
