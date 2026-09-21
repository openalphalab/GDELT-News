# Validation — 21 September 2026

This records the original v0.1 Type 1 implementation. For v0.2 Type 2 support,
current tests and the paper-issue audit, see [TYPE2-VALIDATION.md](TYPE2-VALIDATION.md).

Built and tested on Windows x86-64 with Rust 1.97.1 and Python 3.10.0.
The executable is built locally at `target/release/gdelt-type1.exe`.

## Real GDELT collection

Source:
https://data.gdeltproject.org/gdeltv3/webngrams/20250316000100.webngrams.json.gz

| Measurement | Result |
| --- | ---: |
| Preserved compressed source | 30,555,745 bytes |
| Expanded source | 389,272,770 bytes |
| Source records | 1,241,335 |
| Selected Type 1 fragments | 1,017,538 |
| Type 2 records skipped, retained in raw | 223,797 |
| Distinct URL/time/language observations recovered | 2,169 |
| Language codes | 44 |
| Observations with unmerged fragments | 489 |
| Unmerged fragments explicitly retained | 57,750 |
| Derived compressed JSONL size | 4,836,375 bytes |

Raw SHA-256:
`4b73eed6c10471fa8c05e2eca4d403be66ba5d94c93f195fe4746ceabc4d49bb`

The first successful HTTPS collection, including download, gzip integrity
validation, raw preservation and two-thread recovery, took **12.16 seconds**.
It used raised trial input limits (4,000,000 fragments / 2 GiB expanded).
A subsequent run from the cached raw source using final defaults (2,000,000
fragments / 512 MiB expanded) took **8.28 seconds**, including **6.22 seconds**
inside processing. A verified cached restart took **1.32 seconds** and made no
new download. These are observed single-file times, not sustained throughput
or a guarantee for other dates, hardware or source availability.

The initial 1,000,000-fragment bound correctly rejected this 1,017,538-fragment
file. The shipped default was raised to 2,000,000 after this observation.
TLS was verified against the OS certificate store. No verification bypass was
introduced.

The final output was independently read with Python's gzip/JSON libraries:
all 2,169 identities were distinct, all records were Type 1, and each record
satisfied `input_fragments == merged_fragments + len(unmerged)`.
No completeness claim is made for the recovered articles.

Local validated archive: `data/live-sample/`.
Final profile: `d1ac791615be15a9`.
Its manifest links each output back to the original compressed source.

## Comparison with upstream Python

Upstream revision: `d6babe296e6b40b01465c068f72e30343545009e`.
The benchmark selected **128 evenly spaced URL-sorted article groups** from
the same real file: **69,839 fragments**. Both kernels used one thread and
identical fragments, positional ordering and merge rules.

| Kernel | Run 1 | Run 2 | Run 3 | Median |
| --- | ---: | ---: | ---: | ---: |
| Upstream `reconstruct_sentence` | 13.463 s | 12.499 s | 12.622 s | 12.622 s |
| Rust indexed reconstruction | 0.371 s | 0.330 s | 0.420 s | 0.371 s |

**Measured median kernel speedup: 34.04×.** Core output text hashes matched
exactly for this sample. This comparison excludes download, parsing, output
writing and upstream final CSV cleanup; it does not establish a 34× speedup
for the complete pipeline. Rust additionally retains disconnected fragments.
Timings were collected on a development host without CPU isolation.

See [BENCHMARK.json](BENCHMARK.json) for the machine-readable measurements.
The initial full-file comparison was stopped because it exceeded the useful
interactive validation time; the reported benchmark is explicitly the bounded
128-article sample, not a completed full-file comparison.

Reproduce after building `cargo build --release --locked --examples`:

```sh
python scripts/benchmark.py --input data/live-sample/raw/2025/03/16/20250316000100.webngrams.json.gz --upstream /path/to/gdeltnews --rust-benchmark target/release/examples/benchmark --max-articles 128 --output benchmark-results.json
```

Append `.exe` to the benchmark executable on Windows. Omit `--max-articles`
to benchmark all articles, which can take much longer for upstream Python.

## Automated checks

- `cargo test --locked`: **9 tests passed**. Includes 2,500 randomized
  comparisons against an independent greedy reference implementation.
- Coverage: punctuation/Unicode; disconnected fragments; Type 2 exclusion;
  hostname filtering; distinct observed revisions; byte-identical raw copies;
  changed-filter checkpoints; damaged output/manifest recovery; malformed JSON;
  truncated gzip; HTTP 429 retry; missing 404 versus denied 403; invalid gzip
  responses; verified restart; corrupt raw-cache refetch; input limits and
  minute boundary rejection.
- `cargo clippy --locked --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `cargo build --release --locked`: passed.
- Python benchmark script compilation: passed.

The local HTTP tests use `tiny_http` to avoid Windows connection-reset races
in a hand-written socket fixture. It is a test-only dependency.

The Linux/ARM64 build and long-running collection have not been exercised.
Nothing was deployed to Oracle or Cloudflare; the previous television service
remains decommissioned.
