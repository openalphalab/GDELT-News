# Type 2 optimization and validation, version 0.4

The reconstruction engine is Rust. Adaptive four-grapheme anchor buckets
replace large default tries; dense buckets use an exact failure-link index.
Full overlaps are verified, so hash collisions cannot establish a match.
Unchanged exhausted boundaries are cached, equivalent branch states are
memoized within the search budget, and NFC quick checks avoid redundant work.
The parser borrows strings and transfers fragment batches without cloning.

## Measured improvement

Five alternating runs of each executable reconstructed the same 190 Type 2
observations from `20250316000100.webngrams.json.gz` on this Windows x64 host.

| Metric | Version 0.3 | Version 0.4 |
| --- | ---: | ---: |
| Median single-thread kernel time | 4.214 s | 0.572 s |
| Median benchmark-process peak working set | 136.34 MB | 50.20 MB |

The measured kernel speedup is **7.36×**. Timings include input cloning and
reconstruction but exclude gzip, parsing, serialization, disk and downloads.
Memory covers the entire benchmark process. These are sample-specific results,
not a claim of universally fastest execution. Exact runs and input hash are in
[TYPE2-PERFORMANCE.json](TYPE2-PERFORMANCE.json).

## Evidence and ordering

All 190 conservative main texts, fragment counts and unmerged fragments match
version 0.3 on the real sample. All 2,169 Type 1 observations retain their prior
main texts, counts and residual evidence. Nested-extension handling now places
short compatible extensions before longer ones, avoiding stranded evidence in
adversarial tests. Diagnostic ordering can differ. See
[TYPE2-QUALITY-REGRESSION.json](TYPE2-QUALITY-REGRESSION.json).

The sample has 223,797 Type 2 fragments. Conservative reconstruction assigns
189,319 to primary texts and 34,478 to 179 residual sections. Every fragment is
assigned exactly once. Of 190 observations, 168 are connected under the model;
22 retain uncertain continuity. Optional best-effort assembly preserves every
fragment in one body per observation: 124 overlap joins, 13 contained sections
and 42 position-only joins. Full independent checks are in
[TYPE2-OPTIMIZED-VALIDATION.json](TYPE2-OPTIMIZED-VALIDATION.json).

These tests establish source-fragment preservation, not publisher-ground-truth
accuracy or full-article completeness. Estimated ordering remains explicit.
The engine preserves Simplified/Traditional distinctions and compatibility
characters; it does not invent dictionary-based word boundaries for Type 2.

## Robustness

The suite covers randomized overlap-oracle comparisons, all 766 official
Unicode 17 grapheme cases, nested/ambiguous/disconnected sections, budget
exhaustion, corrupt/multipart gzip, limits, download retry, archive locking,
forced-process termination/resume, deterministic workers and export integrity.
The production runbook documents runtime limits and platform scope.

Unicode references: [UAX #29 revision 47](https://www.unicode.org/reports/tr29/tr29-47.html)
and the [official conformance fixture](https://www.unicode.org/Public/17.0.0/ucd/auxiliary/GraphemeBreakTest.txt).
