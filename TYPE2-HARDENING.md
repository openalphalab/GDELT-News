# Type 2 reconstruction: v0.3 audit and validation

Historical v0.3 results. The current implementation and best-effort export are
documented in [TYPE2-OPTIMIZATION.md](TYPE2-OPTIMIZATION.md).

Checked on 21 September 2026 against the concerns in
[Fronzetti Colladon and Vestrelli, section 2, page 5](https://arxiv.org/pdf/2504.16063).
This supersedes the implementation assessment in the historical
[v0.2 report](TYPE2-VALIDATION.md).

The identified implementation gaps below are fixed and the validation checks
pass. The remaining research question is whether reconstructed order matches
the publisher's original article: source-fragment accounting cannot establish
that. The implementation retains uncertainty where the evidence is insufficient.

## Changes and issue assessment

| Concern | v0.3 behavior and evidence | Status |
| --- | --- | --- |
| No spaces or explicit word boundaries | Join KWIC fields exactly, normalize NFC, and match extended grapheme clusters. Exact known-text tests cover Chinese, Japanese, Thai, Khmer, combining marks and emoji. | Implemented; no linguistic word segmentation required for character assembly. |
| Repeated sequences and competing continuations | Bounded search checks competing maximum-overlap branches; accepts one complete text only after exhausting the permitted alternatives. Tests distinguish unique, multiple, incomplete and budget-exhausted results. One real observation newly resolves. | Improved; uniqueness is relative to the positional/maximum-overlap model. |
| Ambiguity at one end prematurely blocked the other | Eligible extensions at each end are checked independently. A blocked right end does not discard a supported left extension. | Fixed; regression test passes. |
| The same fragment could extend either end | Both orientations are considered when both would add text; conflicting completions are not selected arbitrarily. | Fixed; adversarial regression test passes. |
| A missing bridge left later material unreadable | Residual fragments are assembled into separate supported sections, with fragment IDs and positional bounds. No connection between sections is invented. | Fixed; 179 residual sections recovered in the real sample. |
| Coarse decile position cannot establish exact order | Same/adjacent-decile constraints are retained; search and stopping diagnostics expose unresolved paths. | Mitigated; absent positional information is not reconstructed as fact. |
| Branch search could explode in time or memory | Per-article work budget, 64 pending states and 64 MiB estimated search-state storage; any exhausted bound prevents a uniqueness claim. | Bounded search tested; total process memory and baseline candidate scanning remain input-dependent. |
| Fidelity and regressions need evidence | 27 Rust tests, 300 randomized multilingual cases, independent raw/output accounting, 2,169 real Type 1 comparisons and 190 standalone/combined Type 2 comparisons. | All checks passed; no publisher-ground-truth accuracy study performed. |

Search starts at a primary ambiguity and enumerates alternatives admitted by the
maximum-overlap/positional model, retaining its deterministic compatible merges.
It does not enumerate arbitrary lower-overlap paths, alternate initial seeds,
missing text, or unconstrained within-decile permutations. Multiple paths that
produce the same complete text are equivalent. Any unfinished search is reported
as `budget_exhausted`, including work, frontier or estimated-storage exhaustion.
Residual sections use conservative local merging without additional branch search.

## Real sample

Reused the exact preserved
[GDELT minute file](https://data.gdeltproject.org/gdeltv3/webngrams/20250316000100.webngrams.json.gz).
No new download or publisher fetch was needed.

Raw SHA-256: `4b73eed6c10471fa8c05e2eca4d403be66ba5d94c93f195fe4746ceabc4d49bb`.

| Measurement | v0.3 result |
| --- | ---: |
| Type 2 observations | 190 |
| Input fragments | 223,797 |
| Fragments in primary text | 189,319 |
| Fragments in separately recovered sections, also retained in `unmerged` | 34,478 |
| Residual sections | 179 |
| Primary / residual code points | 216,417 / 44,822 |
| Observations with all supplied fragments in primary text | 168 |
| Primary ambiguous stops | 11 |
| Primary stops without a confident overlap | 11 |
| Search: unique complete / no complete solution / exhausted | 1 / 5 / 6 |
| Observations not requiring branch search | 178 |
| Search work steps across the sample | 135,302 |
| Processing from verified cached raw, two CPU threads | 4.95 s |
| Checksum-verified cached restart | 0.44 s |
| Combined Type 1 and Type 2 processing | 8.87 s |

The language split is 147 `zh`, 17 `zh-TW` and 26 `ja` observations. Thai and
Khmer currently have synthetic tests only. Timings are single Windows-host
measurements, not a throughput guarantee or a Type 2 speedup comparison.

Compared with v0.2, one additional observation merges all supplied fragments
(168 versus 167). Three other primary assemblies gain supported fragments.
One observation stops earlier under the new two-ended conflict checks; its
material is preserved in separate sections. Consequently, primary placed
fragment count decreases by 295 overall. This is not hidden by reporting all
sections as a single complete article. Section lengths can include overlapping
text and must not be added as unique publisher-text coverage.

The independent [evidence report](TYPE2-HARDENING.json) confirms:

- The raw hash is unchanged and all 223,797 fragments are accounted for.
- Primary and residual fragment IDs partition each observation exactly once.
- Every assigned fragment occurs verbatim after NFC normalization in its section.
- Every leftover corresponds to the raw source, including duplicate multiplicity.
- Segment position bounds, counts and search budgets agree with their evidence.
- `completeness` remains `not_verified` for every output.

The [regression report](TYPE2-REGRESSION.json) confirms all 2,169 Type 1 texts,
fragment counts and leftovers match the original v0.1 outputs. All 190 Type 2
records match exactly between standalone and combined processing.

## Tests and reproduction

- `cargo test --locked`: 27 passed, including 16 Type 2 tests and the existing
  2,500-case Type 1 reference comparison.
- The Type 2 randomized test checks 300 multilingual sources, half with missing
  windows. All cases preserve fragment provenance; all 150 complete cases
  reproduce the known original text exactly.
- Search-budget tests cover every smaller budget than a known successful search,
  including stopping after finding one solution but before checking all rivals.
- CLI tests cover raw preservation, corrupt output/checkpoint repair, retries,
  type separation, token limits, invalid options and settings-specific resumes,
  including the new search budget.
- `cargo clippy --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `cargo build --release --locked`: passed.
- Independent validator negative checks: accepted valid evidence and rejected
  duplicated IDs, missing IDs, fabricated section text and incorrect positions.

Run the collector from this directory:

```sh
target/release/gdelt-type1 collect --start 20250316000100 --end 20250316000100 --archive data/live-sample --types 2 --threads 2
```

Use `.exe` on Windows. Default search budget is 20,000; tune it with
`--type2-search-budget` (0 disables, maximum 1,000,000). Each setting change
selects a separate archive profile. Schema version is 3, pipeline identity is
`webngrams-v3`, and Type 2 algorithm identity is `type2-grapheme-assembly-v2`.

Reproduce the independent checks using the paths below:

```sh
python scripts/validate_type2.py --raw data/live-sample/raw/2025/03/16/20250316000100.webngrams.json.gz --articles data/live-sample/articles/5e180e8a72f839f3/4b73eed6c10471fa8c05e2eca4d403be66ba5d94c93f195fe4746ceabc4d49bb.articles.jsonl.gz --output TYPE2-HARDENING.json
python scripts/validate_regression.py --type1-baseline data/live-sample/articles/d1ac791615be15a9/4b73eed6c10471fa8c05e2eca4d403be66ba5d94c93f195fe4746ceabc4d49bb.articles.jsonl.gz --type2-baseline data/live-sample/articles/5e180e8a72f839f3/4b73eed6c10471fa8c05e2eca4d403be66ba5d94c93f195fe4746ceabc4d49bb.articles.jsonl.gz --combined data/live-sample/articles/e5c58ca0d33f4ca5/4b73eed6c10471fa8c05e2eca4d403be66ba5d94c93f195fe4746ceabc4d49bb.articles.jsonl.gz --output TYPE2-REGRESSION.json
```

Raw data and derived outputs remain local under the ignored `data/` directory.
No deployment was performed; Linux/ARM64 execution is not yet verified.
