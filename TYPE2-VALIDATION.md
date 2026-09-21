# Type 2 implementation and paper-issue audit

Historical v0.2 results. See [TYPE2-HARDENING.md](TYPE2-HARDENING.md) for the
current v0.3 implementation, additional tests and independent validation.

Checked on 21 September 2026, version 0.2.0. The report audited is
[Fronzetti Colladon and Vestrelli, section 2, page 5](https://arxiv.org/pdf/2504.16063),
which identifies Type 2 segmentation, overlapping character sequences,
positional ambiguity and scalability/evaluation challenges.

**Conclusion: Type 2 support is implemented; the research problems of ambiguous
ordering and verified full-article accuracy are not completely solved.**
The new safeguards detect some uncertainty and retain all evidence. They do not
establish the paper's Type 1 accuracy figures for Type 2.

## Issue-by-issue assessment

| Paper concern | Implementation and evidence | Assessment |
| --- | --- | --- |
| Character tokens do not supply explicit word boundaries | Join KWIC fields without separators; NFC normalize; segment extended grapheme clusters. Tests cover Chinese, Japanese, Thai, Khmer, combining marks, emoji and mixed Latin text. | Character handling implemented. Linguistic word segmentation is not inferred or claimed. |
| Word-overlap matching cannot simply be reused on unsegmented text | Separate Type 2 engine with grapheme IDs, indexed prefix/suffix matching and exact grapheme concatenation. Existing spaces/punctuation are retained. | Adaptation implemented and tested, including exact reconstruction of known synthetic texts. |
| Short repeated sequences create competing paths | Require four graphemes by default and some letter/digit content. Compare all eligible candidates at the maximum overlap; stop on conflicting extensions and preserve them. Equivalent duplicate/nested extensions are accepted. | Partly mitigated. Equal-score conflicting branches are detected; a wrong uniquely highest-score repetition may still pass. |
| Decile position alone may not disambiguate reconstruction | Preserve monotonic positional constraints and limit each Type 2 merge to a 10-point decile gap by default. | Partly mitigated. Within-decile order remains unknown; conservative thresholds can also reject a valid continuation. |
| Extra computational cost requires evaluation | Rust trie indices, buffered input, Rayon batches and input bounds. Real sample processed with two CPU threads in 4.02 seconds from verified cached raw data. | Operationally tested on one Windows sample. Repetitive candidate scans can be quadratic; no general scalability guarantee or Type 2 speedup factor is established. |
| Reconstruction fidelity must be evaluated | Known synthetic ground truth; source-fragment accounting over the complete real sample; Type 1 regression comparison. | Structural/source validation passed. No independent original-publisher corpus or multilingual accuracy study has been performed. |

Missing fragments, extraction artifacts, repeated passages and already incorrect
source text cannot be resolved merely by adding a grapheme tokenizer. The
implementation never synthesizes missing text. `completeness` remains
`not_verified`, even when all supplied fragments have been merged.

## Real source and results

Used the original preserved GDELT file:
https://data.gdeltproject.org/gdeltv3/webngrams/20250316000100.webngrams.json.gz

Raw SHA-256:
`4b73eed6c10471fa8c05e2eca4d403be66ba5d94c93f195fe4746ceabc4d49bb`

No new source download was needed. Type 2 recovery reused the raw object from
the Type 1 collection, preserving its original bytes and checksum.

| Measurement | Result |
| --- | ---: |
| Type 2 fragments | 223,797 |
| Type 2 observations | 190 |
| Chinese (`zh`) observations | 147 |
| Traditional Chinese (`zh-TW`) observations | 17 |
| Japanese (`ja`) observations | 26 |
| Placed fragments | 189,614 |
| Unplaced fragments retained separately | 34,183 |
| Recovered Unicode code points | 216,859 |
| Observations with all supplied fragments merged | 167 |
| Observations stopped at ambiguous continuation | 11 |
| Observations stopped without a confident next overlap | 12 |
| First Type 2 processing run, cached raw / two CPU threads | 4.02 s |
| Checksum-verified cached restart | 0.39 s |

The sample contains no real Thai or Khmer records; those scripts currently have
synthetic Unicode/assembly tests only. All times are single development-host
observations, not sustained throughput guarantees.

[TYPE2-VALIDATION.json](TYPE2-VALIDATION.json) contains the independent Python
validation results. `scripts/validate_type2.py` reconstructs the multiset of
source fragments from raw JSON, verifies every leftover comes from that source,
verifies every placed fragment appears in the recovered text, checks all counts,
and checks raw/output hashes. All checks passed. These invariants can detect
lost or fabricated fragments; they cannot certify the order against original
publisher text.

Local output profile: `data/live-sample/articles/9bbcd4f9232041b5/`.
Combined Type 1 + Type 2 profile: `data/live-sample/articles/2df1891123d6df2a/`.
The combined run produced 2,359 observations in 7.70 seconds. An independent
comparison confirmed that all 2,169 previous Type 1 texts, fragment counts and
leftovers are unchanged.

## Tests and build

- `cargo test --locked`: **20 tests passed**, including the existing 2,500-case
  randomized Type 1 reference comparison.
- New tests: exact known-text reconstruction across scripts; NFC equivalence;
  combining sequences across KWIC fields; grapheme-safe matching; preserved
  whitespace/slashes; append/prepend ambiguity; compatible nested extensions;
  minimum-overlap and punctuation-only guards; future-decile eligibility;
  large position jumps; empty/single/duplicate inputs; separate types sharing
  a URL/time/language; raw preservation; settings-specific resume; Type 2 token
  limits and invalid CLI options.
- `cargo clippy --locked --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `cargo build --release --locked`: passed.
- Independent Type 2 raw/output accounting: passed.
- Full real-sample Type 1 regression comparison: passed.

No Oracle/Cloudflare deployment was performed. Linux/ARM64 has not been tested.

## Run Type 2

From this component's directory, after `cargo build --release --locked`:

```sh
target/release/gdelt-type1 collect --start 20250316000100 --end 20250316000100 --archive data/live-sample --types 2 --threads 2
```

Use `gdelt-type1.exe` on Windows. Both types are enabled by default; use
`--types 1` for Type 1 only. The new pipeline/profile identity prevents reusing
an older Type 1-only output as if it included Type 2. Old profiles and raw files
are preserved. `--type2-min-overlap` and `--type2-max-position-gap` are recorded
in the output profile and diagnostics.
