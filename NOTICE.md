# Attribution

This component implements and optimizes the Type 1 reconstruction method from:

Andrea Fronzetti Colladon and Roberto Vestrelli (2026), *Free Access to World
News: Reconstructing Full-Text Articles from GDELT*, Big Data and Cognitive
Computing 10(2), 45. https://doi.org/10.3390/bdcc10020045

The algorithm was developed with reference to their GPL-3.0 Python library:
https://github.com/iandreafc/gdeltnews

Reference revision: `d6babe296e6b40b01465c068f72e30343545009e`.

Versions 0.2 through 0.4 add an original Type 2 extension using NFC normalization,
extended grapheme clusters, bounded branch search, conservative ambiguity checks
and separately recovered residual sections. The paper and
upstream library did not implement or validate this Type 2 method; their
reported Type 1 accuracy figures do not apply to it.

The Unicode 17 conformance fixture in `tests/fixtures/` is from
https://www.unicode.org/Public/17.0.0/ucd/auxiliary/GraphemeBreakTest.txt.
It retains its Unicode copyright header and is distributed with
`tests/fixtures/LICENSE-UNICODE.txt` under Unicode License V3.

The Rust component, tests and benchmark harness are provided under GPL-3.0-only;
the license text is included in `LICENSE`. This notice applies to this component
and does not relicense unrelated files in the parent project. Third-party Rust
dependencies retain their respective licenses, recorded in their packages.

Data format and public minute-file location:
https://blog.gdeltproject.org/announcing-the-new-web-news-ngrams-3-0-dataset/

The source-code license does not license third-party news text. Raw dataset
preservation and reconstruction do not change rights attached to source content.

## Metadata parsers

The bundled metadata parsers were adapted from the same author's Firstlight project so this repository runs independently. The FIPS-to-ISO country crosswalk is derived from GeoNames countryInfo.txt (https://download.geonames.org/export/dump/countryInfo.txt), licensed CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/).
