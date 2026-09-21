# Source country and metadata enrichment

The original NGrams export contains URL, language and observation time, but no
verified publisher-country field. Enrichment now combines the datasets already
documented in the parent project's [GDELT audit](../../docs/GDELT-DATASET-AUDIT.md).
It reuses `backend/app/integrations/gdelt.py` for GAL/GEMG/GKG parsing and its
GeoNames FIPS-to-ISO crosswalk. The live application and database are unchanged.

## Example results

For `20250316000100.webngrams.json.gz`, both segmentation types are retained:
2,169 Type 1 and 190 Type 2 observations. Every original field, ID and text
body compares exactly with the input export.

| Annotation | Observations |
| --- | ---: |
| Exact URL match in GAL | 2,359 |
| Exact URL match in GEMG | 2,256 |
| Available title | 2,356 |
| Available description | 2,231 |
| Outlet name or domain | 2,359 |
| Available author field | 892 |
| Publisher-reported publication timestamp | 997 |
| GKG themes/entities/mentioned countries/tone | 1,655 |
| GGG country mentions within an hour | 496 |
| Estimated source country | 2,265 |
| Unknown source country | 94 |

Country estimates use GGG's `DomainCountryCode` for 484 exact-URL matches and
307 exact-host matches from the same daily file. Another 1,474 use the May 2018
source-country lookup. These are **estimates of outlet origin or geographic
affinity**, not verified headquarters, a reporter's physical location, author
nationality, event location or independently confirmed country of publication.
The lookup's age is recorded explicitly; ownership and domains can change.

Type 2 observation 25 has `iso2: "CN"`, `fips: "CH"`, `name: "China"` from the
historical `aweb.com.cn` mapping. Its status is `estimated`; it is not an
inference from the Chinese text or a country mentioned within it.

## Run it

From this directory, with the parent Firstlight checkout present:

```sh
python scripts/enrich_metadata.py --observations data/exports/20250316000100/observations.jsonl --minute 20250316000100 --cache data/enrichment/20250316000100 --output-directory data/exports/new-enriched-export
python -m unittest discover -s scripts -p test_enrich_metadata.py -v
```

The script uses Python's standard library and the parent's standard-library
parsers. The Rust reconstruction engine is unchanged. Ten enrichment tests
cover source-versus-mentioned-country separation, missing evidence, conflicts,
FIPS/ISO distinctions, corrupt cache, limits, time boundaries and suffix rules.

Output includes `observations.enriched.jsonl`, a packed JSON equivalent, gzip
copies, a preview and `enrichment-report.json`. The current validated sample is
under `data/exports/20250316000100-enriched-v2/`. Its metadata-only observation 25
example and token measurements are saved alongside the full files.

The output directory must be new. Publication uses a temporary directory and
rename after validation. Original fields and text are preserved. Input export
and cached source SHA-256 values are checked. Fixed official dataset URLs are
fetched with TLS validation, redirects disabled, a 45-second request timeout,
128 MiB download limit, 512 MiB expansion limit and 8 MiB line limit. No archive
paths are extracted to disk. Missing/failed optional sources and malformed rows
are counted and recorded. Rerunning uses verified cached files; absent sources
are tried again. The Public Suffix List is required for safe parent-domain
matching, including privately hosted sites such as separate Blogspot tenants.

## Field interpretation

- `metadata.source_country` contains a nullable ISO code, retained FIPS code,
  estimate status, matching method and source reference. Conflicting current
  assignments abstain. Historical disagreements are retained. Language,
  country-code domain suffixes and mentioned places cannot fill an unknown.
- `metadata.gkg.mentioned_countries` and `metadata.ggg.mentioned_countries`
  describe locations mentioned in the article. They never establish origin.
- Titles, descriptions, authors and outlet labels use the parent whitelist and
  its length limits. GKG keeps up to 160 themes, 15 people and 15 organizations.
  Complete source files remain cached. `tone` is a whole-document annotation,
  not sentiment toward each entity or a correctness score.
- `observed_at` remains the original GDELT observation timestamp. `gal_date`
  retains the supplied GAL date, which can represent publisher metadata or a
  fallback. `published_at_reported` comes only from explicit GEMG date tags and
  is not verified against a publisher.
- GKG/GGG article joins require exact URLs and timestamps within 60 minutes.
  GGG host-level country assignments may use the entire observation day.
  This is retrospective enrichment: the GGG daily file was published later.
  Do not treat these annotations as available in a realtime backtest at 00:01.
- One nearby translated GKG file contains a malformed UTF-8 row. Such rows
  retain raw bytes in cache and get explicit replacement diagnostics when
  decoded. Reconstructed article text is never changed by these annotations.

The whole-minute GKG search is limited to the containing and next two
quarter-hour slots. Unmatched URLs are not proof that GDELT has no metadata
elsewhere. No paid BigQuery query, Jev call or publisher fetch is made.

Sources: [GDELT source-country methodology](https://blog.gdeltproject.org/mapping-the-media-a-geographic-lookup-of-gdelts-sources/),
[Global Geographic Graph](https://blog.gdeltproject.org/announcing-the-global-geographic-graph/),
[GKG 2.1 codebook](https://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf).
The [Public Suffix List](https://publicsuffix.org/list/) is used only to validate
domain boundaries, not to infer geography. It retains its MPL-2.0 notice in
the downloaded file. GeoNames crosswalk attribution is in the parent's
[third-party notices](../../docs/THIRD_PARTY.md).
