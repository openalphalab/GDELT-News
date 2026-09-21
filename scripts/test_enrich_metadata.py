import json
import gzip
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout
from datetime import datetime, timezone

from enrich_metadata import Suffixes, bounded_lines, build, choose_country, dataset_urls, fetch, time_distance


class MetadataTests(unittest.TestCase):
    def setUp(self):
        self.psl = Suffixes(["com", "uk", "co.uk", "cn", "com.cn", "blogspot.com", "*.ck", "!www.ck"])

    def country(self, domain="news.example.com", direct=None, domain_codes=None, lookup=None):
        return choose_country(domain, direct or set(), domain_codes or set(), lookup or {},
                              {"CH": "China", "US": "United States"}, self.psl, 4, 0)

    def test_fips_not_iso_and_parent_mapping_has_provenance(self):
        result = self.country("shuju.aweb.com.cn", lookup={"aweb.com.cn": {"CH"}})
        self.assertEqual(result["iso2"], "CN")
        self.assertEqual(result["fips"], "CH")
        self.assertEqual(result["status"], "estimated")
        self.assertEqual(result["method"], "historical_lookup_parent")

    def test_private_hosting_boundary_and_hostname_boundary(self):
        result = self.country("alice.blogspot.com", lookup={"blogspot.com": {"US"}})
        self.assertEqual(result["status"], "unknown")
        result = self.country("notexample.com", lookup={"example.com": {"US"}})
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(self.psl.candidates("www.example.co.uk"), ["www.example.co.uk", "example.co.uk"])

    def test_psl_wildcards_exceptions_and_ip(self):
        self.assertEqual(self.psl.registrable("a.b.ck"), "a.b.ck")
        self.assertEqual(self.psl.registrable("a.www.ck"), "www.ck")
        self.assertEqual(self.psl.candidates("127.0.0.1"), [])

    def test_current_gdelt_assignment_discloses_historical_disagreement(self):
        result = self.country(direct={"CH"}, lookup={"example.com": {"US"}})
        self.assertEqual(result["method"], "GGG_exact_url")
        self.assertEqual(result["iso2"], "CN")
        self.assertEqual(result["historical_lookup_disagreement"], ["US"])

    def test_conflicts_and_unmapped_codes_do_not_become_known_countries(self):
        result = self.country(direct={"CH", "US"}, lookup={"example.com": {"US"}})
        self.assertIsNone(result["iso2"])
        self.assertEqual(result["status"], "conflicting")
        result = self.country(lookup={"example.com": {"ZZ"}})
        self.assertIsNone(result["iso2"])
        self.assertEqual(result["status"], "unmapped_fips")

    def test_unknown_is_not_guessed_from_country_tld(self):
        self.assertEqual(self.country("someunknownnews.cn")["status"], "unknown")

    def test_adjacent_files_cross_day_boundary_and_bad_dates_abstain(self):
        point = datetime(2025, 3, 16, 23, 59, tzinfo=timezone.utc)
        urls = dataset_urls(point)
        self.assertTrue(any("20250317001500.translation.gkg" in u for u in urls))
        self.assertEqual(time_distance("bad-date", point), float("inf"))
        self.assertEqual(time_distance("2025-03-17T00:00:00Z", point), 60)

    def test_corrupt_cached_dataset_fails_before_network(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sample.gz").write_bytes(b"corrupt")
            url = "https://data.gdeltproject.org/sample.gz"
            known = {url: {"url": url, "sha256": "a" * 64}}
            with self.assertRaisesRegex(ValueError, "Corrupt enrichment cache"):
                fetch(url, root, known)

    def test_expansion_limit_is_enforced(self):
        with self.assertRaisesRegex(ValueError, "limit exceeded"):
            list(bounded_lines(io.BytesIO(b"123\n456\n"), maximum=5))

    def test_end_to_end_distinguishes_source_country_from_mentioned_country(self):
        for source_country, expected in (("US", "US"), ("", None)):
            with self.subTest(source_country=source_country), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cache = root / "cache"
                cache.mkdir()
                minute = datetime(2025, 3, 16, 0, 1, tzinfo=timezone.utc)
                record = {"id": 1, "type_id": 1, "type": 1, "url": "https://example.com/a",
                          "observed_at": minute.isoformat(), "lang": "en", "text": "Unchanged text 中国"}
                original = root / "observations.jsonl"
                original.write_text(json.dumps({"meta": {"observations": 1}}) + "\n" + json.dumps(record) + "\n", encoding="utf8")
                (root / "export-report.json").write_text(json.dumps({"files": [{"name": original.name,
                    "sha256": hashlib.sha256(original.read_bytes()).hexdigest()}]}), encoding="utf8")
                ggg = {"URL": record["url"], "DateTime": "2025-03-16T00:15:00Z",
                       "DomainCountryCode": source_country, "CountryCode": "CH"}
                urls = dataset_urls(minute)
                for i, data in ((0, b""), (1, b"com\n"), (4, gzip.compress((json.dumps(ggg) + "\n").encode()))):
                    (cache / urls[i].rsplit("/", 1)[-1]).write_bytes(data)
                def local_fetch(url, cache, legacy):
                    path = cache / url.rsplit("/", 1)[-1]
                    return {"url": url, "path": path.name, "status": "available" if path.exists() else "missing"}
                with patch("enrich_metadata.fetch", side_effect=local_fetch), redirect_stdout(io.StringIO()):
                    build(original, cache, root / "out", minute)
                rows = (root / "out/observations.enriched.jsonl").read_text(encoding="utf8").splitlines()
                enriched = json.loads(rows[1])
                self.assertEqual(enriched["text"], record["text"])
                self.assertEqual(enriched["metadata"]["source_country"]["iso2"], expected)
                self.assertEqual(enriched["metadata"]["ggg"]["mentioned_countries"], [{"fips": "CH", "iso2": "CN"}])


if __name__ == "__main__":
    unittest.main()
