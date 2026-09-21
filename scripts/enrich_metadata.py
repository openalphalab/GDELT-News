"""Enrich a reconstructed minute with source-linked GDELT metadata.

Uses bundled GKG/GAL/GEMG whitelist parsers and the FIPS crosswalk.
Never fetches publisher URLs or runs inference.
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import urllib.error
import urllib.request
from urllib.parse import urlsplit
import zipfile

PROJECT = Path(__file__).resolve().parents[1]
from metadata_parsers import FIPS_ISO, number, parse_gal, parse_gemg, parse_gkg, timestamp

BASE = "https://data.gdeltproject.org/"
LOOKUP = BASE + "blog/2018-news-outlets-by-country-may2018-update/MASTER-GDELTDOMAINSBYCOUNTRY-MAY2018.TXT"
PSL = "https://publicsuffix.org/list/public_suffix_list.dat"
MAX_DOWNLOAD = 128 * 1024 * 1024
MAX_EXPANDED = 512 * 1024 * 1024
MAX_LINE = 8 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(128 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic(path, body):
    fd, tmp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def dump(path, value, pretty=False):
    atomic(path, (json.dumps(value, ensure_ascii=False, allow_nan=False,
                           indent=2 if pretty else None,
                           separators=None if pretty else (",", ":")) + "\n").encode("utf8"))


def host(value):
    value = value.rstrip(".").lower()
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        return value.encode("idna").decode("ascii")


class Suffixes:
    """PSL longest-match rules, including wildcard exceptions and private hosts."""
    def __init__(self, lines):
        self.exact, self.wild, self.exception = set(), set(), set()
        for line in lines:
            rule = line.strip().split("//", 1)[0].strip()
            if not rule:
                continue
            rule = rule.split()[0]
            target = self.exception if rule.startswith("!") else self.wild if rule.startswith("*.") else self.exact
            target.add(host(rule.removeprefix("!").removeprefix("*.")))

    def registrable(self, domain):
        domain = host(domain)
        try:
            ipaddress.ip_address(domain)
            return None
        except ValueError:
            pass
        labels = domain.split(".")
        suffix_length = 1
        for i in range(len(labels)):
            suffix = ".".join(labels[i:])
            if suffix in self.exception:
                suffix_length = len(labels) - i - 1
                break
            if suffix in self.exact:
                suffix_length = max(suffix_length, len(labels) - i)
            if i > 0 and suffix in self.wild:
                suffix_length = max(suffix_length, len(labels) - i + 1)
        return ".".join(labels[-suffix_length - 1:]) if len(labels) > suffix_length else None

    def candidates(self, domain):
        domain = host(domain)
        registered = self.registrable(domain)
        if not registered:
            return []
        labels = domain.split(".")
        return [".".join(labels[i:]) for i in range(len(labels) - len(registered.split(".")) + 1)]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def fetch(url, cache, legacy):
    """Fixed dataset URLs only. Checksummed cache; bounded, TLS-verified reads."""
    filename = url.rsplit("/", 1)[-1]
    path = cache / filename
    sidecar = cache / (filename + ".source.json")
    known = json.loads(sidecar.read_text(encoding="utf8")) if sidecar.exists() else legacy.get(url)
    if path.exists() and known:
        require(known["url"] == url and digest(path) == known["sha256"], f"Corrupt enrichment cache: {filename}")
        return {**known, "path": filename, "status": "available"}
    try:
        opener = urllib.request.build_opener(NoRedirect)
        request = urllib.request.Request(url, headers={"User-Agent": "Firstlight-GDELT-enrichment/0.4"})
        with opener.open(request, timeout=45) as response:
            require(response.status == 200, "Expected HTTP 200")
            require(int(response.headers.get("Content-Length", 0)) <= MAX_DOWNLOAD, "Oversized download")
            body = response.read(MAX_DOWNLOAD + 1)
            require(len(body) <= MAX_DOWNLOAD, "Oversized download")
            evidence = {"url": url, "path": filename, "status": "available", "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(), "last_modified": response.headers.get("Last-Modified"),
                        "fetched_at": datetime.now(timezone.utc).isoformat()}
        atomic(path, body)
        dump(sidecar, evidence, True)
        return evidence
    except urllib.error.HTTPError as exc:
        return {"url": url, "path": filename, "status": "missing" if exc.code == 404 else "failed", "http_status": exc.code}
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {"url": url, "path": filename, "status": "failed", "error": str(exc)}


def dataset_urls(minute):
    floor = minute.replace(minute=minute.minute // 15 * 15)
    stamp = minute.strftime("%Y%m%d%H%M%S")
    urls = [LOOKUP, PSL, BASE + f"gdeltv3/gal/{stamp}.gal.json.gz",
            BASE + f"gdeltv3/gemg/{stamp}.gemg.json.gz",
            BASE + f"gdeltv3/ggg/{minute:%Y%m%d}.ggg.v1.english.json.gz"]
    for i in range(3):
        point = floor + timedelta(minutes=15 * i)
        urls.extend(BASE + f"gdeltv2/{point:%Y%m%d%H%M%S}{translation}.gkg.csv.zip"
                    for translation in ("", ".translation"))
    return urls


def bounded_lines(stream, maximum=MAX_EXPANDED):
    total = 0
    while line := stream.readline(MAX_LINE + 1):
        total += len(line)
        require(len(line) <= MAX_LINE and total <= maximum, "Expanded dataset/row limit exceeded")
        yield line


def time_distance(value, observed):
    try:
        return abs((timestamp(value) - observed).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return float("inf")


def choose_country(domain, url_codes, domain_codes, lookup, names, psl, ggg_id, lookup_id):
    """Never reinterpret a mentioned-country code as a publisher-country code."""
    candidates = []
    for key in psl.candidates(domain):
        if key in lookup:
            candidates = sorted(lookup[key])
            break
    else:
        key = None
    evidence = None
    for values, method in ((url_codes, "GGG_exact_url"), (domain_codes, "GGG_exact_host_same_day")):
        if values:
            codes = sorted(values)
            if len(codes) != 1:
                return {"iso2": None, "status": "conflicting", "fips_candidates": codes, "method": method, "source": ggg_id}
            code = codes[0]
            evidence = {"fips": code, "method": method, "source": ggg_id, "matched_domain": domain}
            if candidates and code not in candidates:
                evidence["historical_lookup_disagreement"] = candidates
            break
    if evidence is None and candidates:
        if len(candidates) != 1:
            return {"iso2": None, "status": "conflicting", "fips_candidates": candidates,
                    "method": "historical_lookup", "source": lookup_id}
        evidence = {"fips": candidates[0], "method": "historical_lookup_exact" if key == domain else "historical_lookup_parent",
                    "source": lookup_id, "matched_domain": key, "historical_snapshot": "2018-05"}
    if evidence is None:
        return {"iso2": None, "status": "unknown"}
    evidence.update(iso2=FIPS_ISO.get(evidence["fips"]), name=names.get(evidence["fips"]),
                    status="estimated" if evidence["fips"] in FIPS_ISO else "unmapped_fips")
    return evidence


def build(input_path, cache, output, minute):
    require(not output.exists(), "Output directory already exists; choose a new directory")
    input_report = json.loads((input_path.parent / "export-report.json").read_text(encoding="utf8"))
    expected = next(x["sha256"] for x in input_report["files"] if x["name"] == input_path.name)
    require(digest(input_path) == expected, "Input export checksum mismatch")
    require(input_path.stat().st_size <= MAX_EXPANDED, "Oversized export")
    with input_path.open("rb") as stream:
        lines = iter(bounded_lines(stream))
        meta = json.loads(next(lines))["meta"]
        records = [json.loads(line) for line in lines]
    require(len(records) == meta["observations"] and len(records) <= 100_000, "Invalid observation count")
    require([r["id"] for r in records] == list(range(1, len(records) + 1)), "Observation IDs must be sequential")
    require(all(time_distance(r["observed_at"], minute) < 60 for r in records), "Minute does not match observations")
    targets = {r["url"] for r in records}
    domains = {host(urlsplit(url).hostname) for url in targets}
    cache.mkdir(parents=True, exist_ok=True)
    legacy = {}
    if (cache / "downloads.json").exists():
        legacy = {r["url"]: r for r in json.loads((cache / "downloads.json").read_text(encoding="utf8")) if r.get("sha256")}
    with ThreadPoolExecutor(max_workers=4) as pool:
        sources = list(pool.map(lambda url: fetch(url, cache, legacy), dataset_urls(minute)))
    for i, source in enumerate(sources):
        source["id"] = i
        source["path"] = Path(source["path"]).name
    dump(cache / "enrichment-sources.json", sources, True)
    require(sources[1]["status"] == "available", "Public Suffix List unavailable; cannot safely map parent domains")
    psl = Suffixes((cache / sources[1]["path"]).read_text(encoding="utf8").splitlines())
    lookup, names = defaultdict(set), {}
    if sources[0]["status"] == "available":
        with (cache / sources[0]["path"]).open(encoding="utf8") as stream:
            for line in stream:
                values = line.rstrip("\r\n").split("\t")
                require(len(values) == 3, "Invalid source-country lookup row")
                d, code, name = values
                if re.fullmatch("[A-Z]{2}", code):
                    lookup[host(d)].add(code)
                    names.setdefault(code, name)
    gal, gemg, gkg = {}, {}, defaultdict(list)
    ggg_url, ggg_domain, ggg_places = defaultdict(set), defaultdict(set), defaultdict(set)
    statistics = []
    for source in sources[2:]:
        if source["status"] != "available":
            continue
        p, sid = cache / source["path"], source["id"]
        stats = {"source": sid, "rows": 0, "matched_rows": 0, "malformed_rows": 0, "utf8_replacement_rows": 0}
        if p.name.endswith(".zip"):
            with zipfile.ZipFile(p) as archive:
                entries = archive.infolist()
                require(len(entries) == 1 and entries[0].file_size <= MAX_EXPANDED, "Invalid GKG archive structure or size")
                with archive.open(entries[0]) as stream:
                    for raw in bounded_lines(stream):
                        stats["rows"] += 1
                        try:
                            line = raw.decode("utf8")
                            replaced = False
                        except UnicodeDecodeError:
                            line, replaced = raw.decode("utf8", errors="replace"), True
                            stats["utf8_replacement_rows"] += 1
                        fields = line.rstrip("\r\n").split("\t")
                        if len(fields) != 27:
                            stats["malformed_rows"] += 1
                            continue
                        if fields[4] not in targets or time_distance(fields[1], minute) > 3600:
                            continue
                        try:
                            parsed = parse_gkg(fields, source["url"])
                            if not parsed:
                                continue
                            state = parsed[1]
                            countries = {v.split("#")[2] for v in fields[10].split(";") if len(v.split("#")) >= 9}
                            tone = fields[15].split(",")
                            value = {"source": sid, "record_id": fields[0], "observed_at": state["seen_at"],
                                     "themes": state["themes"], "persons": state["persons"], "organizations": state["organizations"],
                                     "mentioned_countries": [{"fips": c, "iso2": FIPS_ISO.get(c)} for c in sorted(countries)],
                                     "tone": number(tone[0]), "word_count": number(tone[6]) if len(tone) > 6 else None,
                                     "utf8_replacements": replaced}
                            gkg[fields[4]].append(value)
                            stats["matched_rows"] += 1
                        except (ValueError, KeyError, IndexError, TypeError):
                            stats["malformed_rows"] += 1
        else:
            with gzip.open(p, "rb") as stream:
                for raw in bounded_lines(stream):
                    stats["rows"] += 1
                    try:
                        row = json.loads(raw)
                        if ".ggg." in p.name:
                            url = row["URL"]
                            domain = host(urlsplit(url).hostname)
                            code = row.get("DomainCountryCode", "").upper()
                            valid = bool(re.fullmatch("[A-Z]{2}", code)) and code in FIPS_ISO
                            if domain in domains and valid:
                                ggg_domain[domain].add(code)
                            if url in targets and time_distance(row["DateTime"], minute) <= 3600:
                                if valid:
                                    ggg_url[url].add(code)
                                if re.fullmatch("[A-Z]{2}", row.get("CountryCode", "")):
                                    ggg_places[url].add(row["CountryCode"])
                                stats["matched_rows"] += 1
                        else:
                            url = row["url"]
                            if url not in targets:
                                continue
                            if ".gal." in p.name:
                                state = parse_gal(row, source["url"])[1]
                                state["gal_date"] = row.get("date")
                                gal.setdefault(url, {"source": sid, **state})
                            else:
                                if time_distance(row.get("date"), minute) > 3600:
                                    continue
                                state = parse_gemg(row, source["url"])[1]
                                # Only publisher-reported dates from explicit metadata tags.
                                tags = {str(t.get("key", "")).lower(): t.get("value") for t in row.get("metatags", [])[:200] if isinstance(t, dict)}
                                for key in ("article:published_time", "datepublished", "date"):
                                    if tags.get(key):
                                        try:
                                            state["published_at_reported"] = {"value": timestamp(tags[key]).isoformat(), "field": key, "source": sid}
                                            break
                                        except (ValueError, TypeError, OverflowError):
                                            pass
                                if url not in gemg or time_distance(state.get("seen_at"), minute) < time_distance(gemg[url].get("seen_at"), minute):
                                    gemg[url] = {"source": sid, **state}
                            stats["matched_rows"] += 1
                    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
                        stats["malformed_rows"] += 1
        statistics.append(stats)
    counts, methods = Counter(), Counter()
    enriched = []
    for original in records:
        row = original.copy()
        url = row["url"]
        domain = host(urlsplit(url).hostname)
        detail = {"source_domain": domain}
        fields_from = {}
        for field, target in (("title", "title"), ("description", "description"), ("outlet", "publisher"), ("author", "author")):
            for candidate in (gal.get(url), gemg.get(url)):
                if candidate and candidate.get(field):
                    detail[target] = candidate[field]
                    fields_from[target] = candidate["source"]
                    counts[target] += 1
                    break
        if url in gal:
            detail["gal_date"] = gal[url].get("gal_date")
            counts["gal_matches"] += 1
        if url in gemg:
            counts["gemg_matches"] += 1
            if gemg[url].get("published_at_reported"):
                detail["published_at_reported"] = gemg[url]["published_at_reported"]
                counts["published_at_reported"] += 1
        detail["field_sources"] = fields_from
        country = choose_country(domain, ggg_url[url], ggg_domain[domain], lookup, names, psl, 4, 0)
        detail["source_country"] = country
        counts["source_country_" + country["status"]] += 1
        methods[country.get("method", "unknown")] += 1
        if country.get("historical_lookup_disagreement"):
            counts["source_country_disagreements"] += 1
        if gkg[url]:
            matches = sorted(gkg[url], key=lambda g: (time_distance(g["observed_at"], timestamp(row["observed_at"])), g["source"], g["record_id"]))
            detail["gkg"] = matches[0]
            detail["gkg"]["candidate_records"] = len(matches)
            counts["gkg_matches"] += 1
        if ggg_places[url]:
            detail["ggg"] = {"source": 4, "mentioned_countries": [{"fips": c, "iso2": FIPS_ISO.get(c)} for c in sorted(ggg_places[url])]}
            counts["ggg_matches_within_hour"] += 1
        row["metadata"] = detail
        enriched.append(row)
    parent_files = [PROJECT / "scripts/metadata_parsers.py", PROJECT / "scripts/fips-iso.json"]
    metadata = {**meta, "schema": "gdelt.llm.enriched.v1", "base_export_sha256": expected,
                "enrichment": {"sources": sources, "parser_provenance": [{"file": str(p.relative_to(PROJECT)), "sha256": digest(p)} for p in parent_files],
                "country_semantics": "source_country is GDELT's estimated outlet origin/affinity, not verified headquarters, reporter location, event location or author nationality. GKG/GGG mentioned_countries describe article content. Historical lookup is from May 2018; raw FIPS codes are retained alongside explicit ISO mappings. Unknowns stay null. Parent-domain joins stop at the Public Suffix List, including private hosting suffixes.",
                "time_semantics": "Retrospective enrichment. GKG/GGG article annotations require exact URL and observation within 60 minutes. GGG source-country host estimates may use the whole observation day; its daily file became available later. GAL.date may be publication time or a fallback, so it is retained as gal_date. published_at_reported is unverified publisher metadata. Original observed_at is unchanged.",
                "field_semantics": "Titles/descriptions/authors use the bundled parser whitelist and its length limits. GKG entities/themes use its caps (160 themes, 15 persons, 15 organizations); complete source files are retained. GKG tone is whole-document tone, not entity sentiment or truth. Images, publisher bodies and arbitrary JSON-LD are excluded from enrichment. Exact input text is unchanged."}}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as stage_name:
        stage = Path(stage_name)
        with (stage / "observations.enriched.jsonl").open("w", encoding="utf8", newline="\n") as out:
            out.write(json.dumps({"meta": metadata}, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n")
            for row in enriched:
                require({k: v for k, v in row.items() if k != "metadata"} == records[row["id"] - 1], "Original observation changed")
                out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n")
            out.flush()
            os.fsync(out.fileno())
        columns = list(enriched[0]) if enriched else ["id", "type", "text", "metadata"]
        columns[columns.index("text")] = "text_id"
        texts, ids, packed_rows = [], {}, []
        for row in enriched:
            if row["text"] not in ids:
                ids[row["text"]] = len(texts)
                texts.append(row["text"])
            packed_rows.append([ids[row["text"]] if c == "text_id" else row[c] for c in columns])
        dump(stage / "observations.enriched.packed.json", {"meta": metadata, "columns": columns, "observations": packed_rows, "texts": texts})
        first = next((r for r in enriched if r["type"] == 1 and r["metadata"].get("gkg")), enriched[0] if enriched else None)
        second = next((r for r in enriched if r["type"] == 2 and r["type_id"] == 25), enriched[-1] if enriched else None)
        samples = [] if first is None else [first] if first["id"] == second["id"] else [first, second]
        dump(stage / "preview.json", {"meta": metadata, "observations": samples}, True)
        report = {"observations": len(records), "types": dict(Counter(r["type"] for r in records)),
                  "coverage": dict(counts), "source_country_methods": dict(methods), "dataset_checks": statistics,
                  "coverage_scope": "Specified minute, adjacent GKG windows, same-day GGG and historical source lookup; not all GDELT records.",
                  "unavailable_sources": [s["id"] for s in sources if s["status"] != "available"],
                  "all_original_fields_and_texts_unchanged": True, "source_files": sources, "files": []}
        for name in ("observations.enriched.jsonl", "observations.enriched.packed.json"):
            data = (stage / name).read_bytes()
            compressed = gzip.compress(data, compresslevel=6, mtime=0)
            atomic(stage / (name + ".gz"), compressed)
            require(gzip.decompress(compressed) == data, "Compression round-trip failed")
            report["files"].append({"name": name, "bytes": len(data), "gzip_bytes": len(compressed), "sha256": digest(stage / name)})
        dump(stage / "enrichment-report.json", report, True)
        stage.rename(output)
    print(json.dumps({k: v for k, v in report.items() if k != "source_files"}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--minute", required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    require(bool(re.fullmatch(r"\d{12}00", args.minute)), "Use a whole UTC minute YYYYMMDDHHMM00")
    minute = datetime.strptime(args.minute, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    build(args.observations, args.cache, args.output_directory, minute)


if __name__ == "__main__":
    main()
