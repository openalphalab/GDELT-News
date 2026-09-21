"""Bounded, restartable GDELT backfill followed by continuous Hugging Face uploads.

The remote progress file is committed atomically with its data. No local source
is removed until that exact remote commit and its file hashes are verified.
"""
import argparse
import contextlib
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, CommitOperationAdd, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

import enrich_metadata as enrich

LOG = logging.getLogger("gdelt-worker")
FIRST = "20200101000100"
PIPELINE = "gdelt-types12-best-effort-enriched-v1"
GIB = 1024 ** 3
SCHEMA = pa.schema([
    ("observation_id", pa.string()), ("source_minute", pa.string()),
    ("raw_sha256", pa.string()), ("id", pa.int64()), ("type_id", pa.int64()),
    ("type", pa.int8()), ("lang", pa.string()), ("url", pa.string()),
    ("observed_at", pa.string()), ("fragments", pa.int64()),
    ("primary_fragments", pa.int64()), ("assembly", pa.string()),
    ("position_joins", pa.int64()), ("bounded_fallback", pa.bool_()),
    ("search", pa.string()), ("text", pa.string()),
    ("source_country_iso2", pa.string()), ("metadata_json", pa.string()),
])


def parse_minute(value):
    parsed = datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    if parsed.second or len(value) != 14 or value < FIRST:
        raise ValueError("Use a whole UTC minute at or after " + FIRST)
    return parsed


def stamp(value):
    return value.strftime("%Y%m%d%H%M%S")


def successor(value):
    return stamp(parse_minute(value) + timedelta(minutes=1))


def read_json(path):
    return json.loads(path.read_text(encoding="utf8"))


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    enrich.dump(path, value, True)
    if os.name == "posix":
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def remove_owned(path, root):
    """Only remove a named worker scratch child, never arbitrary input paths."""
    path, root = path.resolve(), root.resolve()
    if path.parent != root or path.name not in {"batch", "cache"}:
        raise ValueError("Refusing to remove a path outside worker scratch")
    if path.exists():
        shutil.rmtree(path)


def check_space(root, reserve):
    if shutil.disk_usage(root).free < reserve * GIB:
        raise RuntimeError(f"Less than {reserve} GiB free; preserving pending data")


def run(args, timeout=1800):
    subprocess.run([str(arg) for arg in args], check=True, timeout=timeout,
                   stdout=subprocess.DEVNULL)


def file_record(path, destination):
    return {"path": destination, "bytes": path.stat().st_size,
            "sha256": enrich.digest(path)}


def parquet_row(row, minute, raw_sha):
    detail = row.get("metadata", {})
    result = {k: v for k, v in row.items() if k != "metadata"}
    result.update(observation_id=hashlib.sha256(f"{raw_sha}:{row['id']}".encode()).hexdigest(),
                  source_minute=minute, raw_sha256=raw_sha,
                  source_country_iso2=detail.get("source_country", {}).get("iso2"),
                  metadata_json=json.dumps(detail, ensure_ascii=False, separators=(",", ":"), allow_nan=False))
    return result


def build_batch(args, start, maximum_end):
    """Cache minute outputs, then publish immutable shards; retry from the same start."""
    batch = args.state / "batch"
    ready = batch / "ready.json"
    if ready.exists():
        record = read_json(ready)
        if record["start"] != start:
            raise RuntimeError("Pending batch does not match remote progress")
        for item in record["files"]:
            path = batch / item["local"]
            if path.stat().st_size != item["bytes"] or enrich.digest(path) != item["sha256"]:
                raise RuntimeError("Pending publication checksum mismatch")
        return record
    batch.mkdir(parents=True, exist_ok=True)
    request = batch / "request.json"
    wanted = {"start": start, "maximum_end": maximum_end, "pipeline": PIPELINE}
    if request.exists():
        old = read_json(request)
        if old["start"] != start or old["pipeline"] != PIPELINE:
            raise RuntimeError("Pending request conflicts with remote progress")
        maximum_end = old["maximum_end"]
    else:
        write_json(request, wanted)
    archive = batch / "archive"
    archive.mkdir(exist_ok=True)
    cache = args.state / "cache"
    minute, observations, outcomes, added = start, 0, [], set()
    parquet_path, evidence_path, llm_path = (batch / x for x in
                                           ("observations.parquet", "evidence.tar", "observations.jsonl.gz"))
    # Rebuild only uncommitted aggregate files after a crash; cached minute
    # artifacts and original compressed downloads are independently verified.
    with pq.ParquetWriter(parquet_path, SCHEMA, compression="zstd", compression_level=9,
                          use_dictionary=["lang", "type", "assembly", "search", "source_country_iso2"]) as writer, \
            tarfile.open(evidence_path, "w") as evidence, \
            llm_path.open("wb") as llm_raw, \
            gzip.GzipFile(fileobj=llm_raw, mode="wb", mtime=0, compresslevel=6) as llm:
        def preserve(path, name):
            if name not in added:
                evidence.add(path, arcname=name, recursive=False)
                added.add(name)

        while minute <= maximum_end:
            check_space(args.state, args.min_free_gib)
            run([args.collector, "--archive", archive, "--types", "1,2",
                 "--type2-best-effort", "--threads", args.threads,
                 "--max-expanded-mib", args.max_expanded_mib, "--max-fragments", args.max_fragments,
                 "collect", "--start", minute, "--end", minute,
                 "--downloads", "1", "--min-free-gib", args.min_free_gib,
                 "--max-download-mib", args.max_download_mib])
            runs = sorted((archive / "runs").iterdir())
            latest = runs[-1]
            summary = read_json(latest / "summary.json")
            if summary["failed"] or summary["completed"] + summary["missing"] != 1:
                raise RuntimeError("Collector did not resolve exactly one source minute")
            status = read_json(latest / (minute + ".json"))
            outcomes.append({"minute": minute, "status": status["status"]})
            preserve(latest / (minute + ".json"), f"minutes/{minute}/status.json")
            if summary["missing"]:
                LOG.info("source_missing minute=%s (HTTP 404 recorded)", minute)
            else:
                raw = archive / "raw" / minute[:4] / minute[4:6] / minute[6:8] / (minute + ".webngrams.json.gz")
                raw_sha = enrich.digest(raw)
                matches = list((archive / "articles").glob(f"*/{raw_sha}.manifest.json"))
                if len(matches) != 1:
                    raise RuntimeError("Expected one reconstruction manifest")
                manifest = read_json(matches[0])
                articles = archive / manifest["output"]
                if manifest["raw"]["sha256"] != raw_sha or enrich.digest(articles) != manifest["output_sha256"]:
                    raise RuntimeError("Reconstruction checksum mismatch")
                exported = batch / ("export-" + minute)
                enriched = batch / ("enriched-" + minute)
                if not exported.exists():
                    run([args.exporter, "--articles", articles, "--output-directory", exported])
                if not enriched.exists():
                    with contextlib.redirect_stdout(io.StringIO()):
                        enrich.build(exported / "observations.jsonl", cache, enriched, parse_minute(minute))
                report = read_json(enriched / "enrichment-report.json")
                if any(s["status"] == "failed" for s in report["source_files"]):
                    # Keep successful caches; retry transient metadata errors on
                    # the next attempt instead of publishing silently incomplete data.
                    for p in enriched.iterdir():
                        p.unlink()
                    enriched.rmdir()
                    raise RuntimeError("Transient enrichment source failure; retrying this batch")
                input_path = enriched / "observations.enriched.jsonl"
                expected = next(x["sha256"] for x in report["files"] if x["name"] == input_path.name)
                if enrich.digest(input_path) != expected:
                    raise RuntimeError("Enriched observation checksum mismatch")
                preserve(raw, f"raw/{minute}.webngrams.json.gz")
                preserve(matches[0], f"minutes/{minute}/reconstruction-manifest.json")
                preserve(articles, f"minutes/{minute}/articles.jsonl.gz")
                preserve(enriched / "enrichment-report.json", f"minutes/{minute}/enrichment-report.json")
                for source in report["source_files"]:
                    if source["status"] == "available":
                        p = cache / source["path"]
                        if enrich.digest(p) != source["sha256"]:
                            raise RuntimeError("Enrichment evidence checksum mismatch")
                        preserve(p, "metadata-sources/" + source["path"])
                with input_path.open("rb") as stream:
                    header = json.loads(next(stream))
                    header.update(source_minute=minute, raw_sha256=raw_sha)
                    llm.write((json.dumps(header, ensure_ascii=False, separators=(",", ":")) + "\n").encode())
                    rows, minute_rows = [], 0
                    for line in stream:
                        row = json.loads(line)
                        llm.write(line)
                        rows.append(parquet_row(row, minute, raw_sha))
                        observations += 1
                        minute_rows += 1
                        if len(rows) == 512:
                            writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
                            rows.clear()
                    if rows:
                        writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
                    if minute_rows != report["observations"]:
                        raise RuntimeError("Enriched record count mismatch")
                LOG.info("reconstructed minute=%s observations=%d total=%d", minute, minute_rows, observations)
            # Seal early before accumulating too much local data. Actual minute
            # boundaries, including 404s, are recorded in the commit manifest.
            if evidence_path.stat().st_size >= args.shard_gib * GIB:
                break
            if shutil.disk_usage(args.state).free < (args.min_free_gib + 4) * GIB:
                break
            if minute == maximum_end:
                break
            minute = successor(minute)
    if pq.ParquetFile(parquet_path).metadata.num_rows != observations:
        raise RuntimeError("Parquet row count mismatch")
    prefix = f"{start[:4]}/{start[4:6]}/{start}-{minute}"
    files = []
    for path, destination in [(parquet_path, f"data/{prefix}.parquet"),
                              (llm_path, f"llm/{prefix}.jsonl.gz"),
                              (evidence_path, f"evidence/{prefix}.tar")]:
        files.append({**file_record(path, destination), "local": path.name})
    manifest_path = batch / "manifest.json"
    record = {"schema": 1, "pipeline": PIPELINE, "start": start, "end": minute,
              "next_minute": successor(minute), "observations": observations,
              "code_revision": os.environ.get("GDELT_CODE_REVISION", "unknown"),
              "missing_minutes": [x["minute"] for x in outcomes if x["status"] == "missing"],
              "minutes": len(outcomes), "files": files}
    write_json(manifest_path, record)
    record["files"] = files + [{**file_record(manifest_path, f"manifests/{prefix}.json"), "local": "manifest.json"}]
    write_json(ready, record)
    return record


class Hub:
    def __init__(self, repo, token):
        self.repo, self.token = repo, token
        self.api = HfApi(token=token)

    def ensure_repo(self):
        # The repository is provisioned separately. A token scoped to this one
        # dataset does not need account-wide repository-creation permissions.
        if self.api.repo_info(self.repo, repo_type="dataset").private:
            raise RuntimeError("Destination must be public; refusing to change an existing private repository")

    def progress(self):
        info = self.api.repo_info(self.repo, repo_type="dataset")
        try:
            p = hf_hub_download(self.repo, "progress.json", repo_type="dataset",
                                revision=info.sha, token=self.token)
            return read_json(Path(p)), info.sha
        except EntryNotFoundError:
            return None, info.sha

    def verify(self, files, revision):
        remote = {x.path: x for x in self.api.get_paths_info(self.repo, [x["path"] for x in files],
                                                           repo_type="dataset", revision=revision)}
        for item in files:
            found = remote.get(item["path"])
            if found is None or found.size != item["bytes"]:
                raise RuntimeError("Remote artifact is missing or has the wrong size")
            if found.lfs:
                sha = found.lfs.sha256
                if sha != item["sha256"]:
                    raise RuntimeError("Remote artifact SHA256 mismatch")
            else:
                # Small ordinary Git blobs have a different object hash.
                body = hf_hub_download(self.repo, item["path"], repo_type="dataset",
                                       revision=revision, token=self.token)
                if enrich.digest(Path(body)) != item["sha256"]:
                    raise RuntimeError("Remote small-file checksum mismatch")

    def commit(self, batch, record, progress, parent):
        write_json(batch / "progress.json", progress)
        ops = [CommitOperationAdd(path_in_repo=x["path"], path_or_fileobj=batch / x["local"])
               for x in record["files"]]
        ops.append(CommitOperationAdd(path_in_repo="progress.json", path_or_fileobj=batch / "progress.json"))
        ops.append(CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=Path(__file__).parents[1] / "deploy/dataset-card.md"))
        result = self.api.create_commit(self.repo, repo_type="dataset", operations=ops,
                                       parent_commit=parent,
                                       commit_message=f"GDELT {record['start']} through {record['end']}")
        return result.oid


def publish(hub, batch, record, initial_start):
    for item in record["files"]:
        path = batch / item["local"]
        if path.stat().st_size != item["bytes"] or enrich.digest(path) != item["sha256"]:
            raise RuntimeError("Pending publication checksum mismatch")
    remote, parent = hub.progress()
    next_minute = remote["next_minute"] if remote else initial_start
    if remote and remote.get("pipeline") != PIPELINE:
        raise RuntimeError("Remote dataset uses another pipeline")
    manifest = record["files"][-1]
    if remote and next_minute == record["next_minute"] and remote.get("last_manifest_sha256") == manifest["sha256"]:
        # The prior commit succeeded but its response or local checkpoint was lost.
        hub.verify(record["files"], parent)
        return remote, parent
    if next_minute != record["start"]:
        raise RuntimeError("Concurrent writer or conflicting checkpoint; refusing to overwrite")
    progress = {"schema": 1, "pipeline": PIPELINE, "initial_start": initial_start,
                "next_minute": record["next_minute"], "last_end": record["end"],
                "last_manifest": manifest["path"], "last_manifest_sha256": manifest["sha256"],
                "total_observations": (remote or {}).get("total_observations", 0) + record["observations"],
                "published_bytes": (remote or {}).get("published_bytes", 0) + sum(x["bytes"] for x in record["files"]),
                "updated_at": datetime.now(timezone.utc).isoformat()}
    revision = hub.commit(batch, record, progress, parent)
    hub.verify(record["files"], revision)
    return progress, revision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="openalphalab/gdelt-news")
    parser.add_argument("--state", type=Path, default=Path("/data"))
    parser.add_argument("--token-file", type=Path, default=Path("/run/secrets/hf_token"))
    parser.add_argument("--start", default=FIRST)
    parser.add_argument("--batch-minutes", type=int, default=360)
    parser.add_argument("--shard-gib", type=float, default=4)
    parser.add_argument("--lag-hours", type=int, default=48)
    parser.add_argument("--min-free-gib", type=int, default=12)
    parser.add_argument("--threads", type=int, default=3)
    parser.add_argument("--max-expanded-mib", type=int, default=2048)
    parser.add_argument("--max-download-mib", type=int, default=512)
    parser.add_argument("--max-fragments", type=int, default=8_000_000)
    parser.add_argument("--collector", default="gdelt-type1")
    parser.add_argument("--exporter", default="gdelt-export")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    parse_minute(args.start)
    if not (1 <= args.batch_minutes <= 1440 and 0.01 <= args.shard_gib <= 8
            and args.lag_hours >= 48 and args.min_free_gib >= 8 and 1 <= args.threads <= 16
            and 512 <= args.max_expanded_mib <= 8192 and 128 <= args.max_download_mib <= 2048
            and 2_000_000 <= args.max_fragments <= 16_000_000):
        parser.error("Invalid resource settings; lag must be >=48 hours for retrospective daily enrichment")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args.state.mkdir(parents=True, exist_ok=True)
    # Linux deployment lock covers collection, publication and cleanup.
    import fcntl
    with (args.state / ".worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        token = args.token_file.read_text().strip()
        if not token.startswith("hf_"):
            raise RuntimeError("A Hugging Face write token is required in the mounted secret file")
        hub = Hub(args.repo, token)
        hub.ensure_repo()
        failures = 0
        while True:
            try:
                remote, _ = hub.progress()
                if remote and (remote.get("pipeline") != PIPELINE or remote.get("initial_start") != args.start):
                    raise RuntimeError("Remote checkpoint does not match configured pipeline/start")
                # Pending ready data may belong to an acknowledged remote commit;
                # publish() verifies it again before cleaning up after a restart.
                ready = args.state / "batch/ready.json"
                if ready.exists():
                    record = read_json(ready)
                else:
                    start = (remote or {}).get("next_minute", args.start)
                    cutoff = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(hours=args.lag_hours)
                    if parse_minute(start) > cutoff:
                        write_json(args.state / "status.json", {"state": "waiting", "next_minute": start,
                                                               "checked_at": datetime.now(timezone.utc).isoformat()})
                        if args.once:
                            return
                        time.sleep(60)
                        continue
                    # Publish the first source minute immediately to prove the
                    # complete chain, then use larger efficient batches.
                    span = args.batch_minutes if remote else 1
                    end = stamp(min(parse_minute(start) + timedelta(minutes=span - 1), cutoff))
                    record = build_batch(args, start, end)
                progress, revision = publish(hub, args.state / "batch", record, args.start)
                write_json(args.state / "checkpoint.json", {**progress, "commit": revision})
                write_json(args.state / "status.json", {"state": "published", "next_minute": progress["next_minute"],
                                                       "checked_at": datetime.now(timezone.utc).isoformat()})
                LOG.info("published start=%s end=%s rows=%d commit=%s", record["start"], record["end"], record["observations"], revision)
                remove_owned(args.state / "batch", args.state)
                remove_owned(args.state / "cache", args.state)
                failures = 0
                if args.once:
                    return
            except Exception as exc:
                failures += 1
                # Avoid logging HTTP exception bodies/headers or credential values.
                detail = str(exc) if type(exc) in (RuntimeError, ValueError) else "See source-stage logs or HTTP status"
                response = getattr(exc, "response", None)
                LOG.error("batch_pending error_type=%s status=%s attempt=%d detail=%s; data retained",
                          type(exc).__name__, getattr(response, "status_code", None), failures, detail)
                write_json(args.state / "status.json", {"state": "retrying", "error_type": type(exc).__name__,
                                                       "attempt": failures, "checked_at": datetime.now(timezone.utc).isoformat()})
                if args.once:
                    raise
                time.sleep(min(900, 15 * 2 ** min(failures, 6)))


if __name__ == "__main__":
    main()
