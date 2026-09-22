"""Compact Parquet with native metadata, historical backfill and prompt live collection."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import time

import pyarrow as pa
import pyarrow.parquet as pq

from worker import (FIRST, GIB, Hub, check_space, file_record, parse_minute,
                    read_json, remove_owned, run, stamp, successor, write_json)
from enrich_metadata import digest

LOG = logging.getLogger("gdelt-compact")
PIPELINE = "gdelt-types12-parquet-native-v3"
# The website CDN can cache an early 404 for an hour. GDELT's public bucket
# returns uncached missing-object responses, allowing prompt late-file recovery.
SOURCE_BASE_URL = "https://storage.googleapis.com/data.gdeltproject.org/gdeltv3/webngrams"
NATIVE_FIELDS = [
    ("source_minute", pa.string()), ("raw_sha256", pa.string()),
    ("id", pa.int64()), ("type_id", pa.int64()), ("fragments", pa.int64()),
    ("primary_fragments", pa.int64()), ("assembly", pa.string()),
    ("position_joins", pa.int64()), ("bounded_fallback", pa.bool_()), ("search", pa.string()),
]
BASE_COLUMNS = ["date", "language", "source_url", "text"]
SCHEMA = pa.schema([("date", pa.string()), ("language", pa.string()),
                    ("source_url", pa.string()), ("text", pa.string()),
                    pa.field("observation_id", pa.string(), nullable=False),
                    pa.field("type", pa.int8(), nullable=False),
                    ("metadata", pa.struct(NATIVE_FIELDS))])


def observation_id(row, minute, raw_sha):
    """Identify the source group, independently of text assembly and row order."""
    parse_minute(minute)
    if (not isinstance(raw_sha, str) or len(raw_sha) != 64
            or any(c not in "0123456789abcdef" for c in raw_sha)
            or type(row.get("type")) is not int or row["type"] not in (1, 2)
            or any(not isinstance(row.get(k), str) or not row[k].strip()
                   for k in ("observed_at", "lang", "url"))):
        raise ValueError("Incomplete or invalid observation identity")
    identity = ["gdelt-webngrams-observation-v1", minute, raw_sha, row["type"],
                row["observed_at"], row["lang"], row["url"]]
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False,
                                     separators=(",", ":")).encode("utf8")).hexdigest()


def parquet_row(row, minute=None, raw_sha=None):
    metadata = {name: row.get(name) for name, _ in NATIVE_FIELDS}
    metadata["source_minute"] = minute or row.get("source_minute")
    metadata["raw_sha256"] = raw_sha or row.get("raw_sha256")
    return {"date": row["observed_at"], "language": row["lang"],
            "source_url": row["url"], "text": row["text"], "type": row["type"],
            "observation_id": observation_id(row, metadata["source_minute"], metadata["raw_sha256"]),
            "metadata": metadata}


def verify_local(batch, record):
    items = record["files"] + ([record["receipt"]] if record.get("receipt") else [])
    for item in items:
        path = batch / item["local"]
        if path.stat().st_size != item["bytes"] or digest(path) != item["sha256"]:
            raise RuntimeError("Pending publication checksum mismatch")


def publication_receipt(record):
    """A local receipt identifies checkpoint-only work without uploading a file."""
    return record.get("receipt") or record["files"][-1]


def public_record(record):
    """Adopt older pending empty batches without changing their receipt identity."""
    if record["observations"]:
        return record
    return {**record, "files": [], "receipt": {**publication_receipt(record), "path": None}}


def receipt_matches(acknowledged, original, public):
    receipt = publication_receipt(public)
    paths = {receipt["path"], publication_receipt(original)["path"]}
    return acknowledged.get("sha256") == receipt["sha256"] and acknowledged.get("path") in paths


def collect_window(args, archive, start, end):
    """Reuse the native HTTP pool and overlap bounded downloads with reconstruction."""
    count = int((parse_minute(end) - parse_minute(start)).total_seconds() // 60) + 1
    downloads = min(getattr(args, "download_workers", 4), count)
    # Each concurrent download may consume its full byte allowance. Leave extra
    # headroom for the other in-flight responses as well as the normal reserve.
    extra_gib = ((downloads - 1) * args.max_download_mib + 1023) // 1024
    started = time.monotonic()
    run([args.collector, "--archive", archive, "--types", "1,2", "--type2-best-effort",
         "--quarantine-invalid-metadata", "--threads", args.threads,
         "--max-expanded-mib", args.max_expanded_mib, "--max-fragments", args.max_fragments,
         "collect", "--start", start, "--end", end, "--downloads", downloads,
         "--base-url", SOURCE_BASE_URL,
         "--min-free-gib", args.min_free_gib + extra_gib, "--max-download-mib", args.max_download_mib])
    latest = sorted((archive / "runs").iterdir())[-1]
    request, summary = read_json(latest / "request.json"), read_json(latest / "summary.json")
    for key, expected in (("start", start), ("end", end)):
        actual = datetime.fromisoformat(request[key].replace("Z", "+00:00"))
        if actual != parse_minute(expected):
            raise RuntimeError("Collector window does not match requested minutes")
    if summary["failed"] or summary["completed"] + summary["missing"] != count:
        raise RuntimeError("Collector did not resolve every source minute")
    outcomes, minute = {}, start
    while minute <= end:
        item = read_json(latest / (minute + ".json"))
        actual = datetime.fromisoformat(item["minute"].replace("Z", "+00:00"))
        if actual != parse_minute(minute) or item["status"] not in ("complete", "missing"):
            raise RuntimeError("Collector returned an invalid per-minute outcome")
        outcomes[minute] = item["status"]
        minute = successor(minute)
    if sum(value == "complete" for value in outcomes.values()) != summary["completed"]:
        raise RuntimeError("Collector summary disagrees with per-minute outcomes")
    LOG.info("collected_window start=%s end=%s downloads=%d seconds=%.3f", start, end,
             downloads, time.monotonic() - started)
    return outcomes, request["profile"]


def build_batch(args, start, maximum_end, kind="forward"):
    batch = args.state / "batch"
    ready = batch / "ready.json"
    if ready.exists():
        record = read_json(ready)
        if (record["start"], record["kind"], record["pipeline"]) != (start, kind, PIPELINE):
            raise RuntimeError("Pending batch conflicts with requested work")
        verify_local(batch, record)
        return record
    batch.mkdir(parents=True, exist_ok=True)
    request = batch / "request.json"
    wanted = {"start": start, "maximum_end": maximum_end, "kind": kind, "pipeline": PIPELINE}
    if request.exists():
        old = read_json(request)
        if any(old[k] != wanted[k] for k in ("start", "kind", "pipeline")):
            raise RuntimeError("Pending request conflicts with remote progress")
        maximum_end = old["maximum_end"]
    else:
        write_json(request, wanted)
    archive = batch / "archive"
    archive.mkdir(exist_ok=True)
    parquet = batch / "observations.parquet"
    outcomes, observations, minute = [], 0, start
    window, window_end, profile = {}, start, None
    with pq.ParquetWriter(parquet, SCHEMA, compression="zstd", compression_level=9,
                          use_dictionary=["language"]) as writer:
        while minute <= maximum_end:
            check_space(args.state, args.min_free_gib)
            write_json(args.state / "status.json", {"state": "collecting", "kind": kind,
                       "minute": minute, "checked_at": datetime.now(timezone.utc).isoformat()})
            if minute not in window:
                window_end = min(maximum_end, stamp(parse_minute(minute) + timedelta(
                    minutes=getattr(args, "collect_window_minutes", 15) - 1)))
                window, profile = collect_window(args, archive, minute, window_end)
            outcome = {"minute": minute, "status": window[minute]}
            outcomes.append(outcome)
            if outcome["status"] == "missing":
                LOG.info("source_missing minute=%s", minute)
            else:
                raw = archive / "raw" / minute[:4] / minute[4:6] / minute[6:8] / (minute + ".webngrams.json.gz")
                raw_sha = digest(raw)
                manifest = read_json(archive / "articles" / profile / f"{raw_sha}.manifest.json")
                articles = archive / manifest["output"]
                if (manifest["profile"] != profile or manifest["raw"]["sha256"] != raw_sha
                        or digest(articles) != manifest["output_sha256"]):
                    raise RuntimeError("Reconstruction checksum/profile mismatch")
                counts = manifest["counts"]
                outcome.update(raw_sha256=raw_sha, raw_bytes=raw.stat().st_size,
                               source=f"https://data.gdeltproject.org/gdeltv3/webngrams/{minute}.webngrams.json.gz",
                               download_source=f"{SOURCE_BASE_URL}/{minute}.webngrams.json.gz",
                               observations=counts["articles"], type1=counts["type1_articles"],
                               type2=counts["type2_articles"],
                               quarantined_metadata_records=counts.get("quarantined_metadata_records", 0))
                # An all-quarantined input is a valid zero-row source, not an exporter error.
                if counts["articles"]:
                    exported = batch / ("export-" + minute + "-" + profile)
                    if not exported.exists():
                        run([args.exporter, "--articles", articles, "--output-directory", exported])
                    report = read_json(exported / "export-report.json")
                    input_path = exported / "observations.jsonl"
                    expected = next(x["sha256"] for x in report["files"] if x["name"] == input_path.name)
                    if digest(input_path) != expected:
                        raise RuntimeError("Export checksum mismatch")
                    with input_path.open("rb") as stream:
                        header = json.loads(next(stream))["meta"]
                        if header["raw_sha256"] != raw_sha or header["profile"] != profile:
                            raise RuntimeError("Export source/profile mismatch")
                        rows, minute_rows, identities = [], 0, set()
                        for line in stream:
                            row = parquet_row(json.loads(line), minute, raw_sha)
                            if row["observation_id"] in identities:
                                raise RuntimeError("Duplicate source observation identity")
                            identities.add(row["observation_id"])
                            rows.append(row)
                            minute_rows += 1
                            if len(rows) == 512:
                                writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
                                rows.clear()
                        if rows:
                            writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
                    if minute_rows != counts["articles"] or minute_rows != header["observations"]:
                        raise RuntimeError("Export record count mismatch")
                    observations += minute_rows
                    outcome["estimated_observations"] = report["estimated_observations"]
                    outcome["bounded_fallback_observations"] = report["bounded_fallback_observations"]
                LOG.info("reconstructed minute=%s observations=%d total=%d", minute, counts["articles"], observations)
            # Finish the collected window before sealing: every prefetched source
            # is represented in the publication before its local bytes expire.
            if minute == window_end and (parquet.stat().st_size >= args.shard_gib * GIB
                    or shutil.disk_usage(args.state).free < (args.min_free_gib + 4) * GIB
                    or minute == maximum_end):
                break
            minute = successor(minute)
    if pq.ParquetFile(parquet).metadata.num_rows != observations:
        raise RuntimeError("Parquet row count mismatch")
    prefix = f"{start[:4]}/{start[4:6]}/{start[6:8]}/{start}-{minute}"
    # Empty checks advance only the shared checkpoint. Their receipt stays local
    # until the checkpoint is verified; no per-check file is uploaded.
    files = ([{**file_record(parquet, f"data/{prefix}.parquet"), "local": parquet.name}]
             if observations else [])
    record = {"schema": 3, "pipeline": PIPELINE, "kind": kind, "start": start, "end": minute,
              "next_minute": successor(minute), "observations": observations,
              "code_revision": os.environ.get("GDELT_CODE_REVISION", "unknown"),
              "missing_minutes": [x["minute"] for x in outcomes if x["status"] == "missing"],
              "quarantined_metadata_records": sum(x.get("quarantined_metadata_records", 0) for x in outcomes),
              "minutes": outcomes, "files": files}
    manifest_path = batch / "manifest.json"
    write_json(manifest_path, record)
    receipt = {**file_record(manifest_path, f"manifests/{prefix}.json"), "local": manifest_path.name}
    if observations:
        record["files"] = files + [receipt]
    else:
        record["receipt"] = {**receipt, "path": None}
    write_json(ready, record)
    return record


def pending_within_window(remote, now, retry_hours):
    earliest = stamp(now - timedelta(hours=retry_hours))
    return sorted(m for m in (remote or {}).get("pending_missing_minutes", []) if m >= earliest)


def next_plan(args, remote, schedule, now):
    """Alternate due late-file checks with forward work near the live edge."""
    start = (remote or {}).get("next_minute", args.start)
    cutoff = now.replace(second=0, microsecond=0) - timedelta(minutes=args.lag_minutes)
    near_live = parse_minute(start) >= cutoff - timedelta(minutes=10)
    due = [m for m in pending_within_window(remote, now, args.retry_missing_hours)
           if schedule.get(m, {}).get("next_check", 0) <= now.timestamp()]
    if due and (parse_minute(start) > cutoff or (near_live and (remote or {}).get("last_action") != "repair")):
        minute = min(due, key=lambda m: (schedule.get(m, {}).get("next_check", 0), m))
        return minute, minute, "repair"
    if parse_minute(start) > cutoff:
        return None
    span = 1 if near_live or not remote else args.batch_minutes
    end = stamp(min(parse_minute(start) + timedelta(minutes=span - 1), cutoff))
    return start, end, "forward"


def defer_missing(schedule, minute, now):
    attempts = schedule.get(minute, {}).get("attempts", 0) + 1
    # No delay on other new files: only this absent file backs off to five minutes.
    schedule[minute] = {"attempts": attempts,
                        "next_check": now.timestamp() + min(300, 30 * 2 ** min(attempts - 1, 4))}


def check_upload_budget(hub, files, maximum_gb):
    info = hub.api.dataset_info(hub.repo, expand=["usedStorage"])
    used = info.used_storage
    if used is None or used < 0:
        raise RuntimeError("Storage usage unavailable; retaining local work until it can be checked")
    # Reserve a second copy of the payload for viewer conversion/version overhead.
    projected = used + 2 * sum(item["bytes"] for item in files)
    if projected >= maximum_gb * 1_000_000_000:
        raise RuntimeError(f"Dataset storage guard reached ({maximum_gb:g} GB); no upload or deletion performed")
    return used


def publish(hub, batch, record, initial_start, retry_hours=24, now=None, maximum_gb=None):
    verify_local(batch, record)
    original, record = record, public_record(record)
    now = now or datetime.now(timezone.utc)
    remote, parent = hub.progress()
    if remote and remote.get("schedule"):
        raise RuntimeError("Latest-first checkpoint requires --latest-first scheduling")
    if remote and (remote.get("pipeline") != PIPELINE or remote.get("initial_start") != initial_start):
        raise RuntimeError("Remote checkpoint uses another pipeline/start")
    receipt = publication_receipt(record)
    if remote and remote.get("last_receipt_sha256", remote.get("last_manifest_sha256")) == receipt["sha256"]:
        hub.verify(original["files"] if remote.get("last_manifest") == publication_receipt(original)["path"] else record["files"], parent)
        return remote, parent
    next_minute = (remote or {}).get("next_minute", initial_start)
    kind = record["kind"]
    pending = set((remote or {}).get("pending_missing_minutes", []))
    if kind == "repair":
        if record["start"] != record["end"] or record["start"] not in pending or record["missing_minutes"]:
            raise RuntimeError("Repair is absent, already published, or conflicts with progress")
        pending.remove(record["start"])
    elif next_minute != record["start"]:
        raise RuntimeError("Concurrent writer or conflicting checkpoint; refusing to overwrite")
    earliest = stamp(now - timedelta(hours=retry_hours))
    pending.update(m for m in record["missing_minutes"] if m >= earliest)
    pending = sorted(m for m in pending if m >= earliest)
    progress = {"schema": 3, "pipeline": PIPELINE, "initial_start": initial_start,
                "next_minute": next_minute if kind == "repair" else record["next_minute"],
                "last_end": remote["last_end"] if kind == "repair" else record["end"],
                "last_action": kind, "pending_missing_minutes": pending,
                "last_receipt_sha256": receipt["sha256"],
                "last_manifest": receipt["path"] or (remote or {}).get("last_manifest"),
                "last_manifest_sha256": receipt["sha256"] if receipt["path"] else (remote or {}).get("last_manifest_sha256"),
                "total_observations": (remote or {}).get("total_observations", 0) + record["observations"],
                "total_quarantined_metadata_records": (remote or {}).get("total_quarantined_metadata_records", 0) + record.get("quarantined_metadata_records", 0),
                "published_bytes": (remote or {}).get("published_bytes", 0) + sum(x["bytes"] for x in record["files"]),
                "updated_at": now.isoformat()}
    if maximum_gb is not None:
        check_upload_budget(hub, record["files"], maximum_gb)
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
    parser.add_argument("--shard-gib", type=float, default=0.25)
    parser.add_argument("--lag-minutes", type=int, default=1)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--retry-missing-hours", type=int, default=24)
    parser.add_argument("--max-hub-storage-gb", type=float, default=7000)
    parser.add_argument("--min-free-gib", type=int, default=12)
    parser.add_argument("--threads", type=int, default=3)
    parser.add_argument("--download-workers", type=int, default=4)
    parser.add_argument("--collect-window-minutes", type=int, default=15)
    parser.add_argument("--max-expanded-mib", type=int, default=2048)
    parser.add_argument("--max-download-mib", type=int, default=512)
    parser.add_argument("--max-fragments", type=int, default=8_000_000)
    parser.add_argument("--collector", default="gdelt-type1")
    parser.add_argument("--exporter", default="gdelt-export")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--latest-first", action="store_true",
                        help="Prioritize live minutes and backfill backward between live batches")
    args = parser.parse_args()
    parse_minute(args.start)
    if not (1 <= args.batch_minutes <= 1440 and 0.01 <= args.shard_gib <= 8
            and 1 <= args.lag_minutes <= 60 and 10 <= args.poll_seconds <= 300
            and 1 <= args.retry_missing_hours <= 168 and args.min_free_gib >= 8
            and 1 <= args.max_hub_storage_gb <= 7000
            and 1 <= args.threads <= 16 and 1 <= args.download_workers <= 8
            and 1 <= args.collect_window_minutes <= 30 and 512 <= args.max_expanded_mib <= 8192
            and 128 <= args.max_download_mib <= 2048 and 2_000_000 <= args.max_fragments <= 16_000_000):
        parser.error("Invalid resource or live-collection settings")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args.state.mkdir(parents=True, exist_ok=True)
    import fcntl
    with (args.state / ".worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        token = args.token_file.read_text().strip()
        if not token.startswith("hf_"):
            raise RuntimeError("A Hugging Face write token is required in the mounted secret file")
        hub = Hub(args.repo, token)
        hub.ensure_repo()
        if args.latest_first:
            from latest_first_worker import run_scheduler
            return run_scheduler(args, hub)
        retry_path = args.state / "late-retries.json"
        schedule = read_json(retry_path) if retry_path.exists() else {}
        failures = 0
        last_work_kind = None
        while True:
            try:
                remote, _ = hub.progress()
                if remote and remote.get("schedule"):
                    raise RuntimeError("Latest-first checkpoint requires --latest-first scheduling")
                if remote and (remote.get("pipeline") != PIPELINE or remote.get("initial_start") != args.start):
                    raise RuntimeError("Remote checkpoint does not match configured pipeline/start")
                now = datetime.now(timezone.utc)
                schedule = {m: v for m, v in schedule.items() if m in pending_within_window(remote, now, args.retry_missing_hours)}
                ready = args.state / "batch/ready.json"
                request = args.state / "batch/request.json"
                if ready.exists():
                    record = read_json(ready)
                    if record["pipeline"] != PIPELINE:
                        raise RuntimeError("Pending publication uses another pipeline")
                else:
                    if request.exists():
                        wanted = read_json(request)
                        if wanted["pipeline"] != PIPELINE:
                            raise RuntimeError("Pending request uses another pipeline")
                        plan = wanted["start"], wanted["maximum_end"], wanted["kind"]
                    else:
                        scheduling_progress = dict(remote or {})
                        if last_work_kind:
                            scheduling_progress["last_action"] = last_work_kind
                        plan = next_plan(args, scheduling_progress or None, schedule, now)
                    if not plan:
                        write_json(args.state / "status.json", {"state": "waiting", "next_minute": (remote or {}).get("next_minute", args.start),
                                   "checked_at": now.isoformat()})
                        if args.once:
                            return
                        time.sleep(args.poll_seconds)
                        continue
                    check_upload_budget(hub, [], args.max_hub_storage_gb)
                    record = build_batch(args, *plan)
                if record["kind"] == "repair" and record["missing_minutes"]:
                    # No empty remote shard or commit for a repeat 404; the durable
                    # remote pending set ensures a restart never forgets this minute.
                    defer_missing(schedule, record["start"], datetime.now(timezone.utc))
                    last_work_kind = "repair"
                    write_json(retry_path, schedule)
                    remove_owned(args.state / "batch", args.state)
                    if args.once:
                        return
                    continue
                progress, revision = publish(hub, args.state / "batch", record, args.start, args.retry_missing_hours,
                                             maximum_gb=args.max_hub_storage_gb)
                last_work_kind = record["kind"]
                write_json(args.state / "checkpoint.json", {**progress, "commit": revision})
                for minute in record["missing_minutes"]:
                    if minute in progress["pending_missing_minutes"]:
                        defer_missing(schedule, minute, datetime.now(timezone.utc))
                write_json(retry_path, schedule)
                write_json(args.state / "status.json", {"state": "published", "next_minute": progress["next_minute"],
                           "checked_at": datetime.now(timezone.utc).isoformat()})
                LOG.info("published kind=%s start=%s end=%s rows=%d commit=%s", record["kind"], record["start"], record["end"], record["observations"], revision)
                remove_owned(args.state / "batch", args.state)
                remove_owned(args.state / "cache", args.state)
                failures = 0
                if args.once:
                    return
            except Exception as exc:
                failures += 1
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
