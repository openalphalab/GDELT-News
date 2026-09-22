"""One publisher, disjoint live/backward cursors, and independently retryable work."""
import copy
from datetime import datetime, timedelta, timezone
import logging
import time

import compact_worker as cw
from hub_traffic import rate_limit_delay

LOG = logging.getLogger("gdelt-latest-first")
SCHEDULE = "live-priority-backward-v1"
LANES = ("live", "backfill", "repair")
HUB_REFRESH_SECONDS = 30


def previous(minute):
    # The completed backfill sentinel can be one minute before FIRST.
    return cw.stamp(cw.parse_minute(minute) - timedelta(minutes=1))


def validate_remote(remote, initial_start):
    if remote and (remote.get("pipeline") != cw.PIPELINE
                   or remote.get("initial_start") != initial_start):
        raise RuntimeError("Remote checkpoint does not match configured pipeline/start")
    if remote and remote.get("schedule") not in (None, SCHEDULE):
        raise RuntimeError("Unknown remote scheduling version")


def bootstrap(args, remote, now):
    """Freeze the seam once; the first live data commit also publishes this state."""
    validate_remote(remote, args.start)
    floor = (remote or {}).get("next_minute", args.start)
    cutoff = now.replace(second=0, microsecond=0) - timedelta(minutes=args.lag_minutes)
    live_start = max(floor, cw.stamp(cutoff - timedelta(minutes=14)))
    result = copy.deepcopy(remote or {})
    result.update(schema=3, pipeline=cw.PIPELINE, initial_start=args.start,
                  schedule=SCHEDULE, live_start=live_start, live_next_minute=live_start,
                  next_minute=live_start, backfill_floor=floor,
                  backfill_next_end=previous(live_start),
                  legacy_last_end=(remote or {}).get("last_end"), lane_commits={})
    return result


def plan_for_lane(args, remote, schedule, now, lane):
    cutoff = cw.stamp(now.replace(second=0, microsecond=0) - timedelta(minutes=args.lag_minutes))
    if lane == "live":
        start = remote["live_next_minute"]
        if start > cutoff:
            return None
        end = min(cutoff, cw.stamp(cw.parse_minute(start) + timedelta(
            minutes=args.collect_window_minutes - 1)))
        return start, end, lane
    if lane == "backfill":
        end = remote["backfill_next_end"]
        if end < remote["backfill_floor"]:
            return None
        start = max(remote["backfill_floor"], cw.stamp(cw.parse_minute(end) - timedelta(
            minutes=args.collect_window_minutes - 1)))
        return start, end, lane
    if lane == "repair":
        due = [m for m in cw.pending_within_window(remote, now, args.retry_missing_hours)
               if schedule.get(m, {}).get("next_check", 0) <= now.timestamp()]
        if due:
            # Do not bury delayed current files behind hundreds of normal gaps
            # encountered while traversing yesterday's historical intervals.
            recent = [m for m in due if m >= remote["live_start"]]
            minute = min(recent or due, key=lambda m: (schedule.get(m, {}).get("next_check", 0), m))
            return minute, minute, lane
        return None
    raise ValueError("Unknown scheduling lane")


def pending_plan(state, lane):
    batch = state / lane / "batch"
    for name in ("ready.json", "request.json"):
        path = batch / name
        if path.exists():
            item = cw.read_json(path)
            if item["pipeline"] != cw.PIPELINE or item["kind"] != lane:
                raise RuntimeError("Pending work conflicts with lane/pipeline")
            return item["start"], item.get("maximum_end", item.get("end")), lane
    return None


def choose_work(args, remote, schedule, retries, now, last_lane, repair_streak=None):
    # Latest files always win. Allow four recent recovery probes between backward
    # chunks, but only one historical probe. A live interruption keeps this budget.
    streak = int(last_lane == "repair") if repair_streak is None else repair_streak
    repair = pending_plan(args.state, "repair") or plan_for_lane(args, remote, schedule, now, "repair")
    recent_repair = repair is not None and repair[0] >= remote["live_start"]
    yield_to_backfill = streak >= (4 if recent_repair else 1)
    order = ("live", "backfill", "repair") if yield_to_backfill else ("live", "repair", "backfill")
    for lane in order:
        if retries.get(lane, {}).get("next_check", 0) > now.timestamp():
            continue
        plan = pending_plan(args.state, lane) or plan_for_lane(args, remote, schedule, now, lane)
        if plan:
            return plan
    return None


def publish(hub, batch, record, initial_start, initial, retry_hours=24, now=None, maximum_gb=None,
            protected_missing=()):
    """Commit one lane without changing either other cursor; recover lost responses."""
    cw.verify_local(batch, record)
    original, record = record, cw.public_record(record)
    now = now or datetime.now(timezone.utc)
    remote, parent = hub.progress()
    validate_remote(remote, initial_start)
    if not (remote or {}).get("schedule"):
        if (remote or {}).get("next_minute", initial_start) != initial["backfill_floor"]:
            raise RuntimeError("Legacy progress changed after live/backfill boundary was selected")
        if record["kind"] != "live":
            raise RuntimeError("First latest-first publication must be live data")
        remote = initial
    progress = copy.deepcopy(remote)
    receipt = cw.publication_receipt(record)
    kind = record["kind"]
    if kind not in LANES or record["pipeline"] != cw.PIPELINE:
        raise RuntimeError("Unknown publication lane/pipeline")
    acknowledged = progress.get("lane_commits", {}).get(kind, {})
    if cw.receipt_matches(acknowledged, original, record):
        # Old acknowledged receipts still refer to an uploaded manifest; new
        # empty receipts are acknowledged entirely inside progress.json.
        hub.verify(original["files"] if acknowledged.get("path") else record["files"], parent)
        return progress, parent
    cw.parse_minute(record["start"])
    cw.parse_minute(record["end"])
    if record["start"] > record["end"] or record["next_minute"] != cw.successor(record["end"]):
        raise RuntimeError("Invalid publication interval")
    pending = set(progress.get("pending_missing_minutes", []))
    if kind == "live":
        if record["start"] != progress["live_next_minute"]:
            raise RuntimeError("Conflicting live cursor; refusing duplicate/overlapping data")
        progress.update(live_next_minute=record["next_minute"], next_minute=record["next_minute"],
                        last_end=record["end"])
    elif kind == "backfill":
        if (record["end"] != progress["backfill_next_end"]
                or record["start"] < progress["backfill_floor"]
                or record["end"] >= progress["live_start"]):
            raise RuntimeError("Conflicting backward cursor; refusing gaps/overlapping data")
        progress["backfill_next_end"] = previous(record["start"])
    else:
        if record["start"] != record["end"] or record["start"] not in pending or record["missing_minutes"]:
            raise RuntimeError("Repair is absent, already published, or conflicts with progress")
        pending.remove(record["start"])
    earliest = cw.stamp(now - timedelta(hours=retry_hours))
    pending.update(m for m in record["missing_minutes"] if m >= earliest)
    progress.update(
        pending_missing_minutes=sorted(m for m in pending if m >= earliest or m in protected_missing),
        last_action=kind, last_receipt_sha256=receipt["sha256"],
        total_observations=progress.get("total_observations", 0) + record["observations"],
        total_quarantined_metadata_records=progress.get("total_quarantined_metadata_records", 0)
            + record.get("quarantined_metadata_records", 0),
        published_bytes=progress.get("published_bytes", 0) + sum(x["bytes"] for x in record["files"]),
        updated_at=now.isoformat())
    if receipt["path"]:
        progress.update(last_manifest=receipt["path"], last_manifest_sha256=receipt["sha256"])
    progress.setdefault("lane_commits", {})[kind] = {"path": receipt["path"], "sha256": receipt["sha256"]}
    if maximum_gb is not None:
        cw.check_upload_budget(hub, record["files"], maximum_gb)
    revision = hub.commit(batch, record, progress, parent)
    hub.verify(record["files"], revision)
    return progress, revision


def drain_legacy(args, hub):
    """Finish any previously authorized in-flight batch before freezing its boundary."""
    batch = args.state / "batch"
    if not (batch / "request.json").exists() and not (batch / "ready.json").exists():
        return
    if (batch / "ready.json").exists():
        record = cw.read_json(batch / "ready.json")
    else:
        request = cw.read_json(batch / "request.json")
        record = cw.build_batch(args, request["start"], request["maximum_end"], request["kind"])
    if record["kind"] == "repair" and record["missing_minutes"]:
        retry_path = args.state / "late-retries.json"
        retries = cw.read_json(retry_path) if retry_path.exists() else {}
        cw.defer_missing(retries, record["start"], datetime.now(timezone.utc))
        cw.write_json(retry_path, retries)
        cw.remove_owned(batch, args.state)
        return
    progress, revision = cw.publish(hub, batch, record, args.start, args.retry_missing_hours,
                                    maximum_gb=args.max_hub_storage_gb)
    cw.write_json(args.state / "checkpoint.json", {**progress, "commit": revision})
    cw.remove_owned(batch, args.state)
    cw.remove_owned(args.state / "cache", args.state)
    LOG.info("finished_legacy_batch end=%s rows=%d commit=%s", record["end"], record["observations"], revision)


def run_scheduler(args, hub):
    """Called under the deployment-wide lock; one parsed input and one publisher."""
    retry_path = args.state / "late-retries.json"
    retry_state = args.state / "scheduler-retries.json"
    initial_path = args.state / "latest-first-bootstrap.json"
    schedule = cw.read_json(retry_path) if retry_path.exists() else {}
    retries = cw.read_json(retry_state) if retry_state.exists() else {}
    last_lane, global_failures, repair_streak = None, 0, 0
    remote, remote_checked_at = None, None
    while True:
        lane = None
        try:
            now = datetime.now(timezone.utc)
            cooldown = retries.get("hub", {}).get("next_check", 0) - now.timestamp()
            if cooldown > 0:
                if args.once:
                    return
                time.sleep(min(args.poll_seconds, cooldown))
                continue
            retries.pop("hub", None)
            drain_legacy(args, hub)
            # Missing-source probes do not change the Hub. Reuse a short-lived
            # snapshot instead of spending two API requests on every 404. Each
            # publication still fetches fresh progress and checks quota itself.
            if remote_checked_at is None or time.monotonic() - remote_checked_at >= HUB_REFRESH_SECONDS:
                remote, _ = hub.progress()
                validate_remote(remote, args.start)
                cw.check_upload_budget(hub, [], args.max_hub_storage_gb)
                remote_checked_at = time.monotonic()
            now = datetime.now(timezone.utc)
            if not (remote or {}).get("schedule"):
                if not initial_path.exists():
                    cw.write_json(initial_path, bootstrap(args, remote, now))
                initial = cw.read_json(initial_path)
                effective = initial
            else:
                effective = remote
                initial = remote
            schedule = {m: v for m, v in schedule.items()
                        if m in cw.pending_within_window(effective, now, args.retry_missing_hours)}
            # Before the first atomic live commit, no historical lane may move.
            if not (remote or {}).get("schedule"):
                plan = pending_plan(args.state, "live") or plan_for_lane(args, effective, schedule, now, "live")
                if retries.get("live", {}).get("next_check", 0) > now.timestamp():
                    plan = None
            else:
                plan = choose_work(args, effective, schedule, retries, now, last_lane, repair_streak)
            if not plan:
                cw.write_json(args.state / "status.json", {"state": "waiting", "schedule": SCHEDULE,
                    "live_next_minute": effective["live_next_minute"],
                    "backfill_next_end": effective["backfill_next_end"], "lane_retries": retries,
                    "checked_at": now.isoformat()})
                if args.once:
                    return
                time.sleep(args.poll_seconds)
                continue
            start, end, lane = plan
            lane_args = copy.copy(args)
            lane_args.state = args.state / lane
            lane_args.state.mkdir(parents=True, exist_ok=True)
            cw.write_json(args.state / "status.json", {"state": "collecting", "schedule": SCHEDULE,
                "lane": lane, "start": start, "end": end,
                "live_next_minute": effective["live_next_minute"],
                "backfill_next_end": effective["backfill_next_end"], "lane_retries": retries,
                "checked_at": now.isoformat()})
            record = cw.build_batch(lane_args, *plan)
            if lane == "backfill" and record["end"] != end:
                raise RuntimeError("Backward chunk sealed early; retaining it to avoid skipping its tail")
            if lane == "repair" and record["missing_minutes"]:
                cw.defer_missing(schedule, start, datetime.now(timezone.utc))
                cw.write_json(retry_path, schedule)
            else:
                active_repair = pending_plan(args.state, "repair")
                progress, revision = publish(hub, lane_args.state / "batch", record, args.start, initial,
                    args.retry_missing_hours, maximum_gb=args.max_hub_storage_gb,
                    protected_missing=(active_repair[0],) if active_repair else ())
                remote, remote_checked_at = progress, time.monotonic()
                cw.write_json(args.state / "checkpoint.json", {**progress, "commit": revision})
                for minute in record["missing_minutes"]:
                    if minute in progress["pending_missing_minutes"]:
                        cw.defer_missing(schedule, minute, datetime.now(timezone.utc))
                cw.write_json(retry_path, schedule)
                cw.write_json(args.state / "status.json", {"state": "published", "schedule": SCHEDULE,
                    "lane": lane, "live_next_minute": progress["live_next_minute"],
                    "backfill_next_end": progress["backfill_next_end"],
                    "checked_at": datetime.now(timezone.utc).isoformat()})
                LOG.info("published kind=%s start=%s end=%s rows=%d commit=%s", lane,
                         record["start"], record["end"], record["observations"], revision)
            cw.remove_owned(lane_args.state / "batch", lane_args.state)
            cw.remove_owned(lane_args.state / "cache", lane_args.state)
            retries.pop(lane, None)
            cw.write_json(retry_state, retries)
            if lane == "repair":
                repair_streak += 1
            elif lane == "backfill":
                repair_streak = 0
            last_lane, global_failures = lane, 0
            if args.once:
                return
        except Exception as exc:
            if args.once:
                raise
            now = datetime.now(timezone.utc)
            remote_checked_at = None  # Also recover an uncertain commit response.
            response = getattr(exc, "response", None)
            http_status = getattr(response, "status_code", None)
            cooldown = rate_limit_delay(exc, now)
            if cooldown is not None:
                # A shared API limit must pause every lane, including source
                # retries. Persist the reset so a restart does not hammer it.
                retries["hub"] = {"next_check": now.timestamp() + cooldown, "http_status": 429}
                cw.write_json(retry_state, retries)
            elif lane:
                attempts = retries.get(lane, {}).get("attempts", 0) + 1
                retries[lane] = {"attempts": attempts, "next_check": now.timestamp()
                                + min(900, 15 * 2 ** min(attempts, 6))}
                cw.write_json(retry_state, retries)
            else:
                global_failures += 1
            detail = str(exc) if type(exc) in (RuntimeError, ValueError) else "See source-stage logs or HTTP status"
            LOG.error("work_pending lane=%s error_type=%s http_status=%s cooldown_seconds=%s detail=%s; data retained",
                      lane, type(exc).__name__, http_status, cooldown, detail)
            cw.write_json(args.state / "status.json", {"state": "retrying", "schedule": SCHEDULE,
                "lane": lane, "error_type": type(exc).__name__, "http_status": http_status,
                "cooldown_seconds": cooldown, "lane_retries": retries,
                "checked_at": now.isoformat()})
            # A failed historical minute must not impose its backoff on live work.
            if lane is None and cooldown is None:
                time.sleep(min(900, 15 * 2 ** min(global_failures, 6)))
