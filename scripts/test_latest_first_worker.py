from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx

import compact_worker as cw
import latest_first_worker as lf
from test_compact_worker import collection_result, settings
from test_worker import FakeHub


NOW = datetime(2026, 9, 22, 12, 0, 31, tzinfo=timezone.utc)


def item(root, start, end, kind, missing=()):
    batch = root / kind / "batch"
    batch.mkdir(parents=True, exist_ok=True)
    data = batch / "observations.parquet"
    data.write_bytes((kind + start + end).encode())
    manifest = batch / "manifest.json"
    record = {"pipeline": cw.PIPELINE, "kind": kind, "start": start, "end": end,
              "next_minute": cw.successor(end), "missing_minutes": list(missing), "observations": 7}
    cw.write_json(manifest, record)
    prefix = f"{start}-{end}"
    record["files"] = [
        {**cw.file_record(data, f"data/{prefix}.parquet"), "local": data.name},
        {**cw.file_record(manifest, f"manifests/{prefix}.json"), "local": manifest.name}]
    return batch, record


class LatestFirstTests(unittest.TestCase):
    def test_missing_probes_reuse_checkpoint_and_quota_until_refresh(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            args.once, args.poll_seconds, args.max_hub_storage_gb = False, 30, 7000
            hub = FakeHub()
            hub.state = lf.bootstrap(args, None, NOW)
            hub.state.update(live_next_minute="20260922120000", backfill_next_end=lf.previous(cw.FIRST),
                pending_missing_minutes=[cw.stamp(NOW.replace(second=0) - timedelta(minutes=i)) for i in range(1, 121)])
            probes, ticks = 0, 0
            def fake_collect(command):
                nonlocal probes, ticks
                probes += 1
                if probes == 100:
                    raise KeyboardInterrupt
                if probes in (50, 75):
                    ticks += 31
                archive = Path(command[command.index("--archive") + 1])
                collection_result(archive.parent.parent, command)
            with patch("latest_first_worker.datetime") as clock, \
                    patch("latest_first_worker.time.monotonic", side_effect=lambda: ticks), \
                    patch("compact_worker.run", side_effect=fake_collect), patch("compact_worker.check_space"), \
                    patch("compact_worker.check_upload_budget") as quota, \
                    patch.object(hub, "progress", wraps=hub.progress) as reads:
                clock.now.return_value = NOW
                with self.assertRaises(KeyboardInterrupt):
                    lf.run_scheduler(args, hub)
                self.assertEqual(reads.call_count, 3)
                self.assertEqual(quota.call_count, 3)
                self.assertEqual(hub.uploads, 0)

    def test_rate_limit_headers_use_longest_reset_and_safe_fallback(self):
        for headers, expected in [({"RateLimit": '"api";r=0;t=27'}, 28),
                ({"Retry-After": "60", "RateLimit": '"api";r=0;t=27'}, 61),
                ({"Retry-After": "Tue, 22 Sep 2026 12:01:31 GMT"}, 61),
                ({"Retry-After": "invalid", "RateLimit": "invalid"}, 301)]:
            with self.subTest(headers=headers):
                exc = SimpleNamespace(response=httpx.Response(429, headers=headers))
                self.assertEqual(lf.rate_limit_delay(exc, NOW), expected)
        self.assertIsNone(lf.rate_limit_delay(SimpleNamespace(response=httpx.Response(503)), NOW))

    def test_rate_limit_stops_all_hub_work_and_cooldown_survives_restart(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            args.once, args.poll_seconds, args.max_hub_storage_gb = False, 30, 7000
            hub = FakeHub()
            error = RuntimeError("Too many requests")
            error.response = httpx.Response(429, headers={"RateLimit": '"api";r=0;t=120'})
            with patch("latest_first_worker.datetime") as clock, \
                    patch("latest_first_worker.time.sleep", side_effect=KeyboardInterrupt) as sleep, \
                    patch.object(hub, "progress", side_effect=error) as reads, \
                    patch("compact_worker.build_batch") as build:
                clock.now.return_value = NOW
                with self.assertRaises(KeyboardInterrupt):
                    lf.run_scheduler(args, hub)
                self.assertEqual(reads.call_count, 1)
                build.assert_not_called()
                saved = cw.read_json(args.state / "scheduler-retries.json")
                self.assertEqual(saved["hub"]["next_check"], NOW.timestamp() + 121)
                self.assertEqual(cw.read_json(args.state / "status.json")["http_status"], 429)
                self.assertEqual(sleep.call_args.args, (30,))
                reads.reset_mock()
                with self.assertRaises(KeyboardInterrupt):
                    lf.run_scheduler(args, hub)
                reads.assert_not_called()

    def test_bootstrap_preserves_old_rows_and_defines_disjoint_seam(self):
        args = settings(Path("unused"))
        old = {"pipeline": cw.PIPELINE, "initial_start": cw.FIRST,
               "next_minute": "20200102000000", "last_end": "20200101235900",
               "total_observations": 500, "pending_missing_minutes": []}
        initial = lf.bootstrap(args, old, NOW)
        self.assertEqual(initial["backfill_floor"], old["next_minute"])
        self.assertEqual(initial["legacy_last_end"], old["last_end"])
        self.assertEqual(initial["live_start"], "20260922114500")
        self.assertEqual(cw.successor(initial["backfill_next_end"]), initial["live_start"])
        self.assertEqual(initial["total_observations"], 500)
        self.assertNotIn("schedule", old)

    def test_live_wins_then_backward_chunks_descend_to_exact_floor(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            args.collect_window_minutes = 3
            progress = lf.bootstrap(args, None, NOW)
            progress.update(backfill_floor="20260922113800", live_next_minute="20260922120000")
            plan = lf.choose_work(args, progress, {}, {}, NOW, None)
            self.assertEqual(plan, ("20260922114200", "20260922114400", "backfill"))
            hub = FakeHub()
            hub.state = progress
            covered = []
            while plan:
                start, end, kind = plan
                batch, record = item(args.state, *plan)
                minute = start
                while minute <= end:
                    covered.append(minute)
                    minute = cw.successor(minute)
                progress, _ = lf.publish(hub, batch, record, cw.FIRST, progress, now=NOW)
                cw.remove_owned(batch, args.state / kind)
                plan = lf.choose_work(args, progress, {}, {}, NOW, kind)
            self.assertEqual(len(covered), 7)
            self.assertEqual(len(set(covered)), 7)
            self.assertEqual(min(covered), "20260922113800")
            self.assertEqual(max(covered), "20260922114400")
            self.assertEqual(progress["backfill_next_end"], "20260922113700")
            later = NOW + timedelta(minutes=1)
            self.assertEqual(lf.choose_work(args, progress, {}, {}, later, "backfill"),
                             ("20260922120000", "20260922120000", "live"))

    def test_failed_historical_lane_does_not_block_new_live_minutes(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            remote = lf.bootstrap(args, None, NOW)
            retries = {"backfill": {"next_check": NOW.timestamp() + 900}}
            self.assertEqual(lf.choose_work(args, remote, {}, retries, NOW, "backfill")[2], "live")
            remote["live_next_minute"] = "20260922120000"
            self.assertIsNone(lf.choose_work(args, remote, {}, retries, NOW, "live"))
            retries = {"live": {"next_check": NOW.timestamp() + 900}}
            self.assertEqual(lf.choose_work(args, remote, {}, retries, NOW, "live")[2], "backfill")

    def test_repair_fairness_and_saved_pending_request_survive_restart(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            remote = lf.bootstrap(args, None, NOW)
            remote.update(live_next_minute="20260922120000", pending_missing_minutes=["20260922115000"])
            self.assertEqual(lf.choose_work(args, remote, {}, {}, NOW, "backfill")[2], "repair")
            self.assertEqual(lf.choose_work(args, remote, {}, {}, NOW, "repair", repair_streak=4)[2], "backfill")
            plan = lf.plan_for_lane(args, remote, {}, NOW, "backfill")
            cw.write_json(args.state / "backfill/batch/request.json", {
                "pipeline": cw.PIPELINE, "kind": "backfill", "start": plan[0], "maximum_end": plan[1]})
            args.collect_window_minutes = 1
            self.assertEqual(lf.choose_work(args, remote, {}, {}, NOW, "repair", repair_streak=4), plan)

    def test_delayed_live_files_take_priority_over_older_historical_retry_queue(self):
        args = settings(Path("unused"))
        remote = lf.bootstrap(args, None, NOW)
        old, recent = "20260922000100", "20260922115000"
        remote["pending_missing_minutes"] = [old, recent]
        schedule = {old: {"next_check": 0}, recent: {"next_check": NOW.timestamp() - 1}}
        self.assertEqual(lf.plan_for_lane(args, remote, schedule, NOW, "repair"), (recent, recent, "repair"))
        schedule[recent]["next_check"] = NOW.timestamp() + 30
        self.assertEqual(lf.plan_for_lane(args, remote, schedule, NOW, "repair"), (old, old, "repair"))

    def test_recent_recovery_burst_is_bounded_and_live_always_preempts_it(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            remote = lf.bootstrap(args, None, NOW)
            remote.update(live_next_minute="20260922120000", pending_missing_minutes=["20260922115000"])
            for streak in range(4):
                self.assertEqual(lf.choose_work(args, remote, {}, {}, NOW, "repair", streak)[2], "repair")
            # A new-live interruption does not reset the historical fairness budget.
            self.assertEqual(lf.choose_work(args, remote, {}, {}, NOW, "live", 4)[2], "backfill")
            remote["live_next_minute"] = "20260922115900"
            self.assertEqual(lf.choose_work(args, remote, {}, {}, NOW, "repair", 4)[2], "live")
            remote.update(live_next_minute="20260922120000", pending_missing_minutes=["20260922000100"])
            self.assertEqual(lf.choose_work(args, remote, {}, {}, NOW, "repair", 1)[2], "backfill")

    def test_response_loss_then_other_lane_commit_never_duplicates(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            initial = lf.bootstrap(args, None, NOW)
            hub = FakeHub()
            live_batch, live = item(root, *lf.plan_for_lane(args, initial, {}, NOW, "live"))
            hub.fail_response = True
            with self.assertRaises(ConnectionError):
                lf.publish(hub, live_batch, live, cw.FIRST, initial, now=NOW)
            back_batch, back = item(root, *lf.plan_for_lane(args, hub.state, {}, NOW, "backfill"))
            lf.publish(hub, back_batch, back, cw.FIRST, initial, now=NOW)
            progress, _ = lf.publish(hub, live_batch, live, cw.FIRST, initial, now=NOW)
            self.assertEqual(hub.uploads, 2)
            self.assertEqual(progress["total_observations"], 14)
            self.assertEqual(progress["backfill_next_end"], lf.previous(back["start"]))
            self.assertEqual(progress["live_next_minute"], live["next_minute"])

    def test_overlap_gap_stale_boundary_and_unknown_lane_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            initial = lf.bootstrap(args, None, NOW)
            hub = FakeHub()
            hub.state = initial
            for plan in [("20260922114400", "20260922114500", "backfill"),
                         ("20260922114000", "20260922114300", "backfill"),
                         ("20260922114600", "20260922114900", "live"),
                         (cw.FIRST, cw.FIRST, "other")]:
                batch, record = item(root, *plan)
                with self.subTest(plan=plan), self.assertRaises(RuntimeError):
                    lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)
            hub.state = {"pipeline": cw.PIPELINE, "initial_start": cw.FIRST,
                         "next_minute": cw.successor(cw.FIRST)}
            batch, record = item(root, *lf.plan_for_lane(args, initial, {}, NOW, "live"))
            with self.assertRaisesRegex(RuntimeError, "boundary"):
                lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)
            self.assertEqual(hub.uploads, 0)

    def test_recent_missing_repaired_without_moving_either_cursor(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            initial = lf.bootstrap(settings(root), None, NOW)
            minute = initial["live_start"]
            hub = FakeHub()
            batch, record = item(root, minute, minute, "live", missing=[minute])
            progress, _ = lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)
            cursors = progress["live_next_minute"], progress["backfill_next_end"]
            batch, record = item(root, minute, minute, "repair")
            progress, _ = lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)
            self.assertEqual((progress["live_next_minute"], progress["backfill_next_end"]), cursors)
            self.assertEqual(progress["pending_missing_minutes"], [])
            self.assertEqual(progress["total_observations"], 14)

    def test_corruption_and_failed_verification_preserve_pending_work(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            initial = lf.bootstrap(settings(root), None, NOW)
            batch, record = item(root, initial["live_start"], initial["live_start"], "live")
            hub = FakeHub()
            hub.fail_verify = True
            with self.assertRaisesRegex(RuntimeError, "verification"):
                lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)
            self.assertTrue((batch / "observations.parquet").exists())
            hub.fail_verify = False
            lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)
            self.assertEqual(hub.uploads, 1)
            (batch / "observations.parquet").write_bytes(b"corrupt")
            with self.assertRaisesRegex(RuntimeError, "checksum"):
                lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)

    def test_empty_backfill_and_first_minute_completion_sentinel(self):
        args = settings(Path("unused"))
        remote = {"pipeline": cw.PIPELINE, "initial_start": cw.FIRST,
                  "next_minute": "20260922120000", "last_end": "20260922115900"}
        initial = lf.bootstrap(args, remote, NOW)
        self.assertIsNone(lf.plan_for_lane(args, initial, {}, NOW, "backfill"))
        initial.update(backfill_floor=cw.FIRST, backfill_next_end=cw.FIRST)
        self.assertEqual(lf.plan_for_lane(args, initial, {}, NOW, "backfill"),
                         (cw.FIRST, cw.FIRST, "backfill"))
        initial["backfill_next_end"] = lf.previous(cw.FIRST)
        self.assertIsNone(lf.plan_for_lane(args, initial, {}, NOW, "backfill"))

    def test_forward_worker_refuses_to_erase_latest_first_cursors(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            initial = lf.bootstrap(settings(root), None, NOW)
            batch, record = item(root, initial["live_start"], initial["live_start"], "live")
            hub = FakeHub()
            hub.state = initial
            with self.assertRaisesRegex(RuntimeError, "latest-first"):
                cw.publish(hub, batch, record, cw.FIRST)

    def test_inflight_repair_survives_expiration_while_other_lane_publishes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            initial = lf.bootstrap(settings(root), None, NOW)
            minute = initial["live_start"]
            hub = FakeHub()
            batch, record = item(root, minute, minute, "live", missing=[minute])
            lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW)
            batch, record = item(root, cw.successor(minute), cw.successor(minute), "live")
            progress, _ = lf.publish(hub, batch, record, cw.FIRST, initial,
                now=NOW + timedelta(days=2), protected_missing=[minute])
            self.assertIn(minute, progress["pending_missing_minutes"])
            batch, record = item(root, minute, minute, "repair")
            progress, _ = lf.publish(hub, batch, record, cw.FIRST, initial, now=NOW + timedelta(days=2))
            self.assertEqual(progress["pending_missing_minutes"], [])

    def test_scheduler_publishes_live_then_backward_then_new_live_across_restarts(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            args.once = True
            args.max_hub_storage_gb = 7000
            hub = FakeHub()
            def fake_collect(command):
                archive = Path(command[command.index("--archive") + 1])
                collection_result(archive.parent.parent, command)
            with patch("latest_first_worker.datetime") as clock, patch("compact_worker.run", side_effect=fake_collect), \
                    patch("compact_worker.check_space"), patch("compact_worker.check_upload_budget"), \
                    patch.object(hub, "commit", wraps=hub.commit) as commit:
                clock.now.return_value = NOW
                lf.run_scheduler(args, hub)
                self.assertEqual(hub.state["last_action"], "live")
                self.assertEqual(hub.state["live_next_minute"], "20260922120000")
                self.assertFalse((args.state / "live/batch").exists())
                # Missing live files are deferred, letting historical work run.
                lf.run_scheduler(args, hub)
                self.assertEqual(hub.state["last_action"], "backfill")
                end = hub.state["backfill_next_end"]
                self.assertEqual(end, "20260922112900")
                self.assertFalse((args.state / "backfill/batch").exists())
                clock.now.return_value = NOW + timedelta(minutes=1)
                lf.run_scheduler(args, hub)
                self.assertEqual(hub.state["last_action"], "live")
                self.assertEqual(hub.state["backfill_next_end"], end)
                self.assertEqual(hub.state["live_next_minute"], "20260922120100")
                self.assertEqual(hub.uploads, 3)
                for call in commit.call_args_list:
                    record = call.args[1]
                    self.assertEqual(record["observations"], 0)
                    self.assertEqual(record["files"], [])
                    self.assertIsNone(record["receipt"]["path"])
                self.assertEqual(hub.state["total_observations"], 0)
                self.assertIn("20260922120000", hub.state["pending_missing_minutes"])

    def test_empty_commit_response_loss_preserves_retry_queue_without_duplicate_upload(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            initial = lf.bootstrap(args, None, NOW)
            args.state = args.state / "live"
            start = initial["live_start"]
            def fake_collect(command):
                collection_result(args.state, command)
            with patch("compact_worker.run", side_effect=fake_collect), patch("compact_worker.check_space"):
                record = cw.build_batch(args, start, start, "live")
            hub = FakeHub()
            hub.fail_response = True
            with self.assertRaises(ConnectionError):
                lf.publish(hub, args.state / "batch", record, cw.FIRST, initial, now=NOW)
            back_batch, back = item(args.state.parent, *lf.plan_for_lane(args, hub.state, {}, NOW, "backfill"))
            lf.publish(hub, back_batch, back, cw.FIRST, initial, now=NOW)
            progress, _ = lf.publish(hub, args.state / "batch", record, cw.FIRST, initial, now=NOW)
            self.assertEqual(hub.uploads, 2)
            self.assertEqual(progress["live_next_minute"], cw.successor(start))
            self.assertEqual(progress["pending_missing_minutes"], [start])
            self.assertEqual(progress["total_observations"], 7)
            self.assertIsNone(progress["lane_commits"]["live"]["path"])
            self.assertEqual(progress["last_manifest"], back["files"][-1]["path"])

    def test_old_pending_empty_manifest_is_not_uploaded_and_can_recover_old_ack(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            initial = lf.bootstrap(args, None, NOW)
            batch, old = item(root, initial["live_start"], initial["live_start"], "live")
            old.update(observations=0, files=[old["files"][-1]])
            hub = FakeHub()
            with patch.object(hub, "commit", wraps=hub.commit) as commit:
                progress, _ = lf.publish(hub, batch, old, cw.FIRST, initial, now=NOW)
                self.assertEqual(commit.call_args.args[1]["files"], [])
                self.assertIsNone(progress["lane_commits"]["live"]["path"])
            lf.publish(hub, batch, old, cw.FIRST, initial, now=NOW)
            self.assertEqual(hub.uploads, 1)
            # Also accept a commit made by the previous deployed version before
            # upgrading; do not repeat it or discard its unverified receipt.
            hub.state["lane_commits"]["live"]["path"] = old["files"][-1]["path"]
            lf.publish(hub, batch, old, cw.FIRST, initial, now=NOW)
            self.assertEqual(hub.uploads, 1)

    def test_corrupt_local_empty_receipt_is_not_published(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            initial = lf.bootstrap(args, None, NOW)
            with patch("compact_worker.run", side_effect=lambda cmd: collection_result(args.state, cmd)), \
                    patch("compact_worker.check_space"):
                record = cw.build_batch(args, initial["live_start"], initial["live_start"], "live")
            (args.state / "batch/manifest.json").write_bytes(b"corrupted")
            hub = FakeHub()
            with self.assertRaisesRegex(RuntimeError, "checksum"):
                lf.publish(hub, args.state / "batch", record, cw.FIRST, initial, now=NOW)
            self.assertEqual(hub.uploads, 0)

    def test_polling_without_a_new_minute_does_not_repeat_forward_collection(self):
        args = settings(Path("unused"))
        progress = lf.bootstrap(args, None, NOW)
        progress["live_next_minute"] = "20260922120000"
        self.assertIsNone(lf.plan_for_lane(args, progress, {}, NOW, "live"))
        self.assertIsNone(lf.plan_for_lane(args, progress, {}, NOW + timedelta(seconds=20), "live"))
        self.assertEqual(lf.plan_for_lane(args, progress, {}, NOW + timedelta(seconds=30), "live"),
                         ("20260922120000", "20260922120000", "live"))


if __name__ == "__main__":
    unittest.main()
