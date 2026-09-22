from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import compact_worker as cw
from migrate_compact import convert_parquet
from upgrade_native_metadata import verify_preserved
from test_worker import FakeHub, publication


def settings(root):
    return SimpleNamespace(state=root, collector="collector", exporter="exporter", threads=1,
                           min_free_gib=12, shard_gib=0.25, max_expanded_mib=2048,
                           download_workers=4, collect_window_minutes=15,
                           max_download_mib=512, max_fragments=8_000_000,
                           start=cw.FIRST, lag_minutes=1, batch_minutes=360, retry_missing_hours=24)


def record(batch):
    result = publication(batch)
    result.update(kind="forward", pipeline=cw.PIPELINE, missing_minutes=[])
    return result


def collection_result(root, command, statuses=None):
    start = command[command.index("--start") + 1]
    end = command[command.index("--end") + 1]
    latest = root / "batch/archive/runs/current"
    minute, complete, missing = start, 0, 0
    while minute <= end:
        status = (statuses or {}).get(minute, "missing")
        complete += status == "complete"
        missing += status == "missing"
        cw.write_json(latest / (minute + ".json"), {
            "status": status, "minute": cw.parse_minute(minute).isoformat()})
        minute = cw.successor(minute)
    cw.write_json(latest / "summary.json", {"completed": complete, "missing": missing, "failed": 0})
    cw.write_json(latest / "request.json", {"profile": "current", "start": cw.parse_minute(start).isoformat(),
                                           "end": cw.parse_minute(end).isoformat()})
    return latest


class CompactTests(unittest.TestCase):
    def test_migration_keeps_every_row_and_exact_selected_contents(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            original = cw.pa.Table.from_pylist([
                {"source_minute": cw.FIRST, "raw_sha256": "a" * 64, "type": 2, "observed_at": "2020-01-01T00:01:00Z", "lang": "ja", "url": "https://example.jp/記事", "text": "日本語\n🙂", "metadata_json": "{}"},
                {"source_minute": cw.FIRST, "raw_sha256": "a" * 64, "type": 1, "observed_at": "2020-01-01T00:01:00Z", "lang": "en", "url": "https://example.org", "text": "Same URL observations stay separate", "metadata_json": "{}"}])
            pq.write_table(original, root / "old.parquet")
            count = convert_parquet(root / "old.parquet", root / "new.parquet")
            self.assertEqual(count, 2)
            converted = pq.read_table(root / "new.parquet")
            self.assertTrue(converted.select(cw.BASE_COLUMNS).equals(original.select(["observed_at", "lang", "url", "text"]).rename_columns(cw.BASE_COLUMNS)))
            self.assertNotIn("country", converted["metadata"].to_pylist()[0])

    def test_projection_preserves_dates_and_multiscript_text_exactly(self):
        row = {"observed_at": "2020-01-01T00:01:00.000Z", "lang": "zh",
               "text": "中文\n\nภาษาไทย 👩🏽‍💻", "url": "https://example.org", "metadata": {"country": "XX"},
               "type": 2, "fragments": 7, "assembly": "estimated", "bounded_fallback": True}
        projected = cw.parquet_row(row, cw.FIRST, "a" * 64)
        self.assertEqual(set(projected), {"date", "language", "source_url", "text", "observation_id", "type", "metadata"})
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "sample.parquet"
            pq.write_table(cw.pa.Table.from_pylist([projected], schema=cw.SCHEMA), path)
            actual = pq.read_table(path).to_pylist()[0]
        self.assertEqual(actual["text"], row["text"])
        self.assertEqual(actual["date"], row["observed_at"])
        self.assertEqual(actual["language"], row["lang"])
        self.assertEqual(actual["source_url"], row["url"])
        self.assertEqual(actual["type"], 2)
        self.assertEqual(actual["metadata"]["fragments"], 7)
        self.assertEqual(actual["metadata"]["raw_sha256"], "a" * 64)
        self.assertNotIn("country", actual["metadata"])

    def test_identity_stable_across_reassembly_but_distinct_for_source_groups(self):
        row = {"observed_at": "2020-01-01T00:01:00Z", "lang": "zh", "url": "https://example.org/中文", "type": 2, "text": "before", "id": 1}
        expected = cw.observation_id(row, cw.FIRST, "a" * 64)
        self.assertEqual(len(expected), 64)
        self.assertEqual(expected, cw.observation_id({**row, "id": 999, "text": "after"}, cw.FIRST, "a" * 64))
        alternatives = [cw.observation_id({**row, **change}, cw.FIRST, "a" * 64)
                        for change in ({"type": 1}, {"lang": "ja"}, {"url": "https://other.org"}, {"observed_at": "2020-01-01T00:02:00Z"})]
        alternatives += [cw.observation_id(row, cw.successor(cw.FIRST), "a" * 64),
                         cw.observation_id(row, cw.FIRST, "b" * 64)]
        self.assertNotIn(expected, alternatives)
        self.assertEqual(len(set(alternatives)), len(alternatives))
        for change in ({"type": 0}, {"type": True}, {"url": ""}, {"lang": None}):
            with self.assertRaises(ValueError):
                cw.observation_id({**row, **change}, cw.FIRST, "a" * 64)

    def test_upgrade_rejects_changed_text_even_with_same_row_count(self):
        with tempfile.TemporaryDirectory() as d:
            old, new = Path(d) / "old.parquet", Path(d) / "new.parquet"
            row = {"date": "2020-01-01", "language": "zh", "source_url": "https://example.org", "text": "中文"}
            pq.write_table(cw.pa.Table.from_pylist([row]), old)
            pq.write_table(cw.pa.Table.from_pylist([{**row, "text": "changed"}]), new)
            with self.assertRaisesRegex(RuntimeError, "changed existing"):
                verify_preserved(old, new)

    def test_near_live_uses_last_complete_minute_and_one_minute_shards(self):
        now = datetime(2026, 9, 22, 12, 0, 31, tzinfo=timezone.utc)
        args = settings(Path("unused"))
        remote = {"next_minute": "20260922115900"}
        self.assertEqual(cw.next_plan(args, remote, {}, now), ("20260922115900", "20260922115900", "forward"))
        remote["next_minute"] = "20260922120000"
        self.assertIsNone(cw.next_plan(args, remote, {}, now))

    def test_historical_work_still_batches_efficiently(self):
        args = settings(Path("unused"))
        remote = {"next_minute": "20200101000100"}
        now = datetime(2026, 9, 22, tzinfo=timezone.utc)
        self.assertEqual(cw.next_plan(args, remote, {}, now), (cw.FIRST, "20200101060000", "forward"))

    def test_late_404_backoff_does_not_block_new_files_and_expires(self):
        now = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
        args = settings(Path("unused"))
        minute = "20260922115000"
        remote = {"next_minute": "20260922115900", "pending_missing_minutes": [minute, cw.FIRST]}
        self.assertEqual(cw.next_plan(args, remote, {}, now), (minute, minute, "repair"))
        schedule = {}
        cw.defer_missing(schedule, minute, now)
        self.assertEqual(cw.next_plan(args, remote, schedule, now)[2], "forward")
        remote["last_action"] = "repair"
        self.assertEqual(cw.next_plan(args, remote, {}, now)[2], "forward")
        self.assertEqual(cw.pending_within_window(remote, now + timedelta(days=2), 24), [])

    def test_forward_commit_loss_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            batch = Path(d) / "batch"
            item, hub = record(batch), FakeHub()
            hub.fail_response = True
            with self.assertRaises(ConnectionError):
                cw.publish(hub, batch, item, cw.FIRST)
            progress, _ = cw.publish(hub, batch, item, cw.FIRST)
            self.assertEqual(hub.uploads, 1)
            self.assertEqual(progress["total_observations"], 7)

    def test_repaired_minute_does_not_rewind_cursor_or_duplicate_after_response_loss(self):
        with tempfile.TemporaryDirectory() as d:
            batch = Path(d) / "batch"
            item, hub = record(batch), FakeHub()
            item["kind"] = "repair"
            hub.state = {"pipeline": cw.PIPELINE, "initial_start": cw.FIRST,
                         "next_minute": "20200101001000", "last_end": "20200101000900",
                         "pending_missing_minutes": [cw.FIRST], "total_observations": 100}
            hub.fail_response = True
            with self.assertRaises(ConnectionError):
                cw.publish(hub, batch, item, cw.FIRST)
            progress, _ = cw.publish(hub, batch, item, cw.FIRST)
            self.assertEqual(progress["next_minute"], "20200101001000")
            self.assertEqual(progress["total_observations"], 107)
            self.assertEqual(progress["pending_missing_minutes"], [])
            self.assertEqual(hub.uploads, 1)

    def test_recent_missing_minute_is_durable_but_old_missing_is_not_queued(self):
        with tempfile.TemporaryDirectory() as d:
            batch = Path(d) / "batch"
            item, hub = record(batch), FakeHub()
            item["missing_minutes"] = [cw.FIRST]
            now = cw.parse_minute(cw.FIRST) + timedelta(minutes=2)
            progress, _ = cw.publish(hub, batch, item, cw.FIRST, now=now)
            self.assertEqual(progress["pending_missing_minutes"], [cw.FIRST])
            other = FakeHub()
            progress, _ = cw.publish(other, batch, item, cw.FIRST, now=now + timedelta(days=2))
            self.assertEqual(progress["pending_missing_minutes"], [])

    def test_corrupt_or_conflicting_batch_never_uploads(self):
        with tempfile.TemporaryDirectory() as d:
            batch = Path(d) / "batch"
            item, hub = record(batch), FakeHub()
            hub.state = {"pipeline": cw.PIPELINE, "initial_start": cw.FIRST, "next_minute": "20200101002000"}
            with self.assertRaisesRegex(RuntimeError, "conflicting checkpoint"):
                cw.publish(hub, batch, item, cw.FIRST)
            (batch / "data").write_bytes(b"changed")
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                cw.publish(hub, batch, item, cw.FIRST)
            self.assertEqual(hub.uploads, 0)

    def test_storage_limit_accounts_for_incoming_batch_and_never_deletes(self):
        hub = SimpleNamespace(api=SimpleNamespace(dataset_info=lambda *a, **k: SimpleNamespace(used_storage=6_999_999_999_900)), repo="owner/data")
        with self.assertRaisesRegex(RuntimeError, "storage guard"):
            cw.check_upload_budget(hub, [{"bytes": 100}], 7000)
        hub.api.dataset_info = lambda *a, **k: SimpleNamespace(used_storage=None)
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            cw.check_upload_budget(hub, [], 7000)
        hub.api.dataset_info = lambda *a, **k: SimpleNamespace(used_storage=10)
        self.assertEqual(cw.check_upload_budget(hub, [{"bytes": 100}], 7000), 10)

    def test_missing_batch_has_local_receipt_but_no_public_files(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            def fake_run(command):
                collection_result(root, command)
            with patch("compact_worker.run", side_effect=fake_run), patch("compact_worker.check_space"):
                item = cw.build_batch(args, cw.FIRST, cw.FIRST)
            self.assertEqual(item["missing_minutes"], [cw.FIRST])
            self.assertEqual(item["files"], [])
            self.assertIsNone(item["receipt"]["path"])
            self.assertEqual(cw.read_json(root / "batch/manifest.json")["files"], [])
            self.assertEqual(pq.read_table(root / "batch/observations.parquet").schema, cw.SCHEMA)
            self.assertFalse((root / "batch/evidence.tar").exists())
            with patch("compact_worker.run", side_effect=AssertionError("No recollection")):
                self.assertEqual(cw.build_batch(args, cw.FIRST, cw.FIRST), item)

    def test_valid_zero_row_source_keeps_provenance_without_uploading_empty_data(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            def fake_run(command):
                self.assertNotEqual(command[0], "exporter")
                archive = root / "batch/archive"
                collection_result(root, command, {cw.FIRST: "complete"})
                raw = archive / "raw/2020/01/01" / (cw.FIRST + ".webngrams.json.gz")
                raw.parent.mkdir(parents=True)
                raw.write_bytes(gzip.compress(b"quarantined input"))
                articles = archive / "articles/current/articles.gz"
                articles.parent.mkdir(parents=True)
                articles.write_bytes(gzip.compress(b""))
                cw.write_json(articles.parent / (cw.digest(raw) + ".manifest.json"), {
                    "profile": "current", "raw": {"sha256": cw.digest(raw)},
                    "output": str(articles.relative_to(archive)), "output_sha256": cw.digest(articles),
                    "counts": {"articles": 0, "type1_articles": 0, "type2_articles": 0,
                               "quarantined_metadata_records": 3}})
            with patch("compact_worker.run", side_effect=fake_run), patch("compact_worker.check_space"):
                result = cw.build_batch(args, cw.FIRST, cw.FIRST)
            self.assertEqual(result["observations"], 0)
            self.assertEqual(result["missing_minutes"], [])
            self.assertEqual(result["quarantined_metadata_records"], 3)
            self.assertEqual(result["minutes"][0]["status"], "complete")
            self.assertIn("raw_sha256", result["minutes"][0])
            self.assertEqual(result["files"], [])
            self.assertIsNone(result["receipt"]["path"])

    def test_source_failure_never_seals_a_batch(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            with patch("compact_worker.run", side_effect=RuntimeError("HTTP 503")), patch("compact_worker.check_space"):
                with self.assertRaisesRegex(RuntimeError, "503"):
                    cw.build_batch(args, cw.FIRST, cw.FIRST)
            self.assertFalse((args.state / "batch/ready.json").exists())

    def test_forward_checkpoint_only_commit_recovers_without_new_files(self):
        with tempfile.TemporaryDirectory() as d:
            args = settings(Path(d))
            with patch("compact_worker.run", side_effect=lambda cmd: collection_result(args.state, cmd)), \
                    patch("compact_worker.check_space"):
                record = cw.build_batch(args, cw.FIRST, cw.FIRST)
            hub = FakeHub()
            hub.fail_response = True
            now = cw.parse_minute(cw.FIRST)
            with self.assertRaises(ConnectionError):
                cw.publish(hub, args.state / "batch", record, cw.FIRST, now=now)
            progress, _ = cw.publish(hub, args.state / "batch", record, cw.FIRST, now=now)
            self.assertEqual(hub.uploads, 1)
            self.assertEqual(progress["next_minute"], cw.successor(cw.FIRST))
            self.assertEqual(progress["pending_missing_minutes"], [cw.FIRST])
            self.assertIsNone(progress["last_manifest"])
            self.assertEqual(progress["published_bytes"], 0)

    def test_valid_reconstruction_uses_no_metadata_services(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            raw_sha = None
            def fake_run(command):
                nonlocal raw_sha
                if command[0] == "exporter":
                    output = Path(command[command.index("--output-directory") + 1])
                    output.mkdir()
                    header = {"meta": {"raw_sha256": raw_sha, "profile": "current", "observations": 1}}
                    row = {"type": 2, "observed_at": "2020-01-01T00:01:00Z", "lang": "zh", "url": "https://example.org/article", "text": "中文 news"}
                    observations = output / "observations.jsonl"
                    observations.write_text(json.dumps(header) + "\n" + json.dumps(row) + "\n")
                    cw.write_json(output / "export-report.json", {"files": [{"name": observations.name, "sha256": cw.digest(observations)}],
                                   "estimated_observations": 0, "bounded_fallback_observations": 0})
                    return
                archive = root / "batch/archive"
                collection_result(root, command, {cw.FIRST: "complete"})
                raw = archive / "raw/2020/01/01" / (cw.FIRST + ".webngrams.json.gz")
                raw.parent.mkdir(parents=True)
                raw.write_bytes(gzip.compress(b"raw source"))
                raw_sha = cw.digest(raw)
                articles = archive / "articles/current/articles.gz"
                articles.parent.mkdir(parents=True)
                articles.write_bytes(gzip.compress(b"reconstructed"))
                cw.write_json(articles.parent / (raw_sha + ".manifest.json"), {
                    "profile": "current", "raw": {"sha256": raw_sha}, "output": str(articles.relative_to(archive)),
                    "output_sha256": cw.digest(articles), "counts": {"articles": 1, "type1_articles": 0, "type2_articles": 1,
                    "quarantined_metadata_records": 2}})
            with patch("compact_worker.run", side_effect=fake_run), patch("compact_worker.check_space"), patch("enrich_metadata.build", side_effect=AssertionError("No enrichment")):
                item = cw.build_batch(args, cw.FIRST, cw.FIRST)
            self.assertEqual(item["observations"], 1)
            self.assertEqual(item["quarantined_metadata_records"], 2)
            table = pq.read_table(root / "batch/observations.parquet")
            self.assertEqual(table.select(cw.BASE_COLUMNS).to_pylist(), [{"date": "2020-01-01T00:01:00Z", "language": "zh", "source_url": "https://example.org/article", "text": "中文 news"}])
            self.assertEqual(table["metadata"].to_pylist()[0]["source_minute"], cw.FIRST)
            self.assertEqual(table["metadata"].to_pylist()[0]["raw_sha256"], raw_sha)

    def test_windows_resolve_every_minute_in_order_with_bounded_downloads(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            args.collect_window_minutes = 3
            args.max_download_mib = 1024
            calls = []
            def fake_run(command):
                calls.append(command)
                collection_result(root, command)
            end = "20200101000800"
            with patch("compact_worker.run", side_effect=fake_run), patch("compact_worker.check_space"):
                item = cw.build_batch(args, cw.FIRST, end)
            self.assertEqual(len(calls), 3)
            self.assertEqual([c[c.index("--downloads") + 1] for c in calls], [3, 3, 2])
            self.assertEqual([c[c.index("--min-free-gib") + 1] for c in calls], [14, 14, 13])
            self.assertEqual([x["minute"] for x in item["minutes"]], [f"20200101000{i}00" for i in range(1, 9)])
            self.assertEqual(item["next_minute"], "20200101000900")

    def test_collector_window_rejects_inconsistent_summary_or_identity(self):
        for defect in ("summary", "identity", "missing_outcome", "request"):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as d:
                root = Path(d)
                args = settings(root)
                def fake_run(command):
                    latest = collection_result(root, command)
                    if defect == "summary":
                        cw.write_json(latest / "summary.json", {"completed": 1, "missing": 0, "failed": 0})
                    elif defect == "identity":
                        cw.write_json(latest / (cw.FIRST + ".json"), {"status": "missing", "minute": "2020-01-01T00:02:00Z"})
                    elif defect == "request":
                        cw.write_json(latest / "request.json", {"profile": "current", "start": "2020-01-01T00:02:00Z", "end": "2020-01-01T00:02:00Z"})
                    else:
                        (latest / (cw.FIRST + ".json")).unlink()
                with patch("compact_worker.run", side_effect=fake_run), patch("compact_worker.check_space"):
                    with self.assertRaises((RuntimeError, FileNotFoundError)):
                        cw.build_batch(args, cw.FIRST, cw.FIRST)
                self.assertFalse((root / "batch/ready.json").exists())

    def test_size_limit_seals_only_after_all_prefetched_minutes_are_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            args = settings(root)
            args.collect_window_minutes = 3
            args.shard_gib = 0
            with patch("compact_worker.run", side_effect=lambda command: collection_result(root, command)), patch("compact_worker.check_space"):
                item = cw.build_batch(args, cw.FIRST, "20200101000800")
            self.assertEqual(item["end"], "20200101000300")
            self.assertEqual(len(item["minutes"]), 3)


if __name__ == "__main__":
    unittest.main()
