import copy
import gzip
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from unittest.mock import patch

import pyarrow.parquet as pq
import worker


class FakeHub:
    def __init__(self):
        self.state = None
        self.revision = "initial"
        self.uploads = 0
        self.fail_response = False
        self.fail_verify = False

    def progress(self):
        return self.state, self.revision

    def commit(self, batch, record, progress, parent):
        if parent != self.revision:
            raise RuntimeError("Concurrent commit")
        self.uploads += 1
        self.state = copy.deepcopy(progress)
        self.revision = "uploaded"
        if self.fail_response:
            self.fail_response = False
            raise ConnectionError("Response lost after successful commit")
        return self.revision

    def verify(self, files, revision):
        if self.fail_verify:
            raise RuntimeError("Remote verification failed")


def publication(batch):
    batch.mkdir()
    p = batch / "data"
    p.write_bytes(b"source evidence")
    m = batch / "manifest.json"
    worker.write_json(m, {"observations": 7})
    return {"start": worker.FIRST, "end": worker.FIRST,
            "next_minute": worker.successor(worker.FIRST), "observations": 7,
            "files": [{**worker.file_record(p, "data/shard"), "local": p.name},
                      {**worker.file_record(m, "manifests/shard.json"), "local": m.name}]}


class WorkerTests(unittest.TestCase):
    def test_checkpoint_only_commit_uploads_no_data_or_manifest_and_verifies_checkpoint(self):
        with tempfile.TemporaryDirectory() as d:
            batch = Path(d)
            hub = worker.Hub.__new__(worker.Hub)
            hub.repo = "owner/data"
            hub.api = SimpleNamespace(create_commit=Mock(return_value=SimpleNamespace(oid="new")))
            hub.verify = Mock()
            record = {"start": worker.FIRST, "end": worker.FIRST, "files": []}
            progress = {"next_minute": worker.successor(worker.FIRST), "pending_missing_minutes": [worker.FIRST]}
            self.assertEqual(hub.commit(batch, record, progress, "parent"), "new")
            uploaded = hub.api.create_commit.call_args.kwargs
            self.assertEqual(uploaded["parent_commit"], "parent")
            self.assertEqual([op.path_in_repo for op in uploaded["operations"]], ["progress.json", "README.md"])
            self.assertEqual(worker.read_json(batch / "progress.json"), progress)
            hub.verify.assert_called_once_with([worker.file_record(batch / "progress.json", "progress.json")], "new")

    def test_no_artifacts_to_verify_does_not_make_an_empty_paths_api_call(self):
        hub = worker.Hub.__new__(worker.Hub)
        hub.api = SimpleNamespace(get_paths_info=Mock(side_effect=AssertionError("No empty API request")))
        hub.verify([], "revision")
        hub.api.get_paths_info.assert_not_called()

    def test_lost_commit_response_recovers_without_duplicate_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = Path(directory) / "batch"
            record = publication(batch)
            record["quarantined_metadata_records"] = 3
            hub = FakeHub()
            hub.fail_response = True
            with self.assertRaises(ConnectionError):
                worker.publish(hub, batch, record, worker.FIRST)
            self.assertTrue((batch / "data").exists())
            progress, revision = worker.publish(hub, batch, record, worker.FIRST)
            self.assertEqual(hub.uploads, 1)
            self.assertEqual(progress["total_observations"], 7)
            self.assertEqual(progress["total_quarantined_metadata_records"], 3)
            self.assertEqual(revision, "uploaded")

    def test_verification_failure_keeps_pending_data(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = Path(directory) / "batch"
            record, hub = publication(batch), FakeHub()
            hub.fail_verify = True
            with self.assertRaisesRegex(RuntimeError, "verification failed"):
                worker.publish(hub, batch, record, worker.FIRST)
            self.assertTrue((batch / "data").exists())
            hub.fail_verify = False
            worker.publish(hub, batch, record, worker.FIRST)
            self.assertEqual(hub.uploads, 1)

    def test_local_corruption_fails_before_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = Path(directory) / "batch"
            record, hub = publication(batch), FakeHub()
            (batch / "data").write_bytes(b"corrupt")
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                worker.publish(hub, batch, record, worker.FIRST)
            self.assertEqual(hub.uploads, 0)

    def test_conflicting_remote_progress_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = Path(directory) / "batch"
            record, hub = publication(batch), FakeHub()
            hub.state = {"pipeline": worker.PIPELINE, "next_minute": "20200101000500"}
            with self.assertRaisesRegex(RuntimeError, "conflicting checkpoint"):
                worker.publish(hub, batch, record, worker.FIRST)
            self.assertEqual(hub.uploads, 0)

    def test_cleanup_cannot_escape_owned_scratch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for bad in (root, root / "important", root.parent):
                with self.assertRaises(ValueError):
                    worker.remove_owned(bad, root)
            (root / "batch").mkdir()
            (root / "batch/evidence").write_text("kept until verified")
            worker.remove_owned(root / "batch", root)
            self.assertFalse((root / "batch").exists())

    def test_parquet_preserves_multiscript_text_and_metadata(self):
        row = {"id": 1, "type_id": 1, "type": 2, "lang": "zh", "url": "https://example.com/a",
               "observed_at": "2020-01-01T00:01:00Z", "fragments": 5, "primary_fragments": 3,
               "assembly": "estimated", "position_joins": 1, "bounded_fallback": False,
               "search": "ambiguous", "text": "中文\n\nภาษาไทย 👩🏽‍💻", "metadata": {"source_country": {"iso2": None}}}
        converted = worker.parquet_row(row, worker.FIRST, "a" * 64)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.parquet"
            pq.write_table(worker.pa.Table.from_pylist([converted], schema=worker.SCHEMA), path)
            actual = pq.read_table(path).to_pylist()[0]
        self.assertEqual(actual["text"], row["text"])
        self.assertEqual(json.loads(actual["metadata_json"]), row["metadata"])
        self.assertIsNone(actual["source_country_iso2"])

    def test_missing_source_creates_explicit_empty_shard_and_resumes_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(state=root, collector="collector", exporter="exporter", threads=1,
                                   min_free_gib=12, shard_gib=4, max_expanded_mib=2048,
                                   max_download_mib=512, max_fragments=8_000_000)
            def fake_collect(command):
                run_dir = root / "batch/archive/runs/20260921"
                run_dir.mkdir(parents=True)
                worker.write_json(run_dir / "summary.json", {"completed": 0, "missing": 1, "failed": 0})
                worker.write_json(run_dir / (worker.FIRST + ".json"), {"status": "missing"})
            with patch("worker.run", side_effect=fake_collect), patch("worker.check_space"):
                result = worker.build_batch(args, worker.FIRST, worker.FIRST)
            self.assertEqual(result["missing_minutes"], [worker.FIRST])
            self.assertEqual(pq.read_table(root / "batch/observations.parquet").num_rows, 0)
            with tarfile.open(root / "batch/evidence.tar") as archive:
                self.assertIn(f"minutes/{worker.FIRST}/status.json", archive.getnames())
            with patch("worker.run", side_effect=AssertionError("No recollection after ready")):
                self.assertEqual(worker.build_batch(args, worker.FIRST, worker.FIRST), result)
            (root / "batch/observations.parquet").write_bytes(b"broken")
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                worker.build_batch(args, worker.FIRST, worker.FIRST)

    def test_source_failure_never_becomes_ready_or_advances(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(state=Path(directory), collector="collector", threads=1, min_free_gib=12,
                                   max_expanded_mib=2048, max_download_mib=512, max_fragments=8_000_000)
            with patch("worker.check_space"), patch("worker.run", side_effect=RuntimeError("HTTP 503")):
                with self.assertRaisesRegex(RuntimeError, "503"):
                    worker.build_batch(args, worker.FIRST, worker.FIRST)
            self.assertFalse((args.state / "batch/ready.json").exists())
            self.assertTrue((args.state / "batch/request.json").exists())

    def test_profile_selection_and_quarantine_evidence_survive_batch_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(state=root, collector="collector", exporter="exporter", threads=1,
                                   min_free_gib=12, shard_gib=4, max_expanded_mib=2048,
                                   max_download_mib=512, max_fragments=8_000_000)
            evidence_bytes = gzip.compress(b'{"source_line":1,"reasons":["missing_url"]}\n')
            def fake_run(command):
                if command[0] == "exporter":
                    Path(command[command.index("--output-directory") + 1]).mkdir()
                    return
                archive = root / "batch/archive"
                run_dir = archive / "runs/current"
                worker.write_json(run_dir / "request.json", {"profile": "current"})
                worker.write_json(run_dir / "summary.json", {"completed": 1, "missing": 0, "failed": 0})
                worker.write_json(run_dir / (worker.FIRST + ".json"), {"status": "complete"})
                raw = archive / "raw/2020/01/01" / (worker.FIRST + ".webngrams.json.gz")
                raw.parent.mkdir(parents=True)
                raw.write_bytes(gzip.compress(b'{"url":""}\n'))
                sha = worker.enrich.digest(raw)
                profile_dir = archive / "articles/current"
                profile_dir.mkdir(parents=True)
                articles, quarantine = profile_dir / "articles.gz", profile_dir / "quarantine.gz"
                articles.write_bytes(gzip.compress(b""))
                quarantine.write_bytes(evidence_bytes)
                worker.write_json(profile_dir / (sha + ".manifest.json"), {
                    "profile": "current", "raw": {"sha256": sha},
                    "output": str(articles.relative_to(archive)), "output_sha256": worker.enrich.digest(articles),
                    "quarantine": {"path": str(quarantine.relative_to(archive)),
                                   "sha256": worker.enrich.digest(quarantine), "records": 1}})
                worker.write_json(archive / "articles/stale" / (sha + ".manifest.json"), {"profile": "stale"})
            def fake_enrich(source, cache, output, minute):
                output.mkdir()
                observations = output / "observations.enriched.jsonl"
                observations.write_text('{"meta":{}}\n')
                worker.write_json(output / "enrichment-report.json", {
                    "source_files": [], "observations": 0,
                    "files": [{"name": observations.name, "sha256": worker.enrich.digest(observations)}]})
            with patch("worker.run", side_effect=fake_run), patch("worker.enrich.build", side_effect=fake_enrich), patch("worker.check_space"):
                record = worker.build_batch(args, worker.FIRST, worker.FIRST)
            self.assertEqual(record["observations"], 0)
            self.assertEqual(record["quarantined_metadata_records"], 1)
            with tarfile.open(root / "batch/evidence.tar") as archive:
                self.assertEqual(archive.extractfile(f"minutes/{worker.FIRST}/quarantine.jsonl.gz").read(), evidence_bytes)


if __name__ == "__main__":
    unittest.main()
