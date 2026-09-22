"""Upgrade four-column v2 data to compact native-metadata Parquet without changing text."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
from huggingface_hub import CommitOperationAdd, hf_hub_download
import compact_worker as cw
from migrate_compact import convert_parquet

PREVIOUS = "gdelt-types12-parquet-v2"


def verify_preserved(old, new):
    if not pq.read_table(old, columns=cw.BASE_COLUMNS).equals(pq.read_table(new, columns=cw.BASE_COLUMNS)):
        raise RuntimeError("Native metadata upgrade changed existing observation fields or row order")


def upgrade(hub, stage, apply=False):
    progress, parent = hub.progress()
    if progress.get("pipeline") == cw.PIPELINE:
        plan = stage / "plan.json"
        if plan.exists():
            hub.verify(cw.read_json(plan)["files"], parent)
        print(json.dumps({"already_upgraded": True, "revision": parent}))
        return
    if progress.get("pipeline") != PREVIOUS:
        raise RuntimeError("Expected the four-column v2 dataset")
    names = hub.api.list_repo_files(hub.repo, repo_type="dataset", revision=parent)
    files, converted, observations, identities = [], set(), 0, set()
    for name in sorted(n for n in names if n.startswith("manifests/") and n.endswith(".json")):
        original = cw.read_json(Path(hf_hub_download(hub.repo, name, repo_type="dataset", revision=parent, token=hub.token)))
        source_file = next(f for f in original["files"] if f["path"].endswith(".parquet"))
        old = Path(hf_hub_download(hub.repo, source_file["path"], repo_type="dataset", revision=parent, token=hub.token))
        if cw.digest(old) != source_file["sha256"]:
            raise RuntimeError("Current source shard checksum mismatch")
        destination = stage / source_file["path"]
        if not destination.resolve().is_relative_to(stage.resolve()):
            raise RuntimeError("Invalid source shard path")
        if original.get("migrated_from"):
            ancestor = original["migrated_from"]
            legacy = Path(hf_hub_download(hub.repo, source_file["path"], repo_type="dataset",
                                         revision=ancestor["revision"], token=hub.token))
            if cw.digest(legacy) != ancestor["parquet_sha256"]:
                raise RuntimeError("Legacy source checksum mismatch")
            count = convert_parquet(legacy, destination)
        else:
            work = stage / "reconstruct" / original["start"]
            work.mkdir(parents=True, exist_ok=True)
            args = SimpleNamespace(state=work, collector="gdelt-type1", exporter="gdelt-export",
                                   threads=4, max_expanded_mib=4096, max_download_mib=1024,
                                   max_fragments=16_000_000, min_free_gib=12, shard_gib=8)
            recovered = cw.build_batch(args, original["start"], original["end"], original["kind"])
            if recovered["end"] != original["end"] or recovered["observations"] != original["observations"]:
                raise RuntimeError("Reprocessing does not cover the exact original batch")
            expected = {x["minute"]: (x["status"], x.get("raw_sha256")) for x in original["minutes"]}
            actual = {x["minute"]: (x["status"], x.get("raw_sha256")) for x in recovered["minutes"]}
            if actual != expected:
                raise RuntimeError("Upstream source availability/content changed; refusing inconsistent upgrade")
            destination.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.copyfile(work / "batch/observations.parquet", destination)
            count = recovered["observations"]
        verify_preserved(old, destination)
        table = pq.read_table(destination)
        if table.schema != cw.SCHEMA:
            raise RuntimeError("Upgraded shard has an unexpected schema")
        ids = table["observation_id"].to_pylist()
        if len(set(ids)) != len(ids) or identities.intersection(ids):
            raise RuntimeError("Upgrade contains duplicate observation identities")
        identities.update(ids)
        if count != original["observations"] or source_file["path"] in converted:
            raise RuntimeError("Duplicate shard or changed row count")
        observations += count
        converted.add(source_file["path"])
        artifact = {**cw.file_record(destination, source_file["path"]), "local": source_file["path"]}
        files.append(artifact)
        updated = {**original, "pipeline": cw.PIPELINE, "schema": 3, "files": [artifact],
                   "code_revision": os.environ.get("GDELT_CODE_REVISION", "unknown"),
                   "native_metadata_upgrade_from": parent}
        manifest = stage / name
        if not manifest.resolve().is_relative_to(stage.resolve()):
            raise RuntimeError("Invalid manifest path")
        cw.write_json(manifest, updated)
        files.append({**cw.file_record(manifest, name), "local": name})
    if converted != {n for n in names if n.startswith("data/") and n.endswith(".parquet")} or observations != progress["total_observations"]:
        raise RuntimeError("Upgrade would lose published observations")
    latest_manifest = next(f for f in files if f["path"] == progress["last_manifest"])
    updated = {**progress, "pipeline": cw.PIPELINE, "schema": 3,
               "last_manifest_sha256": latest_manifest["sha256"],
               "published_bytes": sum(f["bytes"] for f in files),
               "updated_at": datetime.now(timezone.utc).isoformat()}
    cw.write_json(stage / "progress.json", updated)
    cw.write_json(stage / "plan.json", {"parent": parent, "files": files, "rows": observations})
    print(json.dumps({"rows": observations, "bytes": updated["published_bytes"], "apply": apply}), flush=True)
    if not apply:
        return
    cw.check_upload_budget(hub, files, 7000)
    ops = [CommitOperationAdd(path_in_repo=f["path"], path_or_fileobj=stage / f["local"]) for f in files]
    ops.append(CommitOperationAdd(path_in_repo="progress.json", path_or_fileobj=stage / "progress.json"))
    ops.append(CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=Path(__file__).parents[1] / "deploy/dataset-card.md"))
    result = hub.api.create_commit(hub.repo, repo_type="dataset", parent_commit=parent,
                                  operations=ops, commit_message="Retain native reconstruction metadata without daily enrichment")
    hub.verify(files, result.oid)
    cw.write_json(stage / "verified.json", {"revision": result.oid, "rows": observations, "bytes": updated["published_bytes"]})
    print(json.dumps({"verified": True, "revision": result.oid, "rows": observations}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="openalphalab/gdelt-news")
    parser.add_argument("--state", type=Path, default=Path("/data"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    import fcntl
    with (args.state / ".worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        hub = cw.Hub(args.repo, Path("/run/secrets/hf_token").read_text().strip())
        hub.ensure_repo()
        upgrade(hub, args.state / "native-metadata-upgrade", args.apply)


if __name__ == "__main__":
    main()
