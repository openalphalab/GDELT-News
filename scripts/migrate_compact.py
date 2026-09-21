"""Replace the enriched v1 publication with verified compact Parquet, atomically.

Run with the collector stopped. Existing Git history is retained; only the current
published tree changes. --apply is required to commit the prepared migration.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
from huggingface_hub import CommitOperationAdd, CommitOperationDelete, hf_hub_download

import compact_worker as cw
from worker import Hub, PIPELINE as LEGACY_PIPELINE


def convert_parquet(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pq.read_table(source, columns=["observed_at", "lang", "url", "text"])
    compact = table.rename_columns(cw.SCHEMA.names)
    if compact.schema != cw.SCHEMA:
        raise RuntimeError("Unexpected source schema")
    pq.write_table(compact, destination, compression="zstd", compression_level=9,
                   use_dictionary=["language"], row_group_size=512)
    actual = pq.read_table(destination)
    if not actual.equals(compact):
        raise RuntimeError("Migration changed observation contents")
    return actual.num_rows


def migrate(hub, stage, maximum_gb, apply=False):
    progress, parent = hub.progress()
    if progress and progress.get("pipeline") == cw.PIPELINE:
        print(json.dumps({"already_migrated": True, "revision": parent}))
        return
    if not progress or progress.get("pipeline") != LEGACY_PIPELINE:
        raise RuntimeError("Only the known enriched-v1 dataset may be migrated")
    names = hub.api.list_repo_files(hub.repo, repo_type="dataset", revision=parent)
    stage.mkdir(parents=True, exist_ok=True)
    files, rows, converted = [], 0, set()
    for name in sorted(n for n in names if n.startswith("manifests/") and n.endswith(".json")):
        original = cw.read_json(Path(hf_hub_download(hub.repo, name, repo_type="dataset", revision=parent, token=hub.token)))
        if original.get("pipeline") != LEGACY_PIPELINE:
            raise RuntimeError("Mixed legacy pipeline")
        shards = [f for f in original["files"] if f["path"].startswith("data/") and f["path"].endswith(".parquet")]
        if len(shards) != 1:
            raise RuntimeError("Expected one data shard per legacy batch")
        shard = shards[0]
        source = Path(hf_hub_download(hub.repo, shard["path"], repo_type="dataset", revision=parent, token=hub.token))
        if cw.digest(source) != shard["sha256"] or source.stat().st_size != shard["bytes"]:
            raise RuntimeError("Legacy shard checksum mismatch")
        destination = stage / shard["path"]
        if not destination.resolve().is_relative_to(stage.resolve()):
            raise RuntimeError("Invalid remote shard path")
        count = convert_parquet(source, destination)
        if count != original["observations"] or shard["path"] in converted:
            raise RuntimeError("Legacy observation count or shard uniqueness mismatch")
        converted.add(shard["path"])
        rows += count
        data_file = {**cw.file_record(destination, shard["path"]), "local": shard["path"]}
        files.append(data_file)
        manifest = {**original, "schema": 2, "pipeline": cw.PIPELINE, "kind": "forward",
                    "files": [data_file], "code_revision": os.environ.get("GDELT_CODE_REVISION", "unknown"),
                    "migrated_from": {"revision": parent, "pipeline": LEGACY_PIPELINE,
                                      "parquet_sha256": shard["sha256"], "code_revision": original.get("code_revision")}}
        path = stage / name
        if not path.resolve().is_relative_to(stage.resolve()):
            raise RuntimeError("Invalid remote manifest path")
        cw.write_json(path, manifest)
        files.append({**cw.file_record(path, name), "local": name})
    expected_data = {n for n in names if n.startswith("data/") and n.endswith(".parquet")}
    if converted != expected_data or rows != progress["total_observations"]:
        raise RuntimeError("Migration would lose or duplicate published observations")
    deleted = sorted(n for n in names if n.startswith(("evidence/", "llm/")))
    latest_manifest = next(f for f in files if f["path"] == progress["last_manifest"])
    updated = {**progress, "schema": 2, "pipeline": cw.PIPELINE, "last_action": "forward",
               "pending_missing_minutes": [], "last_manifest_sha256": latest_manifest["sha256"],
               "published_bytes": sum(f["bytes"] for f in files),
               "updated_at": datetime.now(timezone.utc).isoformat(), "migrated_from_commit": parent}
    cw.write_json(stage / "progress.json", updated)
    cw.write_json(stage / "plan.json", {"parent": parent, "files": files, "remove_from_current_tree": deleted,
                                       "rows": rows, "new_published_bytes": updated["published_bytes"]})
    print(json.dumps({"rows": rows, "parquet_files": len(converted), "removed_files": len(deleted),
                      "new_published_bytes": updated["published_bytes"], "apply": apply}))
    if not apply:
        return
    cw.check_upload_budget(hub, files, maximum_gb)
    ops = [CommitOperationDelete(path_in_repo=name) for name in deleted]
    ops.extend(CommitOperationAdd(path_in_repo=f["path"], path_or_fileobj=stage / f["local"]) for f in files)
    ops.append(CommitOperationAdd(path_in_repo="progress.json", path_or_fileobj=stage / "progress.json"))
    ops.append(CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=Path(__file__).parents[1] / "deploy/dataset-card.md"))
    result = hub.api.create_commit(hub.repo, repo_type="dataset", parent_commit=parent,
                                  operations=ops, commit_message="Replace enriched archive with four-column Parquet")
    hub.verify(files, result.oid)
    current = set(hub.api.list_repo_files(hub.repo, repo_type="dataset", revision=result.oid))
    if any(name in current for name in deleted):
        raise RuntimeError("Legacy files remain in migrated current tree")
    cw.write_json(stage / "verified.json", {"revision": result.oid, "rows": rows, "bytes": updated["published_bytes"]})
    print(json.dumps({"verified": True, "revision": result.oid, "rows": rows}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="openalphalab/gdelt-news")
    parser.add_argument("--state", type=Path, default=Path("/data"))
    parser.add_argument("--token-file", type=Path, default=Path("/run/secrets/hf_token"))
    parser.add_argument("--max-hub-storage-gb", type=float, default=7000)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    import fcntl
    with (args.state / ".worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        hub = Hub(args.repo, args.token_file.read_text().strip())
        hub.ensure_repo()
        migrate(hub, args.state / "compact-migration", args.max_hub_storage_gb, args.apply)


if __name__ == "__main__":
    main()
