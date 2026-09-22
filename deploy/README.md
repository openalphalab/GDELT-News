# Persistent Alibaba deployment: compact Parquet

Run this worker on Linux with Docker and the Docker Compose plugin. It needs no
inbound network port. It uses up to 4 CPUs and 12 GiB memory, keeps a 12 GiB disk
reserve, and stores at most one unpublished batch plus its verified caches.
The server's existing operating system need not be replaced.

The Docker entrypoint is `scripts/compact_worker.py`. Public data has seven
columns: `date`, `language`, `source_url`, `text`, `observation_id`, `type` and
`metadata` (a struct of native source/reconstruction diagnostics). Both Type 1
and Type 2 are included. Only ngram data is used: no country, publisher, author
or external metadata dependency. The old enriched worker and
enrichment scripts remain available for separate use, but are not deployed.

Historical source minutes can exceed the CLI's conservative default input size.
The supplied Compose configuration allows 1 GiB compressed / 4 GiB expanded and
sixteen million total fragments per minute, while retaining the per-observation and bounded-search
limits. These limits are explicit CLI options; larger inputs stop the checkpoint
for investigation rather than being skipped or silently truncated.

Input byte limits are not RAM allocations. Parsing, indexes, reconstruction and
serialization also use memory. The 12 GiB container ceiling leaves roughly 3 GiB
for Linux, Docker and host services on this VM, whose kernel exposes about 14.8 GiB.
All four CPU cores are available; actual usage depends on the current download,
parsing, reconstruction or upload stage. The 512 MiB `/tmp` mount is a separate
temporary-file ceiling; downloads, intermediate files and upload caches use `/data` on disk.
The standalone worker defaults remain smaller for other deployment environments.

The worker enables `--quarantine-invalid-metadata`: fragments with missing identity
or invalid deciles are preserved with source line numbers and reasons in a
checksummed local sidecar. They never become anonymous merged articles. Batch
manifests and the remote checkpoint count them separately from valid observations;
the sidecar and raw source are not uploaded and expire after verified publication.
Malformed JSON, gzip corruption and exceeded resource limits still fail visibly.

Provision the public Hugging Face dataset `openalphalab/gdelt-news` and create a
fine-grained token granting write access only to that dataset. Keep it out of Git,
command history, images and logs. Use an interactive hidden prompt on the VM:

```sh
sudo install -d -m 700 /srv/gdelt-news/secrets
sudo install -d -o 10001 -g 10001 -m 750 /srv/gdelt-news/data
read -rs -p 'Hugging Face token: ' GDELT_HF_TOKEN
printf '%s' "$GDELT_HF_TOKEN" | sudo tee /srv/gdelt-news/secrets/hf_token >/dev/null
unset GDELT_HF_TOKEN
sudo chown 10001:10001 /srv/gdelt-news/secrets/hf_token
sudo chmod 400 /srv/gdelt-news/secrets/hf_token
```

From the checked-out repository root:

```sh
export GDELT_CODE_REVISION=$(git rev-parse HEAD)
sudo -E docker compose -f deploy/compose.yaml build
sudo -E docker compose -f deploy/compose.yaml up -d
sudo docker compose -f deploy/compose.yaml logs --tail=50 -f
```

Backfill begins at 2020-01-01 00:01 UTC. Historical batches span up to six hours,
sealing sooner at 256 MiB of compressed Parquet or near the disk reserve.
After catching up, the worker follows new files with a one-minute safety margin,
polls every 30 seconds, and publishes individual live source minutes. There is no
48-hour wait. Actual latency includes upstream publication and processing time.
Check `/srv/gdelt-news/data/status.json`, `checkpoint.json` and remote `progress.json`
for actual progress and errors. The historical run does not deliver current news
until it reaches current dates.

Each publication atomically commits Parquet, a small manifest and the remote
checkpoint. Local scratch is removed only after remote hashes are verified. A
lost upload response or power failure retries the same immutable batch. Failed
downloads, corrupt inputs, full disks, authentication failures and storage quota
errors retain local pending work and retry with backoff. Recent HTTP 404s are stored
in a durable retry queue for 24 hours, checked with 30-second to five-minute backoff
per absent file. Successful late arrivals are separate `-late` Parquet shards and
never rewind the forward cursor. Repeated 404 probes do not create remote commits.
Older gaps and arrivals beyond the retry window need a separate repair run.

All compact history is retained. Before collection and upload, the worker checks
the dataset's reported `usedStorage`, stopping new uploads if usage plus twice the
incoming batch bytes would reach 7,000 GB (decimal). It does not automatically
delete history or purchase storage. The check conservatively reserves an extra
payload copy for conversion/version overhead; it is not an account-wide atomic
quota guarantee. Reporting may lag and other repositories can consume allowance.

For a one-time migration from the original enriched deployment, first stop the
collector, then run `scripts/migrate_compact.py` inside the image. Without `--apply`
it prepares and validates the conversion; with `--apply` it replaces all existing
shards and removes evidence/JSONL from the current branch in one commit. Selected
fields and row counts are verified unchanged. It preserves earlier Git history.
An existing unpublished v1 batch must have its request explicitly converted to the
new pipeline before restarting; the compact worker refuses mixed checkpoints.

For the already-published four-column v2 dataset, stop the collector and use
`scripts/upgrade_native_metadata.py --apply`. This restores native fields from
verified original exports or reconstructs the same source bytes, checking the
four core fields, row order, row counts and unique observation IDs before one
atomic commit. All shards receive the same v3 schema. Existing unfinished v2
requests must be explicitly adopted after verifying the remote cursor; never
discard an unverified pending publication. The upgrade retains Git history.

Only one writer should publish this dataset. A Linux file lock prevents two local
workers, and commit parent checking prevents a concurrent remote writer from
silently overwriting progress. To stop without losing work:

```sh
sudo docker compose -f deploy/compose.yaml stop
```

Hugging Face public storage is subject to its current policies and quota decisions;
historical reconstructed news can occupy terabytes. The worker does not buy storage or assume
unlimited free capacity. A rejected upload stops progress, preserves the batch and
appears in the logs. Check quotas and observed throughput before projecting the
completion date. Upload speed and historical coverage have not been benchmarked
across the full historical archive.
