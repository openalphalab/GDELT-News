# Persistent Alibaba deployment: compact Parquet

Run this worker on Linux with Docker and the Docker Compose plugin. It needs no
inbound network port. It uses up to 4 CPUs and 12 GiB memory, keeps a 12 GiB disk
reserve, and stores at most one unpublished batch per scheduling lane plus verified caches.
The server's existing operating system need not be replaced.

The Docker entrypoint is `scripts/compact_worker.py`. Public data has seven
columns: `date`, `language`, `source_url`, `text`, `observation_id`, `type` and
`metadata` (a struct of native source/reconstruction diagnostics). Both Type 1
and Type 2 are included. Only ngram data is used: no country, publisher, author
or external metadata dependency. The old enriched worker and
enrichment scripts remain available for separate use, but are not deployed.

Backfill uses 15-minute native collection windows with four concurrent downloads.
The Rust HTTP connection pool is reused across each window, and bounded downloads
overlap reconstruction of one input file at a time. All four CPU threads remain
available for reconstruction. Outputs are exported in chronological order, with
per-minute results checked against the collector summary before publication.
A failed or unresolved minute prevents the batch from being published.

Shard-size and low-space sealing happen at window boundaries so prefetched sources
are not discarded before publication. A shard can exceed its target by one window.
The download reserve includes the maximum bytes of other in-flight responses.
Near the live edge, the existing one-minute publication plan reduces the window to
one minute automatically; this setting does not add 15 minutes of live lag.
Use `--download-workers 1 --collect-window-minutes 1` for a serial comparison.

The worker explicitly downloads from GDELT's public Google Cloud Storage bucket:
`https://storage.googleapis.com/data.gdeltproject.org/gdeltv3/webngrams/`.
The website download endpoint was observed caching early 404s for 3,600 seconds,
even after the same object was present in that bucket. The bucket's missing-object
response uses `private, max-age=0`. Manifests retain the canonical source URL and
the actual `download_source`; reconstruction and observation identity are unchanged.
The standalone Rust CLI still accepts `collect --base-url` for explicit source selection.

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

The deployed `--latest-first` scheduler prioritizes current files and fills older
history backward in chunks of up to 15 minutes. It begins live collection with
the 15 source minutes ending at the one-minute safety cutoff, then follows newly
available minutes. It checks live work before every historical chunk, polling
every 30 seconds when idle. There is no 48-hour wait or requirement to finish
history first. Actual latency includes upstream publication, the current bounded
work unit, reconstruction and uploading; it is not a delivery-time guarantee.

This is one coordinated process, not simultaneous reconstruction jobs: it keeps
the existing 12 GiB ceiling and one atomic uploader. Live, backfill and late-file
repair have separate local pending directories and retry backoffs. An errored
historical chunk stays on disk for retry while live work remains eligible.
Delayed files in the live date range take priority over historical missing-file
retries. Up to four recent repair attempts can run between backward chunks, with
new live work still taking precedence. Historical-only retries retain one turn
per backward chunk; neither queue changes or rewinds the coverage cursors.
Shared disk/quota/network failures can still affect all lanes. A single difficult
input can delay switching lanes until its collector attempt finishes or fails.

On upgrade, any existing forward batch is verified and published first. Already
published 2020 data is retained. The remote checkpoint freezes a live/backfill
boundary and a historical lower boundary at the end of that retained prefix.
Backward chunks meet that prefix without republishing it. Source minutes within
each chunk are still written in ascending order; chunks are selected newest first.
The two cursors and data files commit atomically. Per-lane acknowledgements make a
lost commit response idempotent even if another lane publishes before retry.
The old chronological mode refuses checkpoints created by `--latest-first`.

Check `/srv/gdelt-news/data/status.json`, `checkpoint.json`, per-lane `status.json`
and remote `progress.json` for actual progress and errors. `live_next_minute` is
the next new source minute; `backfill_next_end` is the newest still-unprocessed
historical minute. Backfill is complete when it is less than `backfill_floor`.

Each nonempty publication atomically commits Parquet, a small manifest and the remote
checkpoint. Local scratch is removed only after remote hashes are verified. A
lost upload response or power failure retries the same immutable batch. Failed
downloads, corrupt inputs, full disks, authentication failures and storage quota
errors retain local pending work and retry with backoff. Recent HTTP 404s are stored
in a durable retry queue for 24 hours, checked with 30-second to five-minute backoff
per absent file. Recovered files use the same `START-END.parquet` naming as other
shards; their manifests record `kind: repair`. Recovery never rewinds the forward
cursor. Repeated 404 probes do not create remote commits.
Older gaps and arrivals beyond the retry window need a separate repair run.

Missing-source retries reuse the Hub checkpoint and preflight quota result for
up to 30 seconds. Every publication still reads fresh progress, checks storage
and uses an atomic parent-commit guard. SDK requests to the Hub are paced at one
per second (about 300 per five minutes versus the observed 1,000-request quota),
including internal commit calls. Bulk CDN/Xet transfers are unaffected. The
client waits for reset when the response headers report 100 or fewer remaining
requests, leaving headroom for other activity. HTTP 429 pauses all lanes until the Hub's
advertised reset time; this cooldown survives restarts. Worker status and logs
include the HTTP status and cooldown duration without exposing credentials.

A new source interval with zero rows updates only the shared `progress.json`:
no Parquet and no per-batch manifest is uploaded. This applies to missing files
and valid inputs containing no reconstructable observations. A temporary local
receipt is checksummed for restart recovery and removed after the exact remote
checkpoint is verified. Missing recent timestamps stay in the checkpoint retry
queue, and aggregate quarantine counts remain available. Older empty-only gaps
do not retain individual public receipts. Rechecking an absent file does not
publish another commit, and idle 30-second polls do not recollect already-processed
live minutes. `lane_commits.*.path` is null for checkpoint-only acknowledgements;
`last_manifest` refers to the latest available nonempty manifest, when present.

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
