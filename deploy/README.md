# Persistent Alibaba deployment

Run this worker on Linux with Docker and the Docker Compose plugin. It needs no
inbound network port. It uses up to 4 CPUs and 12 GiB memory, keeps a 12 GiB disk
reserve, and stores at most one unpublished batch plus its verified caches.
The server's existing operating system need not be replaced.

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
temporary-file ceiling; downloads, evidence and upload caches use `/data` on disk.
The standalone worker defaults remain smaller for other deployment environments.

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

The first publication contains the earliest minute, 2020-01-01 00:01 UTC. Subsequent
batches span up to six hours, sealing sooner when the evidence TAR reaches 4 GiB.
After catching up, the worker follows new files with a 48-hour lag so daily GGG
metadata can be available. It does not claim instant current-news coverage during
the historical backfill. Check `/srv/gdelt-news/data/status.json`, `checkpoint.json`
and the remote `progress.json` for actual progress and errors.

Each publication atomically commits data, evidence, manifests and the remote
checkpoint. Local scratch is removed only after remote hashes are verified. A
lost upload response or power failure retries the same immutable batch. Failed
downloads, metadata errors, full disks, authentication failures and storage quota
errors retain local pending work and retry with backoff. Source HTTP 404s are
recorded as missing, not reconstructed; 48-hour lag reduces publication races but
cannot guarantee that GDELT never fills historical gaps later. Manifests identify
those gaps for a separate repair run.

Only one writer should publish this dataset. A Linux file lock prevents two local
workers, and commit parent checking prevents a concurrent remote writer from
silently overwriting progress. To stop without losing work:

```sh
sudo docker compose -f deploy/compose.yaml stop
```

Hugging Face public storage is subject to its current policies and quota decisions;
historical raw news can occupy terabytes. The worker does not buy storage or assume
unlimited free capacity. A rejected upload stops progress, preserves the batch and
appears in the logs. Check quotas and observed throughput before projecting the
completion date. Upload speed and metadata enrichment have not been benchmarked
across the full historical archive.
