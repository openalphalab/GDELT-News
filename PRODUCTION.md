# Production operation

This component provides bounded inputs, checksummed checkpoints, raw-file
preservation, explicit uncertain reconstruction, retries and atomic output
publication. Production suitability still depends on measured workload and
the deployment host. Publisher accuracy is not guaranteed.

## Build and execution

Build from `Cargo.lock` using `cargo build --release --locked`. The container
build runs formatting, tests, Clippy with warnings denied and release compilation.
Both builder and runtime images are pinned by digest. The runtime contains the
collector and `gdelt-export`, uses UID/GID 10001, and needs a writable `/data`.

```sh
docker build --platform linux/amd64 -t firstlight-gdelt:0.4.0 .
docker volume create gdelt-news
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges --cpus 2 --memory 4g --mount source=gdelt-news,target=/data firstlight-gdelt:0.4.0 collect --start 20250316000100 --end 20250316000100 --types 1,2 --type2-best-effort --threads 2
```

The memory setting is an example deployment cap, not a guarantee that arbitrary
files fit it. The exact provided sample is used for container smoke validation.
For bind mounts on Linux, grant UID 10001 write access to the archive directory.
The exporter can run via `--entrypoint /usr/local/bin/gdelt-export`; pass its
`--articles` and `--output-directory` paths within the mounted volume.

Windows x64 and Linux x64 are tested. Linux ARM64 is an intended Oracle target
but remains untested; no ARM64 deployment has been performed.

Version 0.4 passes 48 tests on Windows x64 and Linux x64. The Linux container
reconstructed and exported the complete 2,359-observation sample as UID 10001
with a read-only root, no capabilities, no-new-privileges, two CPUs and a 4 GiB
memory limit. All observation records match Windows exactly; only file-level
provenance differs because the container imported its raw source. A repeated
collection verified and reused the cache. Machine-readable evidence is in
[PRODUCTION-VALIDATION.json](PRODUCTION-VALIDATION.json).

## Limits and failure behavior

Defaults bound each source to 128 MiB compressed, 512 MiB expanded, 1 MiB per
source JSON row, two million selected fragments and 100,000 per observation.
Fragments have at most 256 word/grapheme tokens. Imports check size before
copying and reserve 64 MiB beyond the source length. Downloads check the
configured free-space reserve before requesting data. Monitor actual RAM and
disk usage: concurrent writes and reconstruction need additional space.

Type 2 branch search defaults to 20,000 work units, at most 64 pending states
and an estimated 64 MiB of search-state/memo storage per observation. Shared
indices are outside this estimate. Exhaustion leaves uncertainty visible.
Best-effort section comparison has a bounded budget and falls back to marked
position-only joins while retaining text.

The exporter bounds expanded input, each derived record (64 MiB), observations
(100,000), per-observation fragments (100,000) and aggregate text (512 MiB).
Type 1 residual reconstruction caps summed passes at two million fragments and
substring-comparison estimates at 100 million bytes per observation. A cap
retains remaining fragments and sets `bounded_fallback`; it cannot silently
discard content. Export holds records and packed text in memory, so input
limits are not a process-memory guarantee.

Only HTTP 200 is accepted as an object. A 206 cannot become a completed file.
TLS certificate validation stays enabled; HTTPS sources reject HTTP redirects.
429/503 apply shared Retry-After cooldowns. Delays above 300 seconds fail
visibly for a later run. Connect and request timeouts are 15 and 180 seconds.

## Monitoring and recovery

- Exit 0 means no failed files, but inspect `missing`: 404 is recorded separately.
  Exit 1 means an invalid request, startup error or one or more failed files.
- Keep `runs/*/request.json`, per-file statuses and `summary.json`. Summary
  counters include both types, uncertain articles, best-effort outputs and
  position-only joins. Run time is recorded.
- Rerun the same interval to resume. Successful raw/output SHA-256 hashes are
  verified before reuse. Corrupt downloaded raw files are quarantined for
  refetch; corrupt imports fail visibly. Changed settings use new profiles.
- A process-wide archive lock prevents concurrent writers to the same archive.
  Process termination releases the lock. Forced-stop tests cover restart while
  a request was pending; they do not simulate power loss on every filesystem.
- Back up the complete archive and keep raw files with derived exports. Export
  JSON omits per-fragment diagnostic arrays to reduce LLM input size; those
  remain in the detailed article files and exact raw evidence.
- An export destination must be new. A staging directory is renamed only after
  validation and file sync. Failed validation leaves no completed export.

No service, scheduler, retention deletion or remote deployment is installed.
Use the collector from the deployment's existing job runner and alert on failed
runs, persistent missing files, resource limits and unexpected count changes.

`assembly: "estimated"` and `position_joins` describe heuristic ordering;
`completeness: "not_verified"` applies to every observation. Retaining every
supplied fragment does not prove that GDELT supplied the entire publisher text.
