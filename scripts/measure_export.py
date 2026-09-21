"""Validate a compact export against every raw fragment and measure actual sizes.

Optional local packages: tiktoken (token counts), zstandard and brotli (codecs).
No article text is sent to a model or remote service. Tokenizer tables may be
downloaded by tiktoken if they are not already cached.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import tempfile
from time import perf_counter
import unicodedata


def check(condition, message):
    if not condition:
        raise ValueError(message)


def atomic_write(path, content):
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--export-directory", type=Path, required=True)
    parser.add_argument("--keep-artifacts", action="store_true", help="Match collector --keep-artifacts")
    parser.add_argument("--tokenizer", default="o200k_base")
    args = parser.parse_args()
    directory = args.export_directory
    with (directory / "observations.jsonl").open(encoding="utf8") as stream:
        meta = json.loads(next(stream))["meta"]
        records = [json.loads(line) for line in stream]
    check(digest(args.raw) == meta["raw_sha256"], "raw checksum mismatch")
    by_key = {(r["url"], r["observed_at"], r["lang"], r["type"]): r for r in records}
    check(len(by_key) == len(records), "duplicate observations")
    check(len(records) == meta["observations"], "observation count mismatch")
    expected = Counter()
    expanded = 0
    with gzip.open(args.raw, "rb") as stream:
        for line in stream:
            expanded += len(line)
            raw = json.loads(line)
            check(raw["type"] in (1, 2), "unexpected source type")
            key = raw["url"], raw["date"], raw["lang"], raw["type"]
            check(key in by_key, f"missing raw observation: {key}")
            if raw["type"] == 1:
                text = " ".join(" ".join((raw["pre"], raw["ngram"], raw["post"])).split())
                if not args.keep_artifacts and raw["pos"] < 20 and " / " in text:
                    text = text.split(" / ", 1)[1]
            else:
                text = unicodedata.normalize("NFC", raw["pre"] + raw["ngram"] + raw["post"])
            check(text in by_key[key]["text"], f"missing normalized fragment: {key}")
            expected[key] += 1
    check(set(expected) == set(by_key), "extra export observations")
    check(all(expected[k] == r["fragments"] for k, r in by_key.items()), "source fragment count mismatch")
    packet = json.loads((directory / "observations.packed.json").read_text(encoding="utf8"))
    unpacked = []
    for values in packet["observations"]:
        check(len(values) == len(packet["columns"]), "packed row length mismatch")
        row = dict(zip(packet["columns"], values))
        row["text"] = packet["texts"][row.pop("text_id")]
        unpacked.append(row)
    check(packet["meta"] == meta and unpacked == records, "packed JSON does not round-trip")
    print(json.dumps({"validation": "passed", "observations": len(records), "raw_fragments": sum(expected.values())}), flush=True)
    codecs = [("gzip6", lambda b: gzip.compress(b, compresslevel=6, mtime=0), gzip.decompress)]
    versions = {"python": platform.python_version()}
    try:
        import zstandard
        versions["zstandard"] = importlib.metadata.version("zstandard")
        for level in (3, 9, 19):
            compressor = zstandard.ZstdCompressor(level=level, write_checksum=True)
            codecs.append((f"zstd{level}", compressor.compress, zstandard.ZstdDecompressor().decompress))
    except ImportError:
        pass
    try:
        import brotli
        versions["brotli"] = importlib.metadata.version("brotli")
        codecs.append(("brotli11", lambda b: brotli.compress(b, quality=11), brotli.decompress))
    except ImportError:
        pass
    try:
        import tiktoken
        encoder = tiktoken.get_encoding(args.tokenizer)
        versions["tiktoken"] = importlib.metadata.version("tiktoken")
    except ImportError:
        encoder = None
    measurements = []
    for name in ("observations.jsonl", "observations.packed.json"):
        path = directory / name
        data = path.read_bytes()
        tokens = len(encoder.encode_ordinary(data.decode("utf8"))) if encoder else None
        measured = {"file": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                    "tokens": tokens, "compressions": []}
        for label, compress, decompress in codecs:
            start = perf_counter()
            compressed = compress(data)
            compress_seconds = perf_counter() - start
            start = perf_counter()
            restored = decompress(compressed)
            decompress_seconds = perf_counter() - start
            check(restored == data, "compression round-trip failed")
            extension = "gz" if label.startswith("gzip") else "br" if label.startswith("brotli") else "zst"
            filename = name + "." + label + "." + extension
            atomic_write(directory / filename, compressed)
            measured["compressions"].append({"codec": label, "file": filename, "bytes": len(compressed),
                "sha256": hashlib.sha256(compressed).hexdigest(), "compression_seconds": compress_seconds,
                "decompression_seconds": decompress_seconds, "round_trip_verified": True})
        measurements.append(measured)
        print(json.dumps(measured), flush=True)
    counts = Counter(r["type"] for r in records)
    estimates = Counter(r["type"] for r in records if r["assembly"] == "estimated")
    report = {"source_file": args.raw.name, "raw_sha256": meta["raw_sha256"],
        "source_gzip_bytes": args.raw.stat().st_size, "source_expanded_bytes": expanded,
        "observations": len(records), "types": dict(counts), "languages": dict(Counter(r["lang"] for r in records)),
        "estimated_observations_by_type": dict(estimates), "source_fragments": sum(expected.values()),
        "all_normalized_source_fragments_present": True, "packed_round_trip_verified": True,
        "unique_texts": len(packet["texts"]), "tokenizer": args.tokenizer if encoder else None,
        "token_scope": "Literal file text, not chat framing; actual model tokenization may differ.",
        "compression_scope": "Single local run per codec, in-memory compression/decompression excluding disk I/O.",
        "versions": versions, "measurements": measurements}
    atomic_write(directory / "size-report.json", (json.dumps(report, ensure_ascii=False, indent=2) + "\n").encode())


if __name__ == "__main__":
    main()
