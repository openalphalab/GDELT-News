"""Alternate old/new Type 2 kernels on identical prepared fragments."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess


def peak_memory(process):
    if os.name != "nt":
        return None
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in
            ("peak_working_set", "working_set", "peak_paged", "paged", "peak_nonpaged", "nonpaged", "pagefile", "peak_pagefile")]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    query = ctypes.WinDLL("psapi").GetProcessMemoryInfo
    query.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    query.restype = wintypes.BOOL
    return counters.peak_working_set if query(int(process._handle), ctypes.byref(counters), counters.cb) else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--current", required=True, type=Path)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    results = {"baseline": [], "optimized": []}
    for trial in range(args.runs):
        order = [("baseline", args.baseline), ("optimized", args.current)]
        if trial % 2:
            order.reverse()
        for name, binary in order:
            process = subprocess.Popen([str(binary.resolve()), str(args.input.resolve()), "1"],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       creationflags=0x08000000 if os.name == "nt" else 0)
            stdout, stderr = process.communicate()
            if process.returncode:
                raise RuntimeError(stderr.decode("utf8", errors="replace"))
            run = json.loads(stdout)
            results[name].append({"seconds": run["seconds"][0], "peak_working_set_bytes": peak_memory(process),
                                  "version": run["version"], "recovery_sha256": run["recovery_sha256"]})
    report = {"scope": "Single-thread kernel: includes input cloning and reconstruction; excludes file parsing, serialization, downloads. Peak memory includes the complete benchmark process.",
              "input_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(), "runs": results}
    for name in results:
        assert len({r["recovery_sha256"] for r in results[name]}) == 1, "Nondeterministic output"
        report[name + "_median_seconds"] = statistics.median(r["seconds"] for r in results[name])
        peaks = [r["peak_working_set_bytes"] for r in results[name] if r["peak_working_set_bytes"] is not None]
        if peaks:
            report[name + "_median_peak_working_set_bytes"] = statistics.median(peaks)
    report["kernel_speedup"] = report["baseline_median_seconds"] / report["optimized_median_seconds"]
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf8")
    print(json.dumps({k:v for k,v in report.items() if k != "runs"}, indent=2))


if __name__ == "__main__":
    main()
