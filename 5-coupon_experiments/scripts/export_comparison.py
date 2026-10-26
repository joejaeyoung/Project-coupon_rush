#!/usr/bin/env python3
"""Export measured distribution case summaries to a compact, reproducible CSV.

python3 scripts/export_comparison.py results/distribution/summary.json \
    --output results/distribution/comparison.csv
"""

import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.summary.read_text())
    rows = []
    for index, case in enumerate(report["cases"], 1):
        p, m = case["parameters"], case["metrics"]
        if "workload" not in p:
            continue
        api_samples = m["apiLatencyMs"]["samples"]
        submission_ms = m.get("submissionWallMs")
        rows.append({
            "strategy": p["strategy"], "distribution": p["workload"],
            "partitions": p["partitions"], "workers": p["workers"],
            "measured_requests": p["requests"], "warmup_requests": p["warmupRequests"],
            "concurrency": p["concurrency"], "seed": p["seed"], "execution_index": index,
            "api_submission_requests_per_second": api_samples / (submission_ms / 1000) if submission_ms else None,
            "api_p50_ms": m["apiLatencyMs"]["p50"], "api_p95_ms": m["apiLatencyMs"]["p95"],
            "api_p99_ms": m["apiLatencyMs"]["p99"],
            "issued_end_to_end_p95_ms": m["issuedEndToEndLatencyMs"]["p95"],
            "all_terminal_end_to_end_p95_ms": m["allTerminalEndToEndLatencyMs"]["p95"],
            "issued_through_settlement_per_second": m["issuedPerWallSecond"],
            "submission_wall_ms": submission_ms,
            "submission_through_settlement_wall_ms": m["submissionThroughSettlementWallMs"],
            "measured_issued": m["issued"],
            "partition_request_counts": json.dumps(m["partitionRequestCounts"], sort_keys=True, separators=(",", ":")),
            "passed": case["passed"], "raw_case_file": case["file"],
        })
    if not rows:
        parser.error("No distribution cases found in the supplied summary")
    rows.sort(key=lambda r: (r["strategy"], r["distribution"], r["partitions"], r["workers"]))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({"output": str(args.output), "rows": len(rows),
                      "note": "Single finite burst per combination. Rates include client/network or settlement-poll overhead; not steady-state capacity."}))


if __name__ == "__main__":
    main()
