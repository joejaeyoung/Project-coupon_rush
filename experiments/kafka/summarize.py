#!/usr/bin/env python3
"""Audit saved traces and render figures. Requires matplotlib (and its numpy dependency)."""
import argparse
import gzip
import hashlib
import json
import shutil
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def read(path):
    return json.loads(path.read_text())


def trace(path):
    with gzip.open(path, "rt") as file:
        return [json.loads(line) for line in file]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    source, destination = args.source.resolve(), args.destination.resolve()
    completion = read(source / "completion.json")
    assert completion == {"passed": True, "benchmark_cases": 18, "recovery_cases": 3}, completion
    results = read(source / "benchmark-results.json")
    recoveries = read(source / "recovery-results.json")
    groups, series = defaultdict(list), defaultdict(list)
    evidence = destination / "evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    audited = 0
    for result in results:
        case = source / result["case"]
        requests = trace(case / "requests.jsonl.gz")
        completed = trace(case / "completions.jsonl.gz")
        keyed = {(r["couponId"], r["userId"]): r["completedAtMs"] for r in completed}
        assert len(requests) == len(completed) == len(keyed) == result["requests"]
        assert all(r["status"] == 202 for r in requests)
        assert len({(r["partition"], r["offset"]) for r in completed}) == len(completed)
        partitions_by_coupon, offsets_by_partition = defaultdict(set), defaultdict(list)
        for row in completed:
            partitions_by_coupon[row["couponId"]].add(row["partition"])
            offsets_by_partition[row["partition"]].append(row["offset"])
        assert all(len(partitions) == 1 for partitions in partitions_by_coupon.values())
        for offsets in offsets_by_partition.values():
            assert all(b == a + 1 for a, b in zip(offsets, offsets[1:]))
        if result["workload"] == "hot":
            assert len(offsets_by_partition) == len(result["thread_records"]) == 1
        elif result["consumers"] == 3:
            assert len(offsets_by_partition) == len(result["thread_records"]) == 3
        latencies = [keyed[(r["couponId"], r["userId"])] - r["startedAtMs"] for r in requests]
        # Independent library computation of the raw-trace percentile.
        assert abs(float(np.quantile(latencies, .95)) - result["request_to_db_p95_ms"]) < .0001
        state = read(case / "state.json")
        assert sum(c["db_rows"] for c in state["coupons"]) == result["requests"]
        assert all(c["db_rows"] == c["issued_quantity"] == c["total_quantity"] for c in state["coupons"])
        assert not state["duplicateGroups"]
        assert all(int(c["stock"]) == 0 for c in state["redis"])
        assert sum(c["users"] for c in state["redis"]) == result["requests"]
        samples = read(case / "lag.json")
        assert samples["errors"] == []
        assert max(s["lag"] for s in samples["samples"]) == result["observed_max_lag"]
        assert all(o["end"] == o["committed"] for o in result["final_offsets"])
        group = (result["workload"], result["partitions"], result["consumers"])
        groups[group].append(result)
        start = min(r["startedAtMs"] for r in requests)
        series[group].append(([(s["atMs"] - start) / 1000 for s in samples["samples"]],
                              [s["lag"] for s in samples["samples"]]))
        output = evidence / case.name
        output.mkdir(exist_ok=True)
        for name in ("requests.jsonl.gz", "completions.jsonl.gz", "state.json", "lag.json", "result.json", "config.json"):
            shutil.copy2(case / name, output / name)
        audited += len(requests)
    for result in recoveries:
        assert result["exit_code"] == -9
        assert result["checkpoint"]["completion"]["offset"] == result["checkpoint"]["committedOffset"] == 1
        assert result["before_kill_db"] == result["killed_db"] == {"issued_quantity": 2, "db_rows": 2, "distinct_users": 2}
        assert result["replayed_same_offset"]["offset"] == 1
        assert result["final_db_rows"] == result["final_issued_quantity"] == 51
        assert result["duplicate_groups"] == 0
        case = source / result["case"]
        output = evidence / case.name
        output.mkdir(exist_ok=True)
        for name in ("requests.jsonl.gz", "recovered-completions.jsonl.gz", "fault-checkpoint.json",
                     "killed-state.json", "state.json", "result.json", "config.json", "restarted-config.json"):
            shutil.copy2(case / name, output / name)
        log = (case / "restarted-app.log").read_text()
        event_lines = [line for line in log.splitlines() if "중복 시도" in line or "LAB_FAULT_READY" in line]
        assert len([line for line in event_lines if "중복 시도" in line]) == 1
        (output / "replay-log.txt").write_text("\n".join(event_lines) + "\n")
    for name in ("run.json", "completion.json", "benchmark.csv", "benchmark-results.json", "recovery-results.json"):
        shutil.copy2(source / name, evidence / name)

    metrics = ("http_p95_ms", "request_to_db_p95_ms", "request_to_db_p99_ms", "db_completion_rate_per_s",
               "observed_max_lag", "lag_clear_after_last_http_ms")
    aggregate = []
    configurations = [(1, 1), (3, 1), (3, 3)]
    for workload in ("hot", "spread"):
        for p, c in configurations:
            rows = groups[(workload, p, c)]
            assert len(rows) == 3
            item = {"workload": workload, "partitions": p, "consumers": c, "runs": 3}
            for metric in metrics:
                values = [r[metric] for r in rows]
                item[metric] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
            item["partition_records_by_run"] = [r["partition_records"] for r in rows]
            aggregate.append(item)
    (destination / "aggregate.json").write_text(json.dumps(aggregate, indent=2) + "\n")

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False,
                         "axes.spines.right": False, "svg.fonttype": "none"})
    colors = ["#64748b", "#d97706", "#0f766e"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.7), sharey=True)
    for axis, workload, title in zip(axes, ("hot", "spread"), ("One hot coupon", "30 coupon keys")):
        for index, (p, c) in enumerate(configurations):
            values = [r["request_to_db_p95_ms"] / 1000 for r in groups[(workload, p, c)]]
            median = statistics.median(values)
            axis.bar(index, median, width=.55, color=colors[index], alpha=.82)
            axis.errorbar(index, median, yerr=[[median-min(values)], [max(values)-median]], color="#172033", capsize=5)
            axis.scatter([index-.075, index, index+.075], values, s=24, color="#172033", zorder=4)
            axis.annotate(f"{median:.2f}s", (index, max(values)), xytext=(0, 9), textcoords="offset points", ha="center")
        axis.set_title(title, fontweight="bold", pad=16)
        axis.set_xticks(range(3), ["P1 / C1", "P3 / C1", "P3 / C3"])
        axis.grid(axis="y", alpha=.18)
        axis.set_axisbelow(True)
        axis.set_ylim(0, max(r["request_to_db_p95_ms"] for r in results)/1000*1.25)
    axes[0].set_ylabel("Request to DB commit observed: p95 (seconds)")
    fig.suptitle("Kafka scaling: 6,000 accepted requests per run", fontweight="bold", fontsize=15)
    fig.text(.5, .02, "Bars: median of 3 runs  |  Dots: each run  |  Whiskers: min-max  |  Single broker, same host", ha="center", fontsize=9)
    fig.tight_layout(rect=(0, .065, 1, .95))
    fig.savefig(destination / "request-to-db-p95.svg")
    fig.savefig(destination / "request-to-db-p95.png", dpi=180)
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), sharey=True)
    for axis, workload, title in zip(axes, ("hot", "spread"), ("One hot coupon", "30 coupon keys")):
        for index, (p, c) in enumerate(configurations):
            for repeat, (times, lags) in enumerate(series[(workload, p, c)]):
                axis.plot(times, lags, color=colors[index], alpha=.65, linewidth=1.1,
                          label=f"P{p}/C{c}" if repeat == 0 else None)
        axis.set_title(title, fontweight="bold", pad=12)
        axis.set_xlabel("Seconds since first HTTP request")
        axis.grid(alpha=.18)
        axis.legend(frameon=False)
    axes[0].set_ylabel("Committed-offset lag (records)")
    fig.suptitle("Consumer lag: all three runs per configuration", fontweight="bold", fontsize=15)
    fig.tight_layout(rect=(0, 0, 1, .95))
    fig.savefig(destination / "consumer-lag.svg")
    fig.savefig(destination / "consumer-lag.png", dpi=180)
    plt.close(fig)
    for svg in destination.glob("*.svg"):
        svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")

    audit = {"benchmark_cases": len(results), "raw_requests_audited": audited,
             "numpy_percentiles_match": True, "recovery_cases": len(recoveries),
             "all_recovery_offsets_replayed": True, "same_coupon_single_partition": True,
             "partition_offsets_contiguous_in_order": True}
    (destination / "audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    sums = [hashlib.sha256(p.read_bytes()).hexdigest() + "  " + str(p.relative_to(destination))
            for p in sorted(evidence.rglob("*")) if p.is_file()]
    (destination / "sha256sums.txt").write_text("\n".join(sums) + "\n")
    print(json.dumps({"audit": audit, "aggregate": aggregate}, indent=2))


if __name__ == "__main__":
    main()
