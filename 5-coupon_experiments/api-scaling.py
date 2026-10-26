#!/usr/bin/env python3
"""Compare one and two real API JVMs while the shared lab consumers are stopped.

The root app.py process owns port 28080. This script owns only the child it launches
on port 28081, verifies its PID/command before signaling, and always stops that child
before enabling the main process's Kafka consumers. No infrastructure is started or
stopped here. Raw HTTP samples and independent final ledger checks are saved.
"""

from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import random
import signal
import socket
import statistics
import subprocess
import threading
import time
import urllib.parse


ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / "runtime"
REPLICA_PORT = 28081
REPLICA_BASE = f"http://127.0.0.1:{REPLICA_PORT}"
REPLICA_STATE = RUNTIME / "api-replica-process.json"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def percentile(values, quantile):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(quantile * len(values)) - 1)]


class Client:
    """Keep one connection per thread/endpoint and never automatically retry a write."""

    def __init__(self, base, timeout=40):
        parsed = urllib.parse.urlsplit(base)
        if parsed.scheme != "http" or not parsed.hostname:
            raise ValueError("Only local HTTP endpoints are supported")
        self.base = base.rstrip("/")
        self.host, self.port = parsed.hostname, parsed.port or 80
        self.timeout = timeout
        self.local = threading.local()

    def call(self, method, path, payload=None):
        if not hasattr(self.local, "connection"):
            self.local.connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        connection = self.local.connection
        started = time.perf_counter()
        try:
            connection.request(method, path, body=None if payload is None else json.dumps(payload).encode(),
                               headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            data = response.read()
            return {"http": response.status, "body": json.loads(data),
                    "ms": (time.perf_counter() - started) * 1000}
        except Exception as error:
            connection.close()
            del self.local.connection
            return {"http": None, "body": {"error": repr(error)},
                    "ms": (time.perf_counter() - started) * 1000}


def require(reply, statuses=(200,)):
    if reply["http"] not in statuses:
        raise RuntimeError(f"Unexpected API response: {reply}")
    return reply["body"]


def command_for_pid(pid):
    query = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True)
    return query.stdout.strip() if query.returncode == 0 else None


class Replica:
    def __init__(self, jar):
        self.jar = jar
        self.process = None
        self.log = None
        self.state = None

    def start(self, run_id):
        # Refuse to repurpose an already occupied port or an unrelated process.
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", REPLICA_PORT))
            except OSError as error:
                raise RuntimeError(f"Port {REPLICA_PORT} is occupied; no process was stopped") from error
        if REPLICA_STATE.exists():
            prior = json.loads(REPLICA_STATE.read_text())
            if command_for_pid(prior["pid"]):
                raise RuntimeError("Recorded API replica PID is still alive; inspect runtime/api-replica-process.json")
            REPLICA_STATE.unlink()
        RUNTIME.mkdir(parents=True, exist_ok=True)
        self.log = (RUNTIME / "api-replica.log").open("a")
        self.log.write(f"\n[{utc_now()}] Start API scaling replica for {run_id}\n")
        self.log.flush()
        arguments = ["java", "-Xms128m", "-Xmx512m", "-jar", str(self.jar),
                     f"--server.port={REPLICA_PORT}"]
        self.process = subprocess.Popen(arguments, cwd=ROOT, stdout=self.log,
                                        stderr=subprocess.STDOUT, start_new_session=True)
        self.state = {"pid": self.process.pid, "jar": str(self.jar), "port": REPLICA_PORT,
                      "arguments": arguments, "startedAt": utc_now(), "runId": run_id}
        save_json(REPLICA_STATE, self.state)
        client = Client(REPLICA_BASE, timeout=2)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"API replica exited {self.process.returncode}; inspect runtime/api-replica.log")
            health = client.call("GET", "/lab/health")
            if health["http"] == 200 and health["body"].get("status") == "UP":
                snapshot = require(client.call("GET", f"/lab/runs/{run_id}"))
                if snapshot["workers"] != 0 or not snapshot["paused"]:
                    raise RuntimeError("Replica restored a run with active consumers or relay")
                self.state["healthyAt"] = utc_now()
                return dict(self.state)
            time.sleep(.3)
        raise RuntimeError("API replica failed to become ready within 90 seconds")

    def stop(self):
        try:
            if self.process is None:
                return
            if self.process.poll() is None:
                command = command_for_pid(self.process.pid)
                if not command or str(self.jar) not in command or f"--server.port={REPLICA_PORT}" not in command or "java" not in command:
                    raise RuntimeError("API replica PID command differs from this script's child; refusing to signal")
                self.process.send_signal(signal.SIGTERM)
                try:
                    self.process.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    command = command_for_pid(self.process.pid)
                    if not command or str(self.jar) not in command or f"--server.port={REPLICA_PORT}" not in command:
                        raise RuntimeError("API replica command changed before forced termination; refusing to signal")
                    self.process.kill()
                    self.process.wait(timeout=10)
            if self.state is not None:
                self.state["stoppedAt"] = utc_now()
                self.state["exitCode"] = self.process.returncode
            REPLICA_STATE.unlink(missing_ok=True)
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None


def drain(main, run_id, expected, timeout):
    deadline = time.monotonic() + timeout
    snapshot = None
    while time.monotonic() < deadline:
        snapshot = require(main.call("GET", f"/lab/runs/{run_id}"))
        if snapshot["issued"] + snapshot["rejected"] + snapshot.get("cancelled", 0) == expected:
            return snapshot
        time.sleep(.4)
    counts = None if snapshot is None else {key: snapshot.get(key) for key in ("received", "issued", "rejected", "pending")}
    raise RuntimeError(f"Run {run_id} did not settle after {timeout}s: {counts}")


def validate(snapshot, expected, expected_by_coupon):
    issued = [row for row in snapshot["requests"] if row["status"] == "ISSUED"]
    checks = {
        "all_requests_received": snapshot["received"] == expected,
        "all_requests_issued": snapshot["issued"] == expected,
        "independent_ledger_matches": snapshot.get("ledgerIssued") == expected,
        "no_duplicate_ledger_users": snapshot.get("ledgerDuplicateUsers") == 0,
        "unique_request_ids": len({row["requestId"] for row in issued}) == expected,
        "unique_coupon_recipients": len({(row["couponId"], row["userId"]) for row in issued}) == expected,
        "no_pending_or_rejected": snapshot["pending"] == 0 and snapshot["rejected"] == 0,
    }
    for coupon in snapshot["perCoupon"]:
        prefix = f"coupon_{coupon['couponId']}"
        count = expected_by_coupon[coupon["couponId"]]
        checks[prefix + "_issued"] = coupon["issued"] == coupon.get("ledgerIssued") == count
        checks[prefix + "_db_capacity"] = coupon["dbRemaining"] + coupon["ledgerIssued"] == coupon["total"]
        checks[prefix + "_redis_capacity"] = coupon["redisStock"] + coupon["redisUsers"] == coupon["total"]
        checks[prefix + "_redis_users"] = coupon["redisUsers"] == count
    return checks


def run_case(main, jar, args, distribution, replicas, repeat, index):
    started = utc_now()
    coupons = 20 if distribution == "uniform20" else 1
    total = args.requests + args.warmup
    record = {"caseIndex": index, "distribution": distribution, "apiJvms": replicas,
              "repeat": repeat, "startedAt": started, "passed": False,
              "configuration": {"strategy": "LUA_REDIS", "stockPerCoupon": total,
                                "coupons": coupons, "partitions": 3, "consumerWorkersDuringTiming": 0,
                                "warmupRequests": args.warmup, "measuredRequests": args.requests,
                                "concurrency": args.concurrency}}
    run_id = None
    replica = Replica(jar)
    try:
        created = require(main.call("POST", "/lab/runs", {"strategy": "LUA_REDIS", "stock": total,
                          "coupons": coupons, "partitions": 3, "workers": 0}), (200, 201))
        run_id = created["runId"]
        record.update(runId=run_id, topic=created["topic"])
        require(main.call("POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0}))
        clients = [main]
        if replicas == 2:
            record["replicaProcess"] = replica.start(run_id)
            clients.append(Client(REPLICA_BASE))
        record["endpoints"] = [client.base for client in clients]

        def send(item):
            phase, number = item
            coupon = 1 + number % coupons
            payload = {"requestId": f"{phase}-{number:06d}", "couponId": coupon,
                       "userId": f"{phase}-user-{number:06d}"}
            endpoint = number % len(clients)
            reply = clients[endpoint].call("POST", f"/lab/runs/{run_id}/requests", payload)
            sample = {"requestId": payload["requestId"], "couponId": coupon, "endpointIndex": endpoint,
                      "http": reply["http"], "status": reply["body"].get("status"), "ms": reply["ms"]}
            if reply["http"] != 202:
                sample["response"] = reply["body"]
            return sample

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            record["warmupSamples"] = list(pool.map(send, [("warmup", i) for i in range(args.warmup)]))
            begin = time.perf_counter()
            samples = list(pool.map(send, [("measured", i) for i in range(args.requests)]))
            elapsed = time.perf_counter() - begin
        record["requestSamples"] = samples
        latencies = [sample["ms"] for sample in samples]
        successes = sum(sample["http"] == 202 and sample["status"] == "PENDING" for sample in samples)
        record["measurement"] = {
            "elapsedSeconds": elapsed, "completedRequestsPerSecond": len(samples) / elapsed,
            "acceptedRequestsPerSecond": successes / elapsed, "successfulRequests": successes,
            "failedRequests": len(samples) - successes, "requestLatencyMs": {
                "min": min(latencies), "mean": statistics.fmean(latencies), "p50": percentile(latencies, .5),
                "p95": percentile(latencies, .95), "p99": percentile(latencies, .99), "max": max(latencies)}}
        before = require(main.call("GET", f"/lab/runs/{run_id}"))
        record["beforeDrain"] = {key: value for key, value in before.items() if key != "requests"}
        # A second JVM has its own in-memory Run. Stop it before resuming the main JVM
        # so there is a single known consumer owner during ledger validation.
        replica.stop()
        if replica.state is not None:
            record["replicaProcess"] = dict(replica.state)
        require(main.call("POST", f"/lab/runs/{run_id}/control", {"paused": False, "workers": 3}))
        snapshot = drain(main, run_id, total, args.drain_timeout)
        expected_by_coupon = {coupon: sum(1 for n in range(args.warmup) if 1 + n % coupons == coupon)
                              + sum(1 for n in range(args.requests) if 1 + n % coupons == coupon)
                              for coupon in range(1, coupons + 1)}
        checks = validate(snapshot, total, expected_by_coupon)
        checks.update(all_measured_http202=successes == args.requests,
                      all_warmup_http202=all(s["http"] == 202 for s in record["warmupSamples"]),
                      consumers_were_stopped=before["issued"] == 0 and before["workers"] == 0 and before["paused"],
                      all_pending_before_drain=before["pending"] == total)
        record["checks"] = checks
        record["snapshot"] = snapshot
        record["passed"] = all(checks.values())
    except Exception as error:
        record["error"] = repr(error)
    finally:
        try:
            replica.stop()
        except Exception as error:
            record.update(passed=False, cleanupError=repr(error))
        if run_id:
            cleanup = main.call("POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
            record["cleanup"] = cleanup
            if cleanup["http"] != 200:
                record["passed"] = False
        record["finishedAt"] = utc_now()
    return record


def summarize(cases):
    rows = []
    for distribution in ("hot1", "uniform20"):
        groups = {}
        for replicas in (1, 2):
            samples = [case["measurement"] for case in cases if case["distribution"] == distribution
                       and case["apiJvms"] == replicas and case["passed"]]
            groups[replicas] = samples
            rows.append({"distribution": distribution, "apiJvms": replicas, "successfulCases": len(samples),
                         "medianAcceptedRequestsPerSecond": statistics.median([s["acceptedRequestsPerSecond"] for s in samples]) if samples else None,
                         "medianCaseRequestP95Ms": statistics.median([s["requestLatencyMs"]["p95"] for s in samples]) if samples else None})
        if groups[1] and groups[2]:
            one = statistics.median(s["acceptedRequestsPerSecond"] for s in groups[1])
            two = statistics.median(s["acceptedRequestsPerSecond"] for s in groups[2])
            rows.append({"distribution": distribution, "observedTwoVsOneApiThroughputRatio": two / one})
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:28080")
    parser.add_argument("--output", type=Path, default=ROOT / "results/api-scaling.json")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20261022)
    parser.add_argument("--drain-timeout", type=float, default=180)
    args = parser.parse_args()
    if min(args.requests, args.warmup, args.concurrency, args.repeats) < 1:
        parser.error("Request, warmup, concurrency, and repeat counts must be positive")
    jars = [path.resolve() for path in (ROOT / "spring/build/libs").glob("*.jar") if not path.name.endswith("-plain.jar")]
    if len(jars) != 1:
        parser.error("Expected one bootJar; build with ./gradlew bootJar first")
    jar = jars[0]
    client = Client(args.base_url)
    require(client.call("GET", "/lab/health"))
    cases = [(distribution, replicas, repeat) for distribution in ("uniform20", "hot1")
             for replicas in (1, 2) for repeat in range(1, args.repeats + 1)]
    random.Random(args.seed).shuffle(cases)
    result = {
        "experiment": "real_api_jvm_scaling", "startedAt": utc_now(), "passed": False,
        "host": {"platform": platform.platform(), "logicalCpus": os.cpu_count()},
        "jar": str(jar), "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "seed": args.seed, "caseOrder": [list(case) for case in cases], "cases": [],
        "scope": [
            "Measures HTTP reservation plus Kafka acknowledgement with zero consumer workers during timing.",
            "All API requests synchronously write/check the shared MySQL lab_requests journal and update metadata; this is not a pure Redis benchmark.",
            "Two API JVMs share the same Redis primary, MySQL, and Kafka cluster on one host; each JVM has its own 32-connection DB pool and Kafka producer.",
            "The second JVM uses the same bootJar and -Xms128m -Xmx512m flags; the root main process is never stopped by this script.",
            "Final issuance is drained by three consumers in the main JVM only after timing and child shutdown; drain duration is not a scaling metric.",
            "Twenty warmup requests and two repeats are a small local sample, not a production capacity estimate or proof that a hotspot bottleneck was saturated.",
            "Round robin chooses one of two HTTP endpoints per request; this exercises two real processes without a separate load balancer."
        ]}
    for index, (distribution, replicas, repeat) in enumerate(cases, 1):
        case = run_case(client, jar, args, distribution, replicas, repeat, index)
        result["cases"].append(case)
        result["summary"] = summarize(result["cases"])
        save_json(args.output, result)
        print(json.dumps({"caseIndex": index, "distribution": distribution, "apiJvms": replicas,
                          "repeat": repeat, "passed": case["passed"], "measurement": case.get("measurement"),
                          "error": case.get("error")}), flush=True)
        if case.get("cleanupError"):
            break
    result["finishedAt"] = utc_now()
    result["passed"] = len(result["cases"]) == len(cases) and all(case["passed"] for case in result["cases"])
    save_json(args.output, result)
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
