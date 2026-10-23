#!/usr/bin/env python3
"""Repeatable, standard-library-only experiments against the real coupon lab API.

Run: python3 scripts/suite.py --base-url http://localhost:28080 \
       --output results/local --scenario all

Each case saves actual HTTP observations and snapshots before the next case starts.
Expected failure demonstrations are separate from safety invariants. An unexpected
transport error, missing result, timeout, or failed observation makes the exit code 1.
This script does not start services, silently retry writes, or manufacture samples.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import gzip
import hashlib
import http.client
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import sys
import threading
import time
import traceback
import urllib.parse


STRATEGIES = ("SPLIT_REDIS", "LUA_REDIS", "KAFKA_FIRST", "DB_OUTBOX", "REDIS_STREAM")
TERMINAL = {"ISSUED", "REJECTED", "CANCELLED"}
REDIS_STRATEGIES = {"SPLIT_REDIS", "LUA_REDIS", "KAFKA_FIRST", "REDIS_STREAM"}


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def percentile(values, quantile):
    """Nearest rank: sorted_values[ceil(q * N) - 1]; no interpolation."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def describe(values):
    return {
        "samples": len(values),
        "min": min(values) if values else None,
        "mean": statistics.fmean(values) if values else None,
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values) if values else None,
    }


def epoch_ms(value):
    if value is None:
        return None
    if isinstance(value, (float, int)):
        # The lab uses milliseconds; also accept an ISO timestamp.
        return float(value)
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    serialized = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    temporary.write_bytes(gzip.compress(serialized, mtime=0) if path.suffix == ".gz" else serialized)
    temporary.replace(path)


class ExperimentError(RuntimeError):
    pass


class Client:
    """One persistent HTTP connection per thread; no automatic write retries."""

    def __init__(self, base_url, timeout):
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("--base-url must be an http(s) URL")
        self.parsed = parsed
        self.timeout = timeout
        self.local = threading.local()

    def call(self, method, path, payload=None):
        if not hasattr(self.local, "connection"):
            cls = http.client.HTTPSConnection if self.parsed.scheme == "https" else http.client.HTTPConnection
            self.local.connection = cls(self.parsed.hostname, self.parsed.port, timeout=self.timeout)
        started_at = utc_now()
        started = time.perf_counter_ns()
        event = {"method": method, "path": path, "payload": payload, "startedAt": started_at}
        try:
            body = None if payload is None else json.dumps(payload).encode()
            self.local.connection.request(
                method, self.parsed.path.rstrip("/") + path, body,
                {"Content-Type": "application/json", "Accept": "application/json"},
            )
            response = self.local.connection.getresponse()
            raw = response.read().decode("utf-8", errors="replace")
            event["status"] = response.status
            try:
                event["response"] = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                event["response"] = raw
        except (OSError, http.client.HTTPException) as error:
            event.update(status=0, response=None, error=f"{type(error).__name__}: {error}")
            self.local.connection.close()
            del self.local.connection
        event["durationMs"] = (time.perf_counter_ns() - started) / 1_000_000
        event["finishedAt"] = utc_now()
        return event


class Case:
    def __init__(self, runner, name):
        self.runner = runner
        self.data = {
            "name": name, "startedAt": utc_now(), "parameters": {}, "http": [],
            "snapshots": [], "checks": [], "expectedObservations": [], "metrics": {},
        }
        self.run_id = None
        self.strategy = None
        self.measured_request_prefix = None
        self.lock = threading.Lock()

    def check(self, name, passed, actual=None, expected=None, observation=False):
        target = "expectedObservations" if observation else "checks"
        self.data[target].append({"name": name, "passed": bool(passed), "actual": actual, "expected": expected})
        return bool(passed)

    def call(self, method, path, payload=None, allowed=(200, 202, 409)):
        event = self.runner.client.call(method, path, payload)
        with self.lock:
            self.data["http"].append(event)
            if event["status"] not in allowed:
                self.check("http_response_allowed", False,
                           {"status": event["status"], "path": path, "response": event["response"], "error": event.get("error")},
                           list(allowed))
        return event

    def create(self, strategy, *, stock=100, coupons=1, partitions=3, workers=3, ordering="BLOCKING"):
        self.strategy = strategy
        config = dict(strategy=strategy, stock=stock, coupons=coupons, partitions=partitions, workers=workers, ordering=ordering)
        self.data["parameters"].update(config)
        event = self.call("POST", "/lab/runs", config, (200, 201, 202))
        if event["status"] not in (200, 201, 202) or not isinstance(event["response"], dict) or not event["response"].get("runId"):
            raise ExperimentError("Run creation failed; see recorded HTTP response")
        self.run_id = event["response"]["runId"]
        self.data["run"] = event["response"]
        return self

    @property
    def path(self):
        return f"/lab/runs/{self.run_id}"

    def submit(self, request_id, user_id=None, coupon_id=1, allowed=(200, 202, 409), **kwargs):
        payload = dict(requestId=request_id, userId=user_id or request_id, couponId=coupon_id, **kwargs)
        return self.call("POST", self.path + "/requests", payload, allowed)

    def bulk(self, payloads):
        started = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.runner.args.concurrency) as pool:
            events = list(pool.map(lambda payload: self.call("POST", self.path + "/requests", payload), payloads))
        self.data["metrics"]["submissionWallMs"] = (time.perf_counter() - started) * 1000
        return events

    def control(self, **kwargs):
        event = self.call("POST", self.path + "/control", kwargs, (200, 202))
        if event["status"] not in (200, 202):
            raise ExperimentError("Consumer control failed")
        return event["response"]

    def snapshot(self, label=None):
        event = self.call("GET", self.path, allowed=(200,))
        if event["status"] != 200 or not isinstance(event["response"], dict):
            raise ExperimentError("Snapshot retrieval failed")
        snapshot = event["response"]
        if label:
            self.data["snapshots"].append({"label": label, "observedAt": utc_now(), "value": snapshot})
        # Avoid duplicating every full polling snapshot in the HTTP trace.
        event["response"] = {"snapshotPoll": True, "received": snapshot.get("received"), "pending": snapshot.get("pending"), "issued": snapshot.get("issued")}
        return snapshot

    def poll(self, predicate, *, label="settled", timeout=None):
        started = time.monotonic()
        deadline = started + (self.runner.args.settle_timeout if timeout is None else timeout)
        while True:
            snapshot = self.snapshot()
            if predicate(snapshot):
                self.data["snapshots"].append({"label": label, "observedAt": utc_now(), "value": snapshot})
                self.data["metrics"][f"{label}WaitMs"] = (time.monotonic() - started) * 1000
                return snapshot
            if time.monotonic() >= deadline:
                self.data["snapshots"].append({"label": label + "_timeout", "observedAt": utc_now(), "value": snapshot})
                self.check(label + "_within_timeout", False, snapshot.get("pending"), "predicate satisfied")
                return snapshot
            time.sleep(0.15)

    def settled(self, count):
        return self.poll(lambda s: len(s.get("requests", [])) >= count and all(r.get("status") in TERMINAL for r in s.get("requests", [])))

    def metrics(self, snapshot):
        submissions = [e for e in self.data["http"] if e["method"] == "POST" and e["path"].endswith("/requests")]
        if self.measured_request_prefix:
            submissions = [e for e in submissions if e["payload"]["requestId"].startswith(self.measured_request_prefix)]
        self.data["metrics"]["apiLatencyMs"] = describe([e["durationMs"] for e in submissions])
        status_counts = {}
        for event in submissions:
            key = str(event["status"])
            status_counts[key] = status_counts.get(key, 0) + 1
        self.data["metrics"]["httpStatusCounts"] = status_counts
        issued_latencies, terminal_latencies, distribution = [], [], {}
        for request in snapshot.get("requests", []):
            if self.measured_request_prefix and not request["requestId"].startswith(self.measured_request_prefix):
                continue
            if request.get("partition") is not None and request["partition"] >= 0:
                key = str(request["partition"])
                distribution[key] = distribution.get(key, 0) + 1
            completed = epoch_ms(request.get("completedAt"))
            received = epoch_ms(request.get("receivedAt"))
            if completed is not None and received is not None and completed >= received and request.get("status") in TERMINAL:
                terminal_latencies.append(completed - received)
                if request.get("status") == "ISSUED":
                    issued_latencies.append(completed - received)
        self.data["metrics"]["issuedEndToEndLatencyMs"] = describe(issued_latencies)
        self.data["metrics"]["allTerminalEndToEndLatencyMs"] = describe(terminal_latencies)
        self.data["metrics"]["partitionRequestCounts"] = distribution
        self.data["metrics"]["issued"] = len(issued_latencies)
        self.data["metrics"]["totalRunIssuedIncludingWarmup"] = snapshot.get("issued")

    def invariants(self, snapshot, *, expected_issued=None, redis_consistent=True, complete=True):
        coupons = snapshot.get("perCoupon", [])
        self.check("coupon_snapshot_present", len(coupons) == self.data["parameters"]["coupons"], len(coupons), self.data["parameters"]["coupons"])
        for coupon in coupons:
            cid = coupon["couponId"]
            issued, total = coupon["issued"], coupon["total"]
            self.check(f"coupon_{cid}_no_oversell", 0 <= issued <= total, issued, f"0..{total}")
            self.check(f"coupon_{cid}_db_counter_conservation", coupon["dbRemaining"] + issued == total,
                       {"dbRemaining": coupon["dbRemaining"], "issued": issued}, total)
            self.check(f"coupon_{cid}_db_stock_within_bounds", 0 <= coupon["dbRemaining"] <= total,
                       coupon["dbRemaining"], f"0..{total}")
            if "ledgerIssued" in coupon:
                self.check(f"coupon_{cid}_request_state_matches_final_ledger", coupon["ledgerIssued"] == issued,
                           coupon["ledgerIssued"], issued)
            if self.strategy in REDIS_STRATEGIES:
                stock, users = coupon.get("redisStock"), coupon.get("redisUsers")
                self.check(f"coupon_{cid}_redis_stock_within_bounds", isinstance(stock, (int, float)) and 0 <= stock <= total,
                           stock, f"0..{total}")
                self.check(f"coupon_{cid}_redis_users_within_bounds", isinstance(users, (int, float)) and 0 <= users <= total,
                           users, f"0..{total}")
            if redis_consistent and self.strategy in REDIS_STRATEGIES:
                self.check(f"coupon_{cid}_redis_counter_conservation", coupon["redisStock"] + coupon["redisUsers"] == total,
                           {"redisStock": coupon["redisStock"], "redisUsers": coupon["redisUsers"]}, total)
        rows = snapshot.get("requests", [])
        issued_rows = [r for r in rows if r.get("status") == "ISSUED"]
        recipients = [(r["couponId"], r["userId"]) for r in issued_rows]
        self.check("no_duplicate_issued_user_per_coupon", len(set(recipients)) == len(recipients), len(recipients), len(set(recipients)))
        self.check("request_ids_unique", len({r["requestId"] for r in rows}) == len(rows), len(rows), len({r["requestId"] for r in rows}))
        self.check("issued_count_matches_requests", snapshot.get("issued") == len(issued_rows), snapshot.get("issued"), len(issued_rows))
        if "ledgerIssued" in snapshot:
            self.check("issued_count_matches_final_database_ledger", snapshot["ledgerIssued"] == snapshot.get("issued"),
                       snapshot["ledgerIssued"], snapshot.get("issued"))
        if "ledgerDuplicateUsers" in snapshot:
            self.check("no_duplicate_users_in_final_database_ledger", snapshot["ledgerDuplicateUsers"] == 0,
                       snapshot["ledgerDuplicateUsers"], 0)
        if expected_issued is not None:
            self.check("expected_issued", snapshot.get("issued") == expected_issued, snapshot.get("issued"), expected_issued)
        if complete:
            pending = [r["requestId"] for r in rows if r.get("status") not in TERMINAL]
            self.check("all_requests_terminal", not pending, pending, [])
        self.metrics(snapshot)


class Runner:
    def __init__(self, args):
        self.args = args
        self.client = Client(args.base_url, args.http_timeout)
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        source_path = Path(__file__).resolve()
        jar_paths = sorted((source_path.parent.parent / "spring" / "build" / "libs").glob("*.jar"))
        jar_hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in jar_paths if not p.name.endswith("-plain.jar")}
        self.report = {
            "schemaVersion": 1, "startedAt": utc_now(), "finishedAt": None,
            "environment": {"python": sys.version, "platform": platform.platform(), "machine": platform.machine(), "cpuCount": os.cpu_count(), "baseUrl": args.base_url,
                            "suiteSha256": hashlib.sha256(source_path.read_bytes()).hexdigest(), "backendJarFilesSha256": jar_hashes},
            "parameters": vars(args), "cases": [],
            "measurementNotes": [
                "Actual HTTP traffic to Spring, Kafka, Redis and MySQL; no simulated service responses.",
                "All strategies share the lab's database unique constraint and conditional stock decrement as a final safety guard. These results do not prove the original examples safe.",
                "API duration uses perf_counter_ns on the load generator and includes network and client work; one persistent HTTP connection per worker thread.",
                "Final issuance duration is server completedAt minus receivedAt for ISSUED requests; API rejection time is not treated as issuance time.",
                "Server receivedAt is the request-log insertion timestamp in the lab, not a globally ordered TCP/API gateway arrival timestamp; publishDelayMs demonstrates divergence from Kafka append order.",
                "Percentiles use nearest rank ceil(q*N)-1 without interpolation; absent observations are null, never zero.",
                "Localhost load, finite samples and one process are not production throughput or HA guarantees; no latency performance thresholds are asserted.",
                "Fault hooks are explicitly named and enabled only in the isolated lab. Broker/Redis process failures are separate infrastructure experiments.",
                "Consumers and relay are paused, and workers are stopped after each case. Topics and durable data are retained for inspection until infrastructure cleanup.",
                "Each distribution case warms the new consumer group with 20 requests, one per coupon; measured latencies and partition counts exclude those requests.",
                "Distribution case order is shuffled with the recorded seed to reduce systematic time-order bias; each combination is sampled once, with no statistical significance claim.",
            ],
        }

    def case(self, name, function):
        if self.args.case_filter and self.args.case_filter not in name:
            return
        print(f"RUN {name}", flush=True)
        case = Case(self, name)
        try:
            function(case)
        except Exception as error:
            case.data["error"] = {"type": type(error).__name__, "message": str(error), "traceback": traceback.format_exc()}
            case.check("unexpected_exception", False, str(error), None)
        finally:
            if case.run_id:
                try:
                    case.control(paused=True, workers=0)
                except Exception as error:
                    case.check("stop_consumers", False, str(error), "workers=0")
            case.data["finishedAt"] = utc_now()
            checks = case.data["checks"] + case.data["expectedObservations"]
            failed = [c for c in checks if not c["passed"]]
            case.data["passed"] = bool(checks) and not failed
            index = len(self.report["cases"]) + 1
            filename = f"{index:03d}-{name.lower()}.json"
            if self.args.gzip_cases:
                filename += ".gz"
            save_json(self.output / filename, case.data)
            self.report["cases"].append({"name": name, "file": filename, "passed": case.data["passed"], "failedChecks": failed, "metrics": case.data["metrics"], "run": case.data.get("run"), "parameters": case.data["parameters"]})
            self.write_summary()
            print(f"{'PASS' if case.data['passed'] else 'FAIL'} {name} ({len(checks)} checks, {len(failed)} failed)", flush=True)

    def write_summary(self):
        self.report["passedCases"] = sum(c["passed"] for c in self.report["cases"])
        self.report["failedCases"] = sum(not c["passed"] for c in self.report["cases"])
        save_json(self.output / "summary.json", self.report)


def payload(request_id, *, coupon=1, user=None, **kwargs):
    return dict(requestId=request_id, couponId=coupon, userId=user or request_id, **kwargs)


def correctness(runner):
    for strategy in runner.args.strategies:
        def saturation(case, strategy=strategy):
            case.create(strategy, stock=100)
            case.bulk([payload(f"load-{i}") for i in range(runner.args.users)])
            snapshot = case.settled(runner.args.users)
            case.invariants(snapshot, expected_issued=min(100, runner.args.users))
        runner.case(f"correctness-saturation-{strategy}", saturation)

        def duplicates(case, strategy=strategy):
            case.create(strategy, stock=10)
            case.bulk([payload(f"duplicate-{i}", user="same-user") for i in range(64)])
            snapshot = case.settled(64)
            case.invariants(snapshot, expected_issued=1)
        runner.case(f"correctness-duplicate-user-{strategy}", duplicates)

        def idempotency(case, strategy=strategy):
            case.create(strategy, stock=10, coupons=2)
            case.control(paused=True)
            first = case.submit("stable-request", "stable-user")
            case.check("initial_request_accepted", first["status"] == 202, first["status"], 202)
            case.bulk([payload("stable-request", user="stable-user") for _ in range(32)])
            pending = case.snapshot("concurrent_retries_while_pending")
            case.check("concurrent_retries_keep_one_request", len(pending.get("requests", [])) == 1,
                       len(pending.get("requests", [])), 1)
            case.control(paused=False)
            snapshot = case.settled(1)
            replay = case.submit("stable-request", "stable-user")
            case.check("same_request_replay_200", replay["status"] == 200, replay["status"], 200)
            conflict = case.submit("stable-request", "other-user", coupon_id=2)
            case.check("conflicting_payload_409", conflict["status"] == 409, conflict["status"], 409)
            status = case.call("GET", case.path + "/requests/stable-request", allowed=(200,))
            case.check("individual_poll_returns_issued", isinstance(status["response"], dict) and status["response"].get("status") == "ISSUED", status["response"], "ISSUED")
            snapshot = case.snapshot("after_replays")
            case.check("one_request_record", len(snapshot.get("requests", [])) == 1, len(snapshot.get("requests", [])), 1)
            case.invariants(snapshot, expected_issued=1)
        runner.case(f"correctness-request-idempotency-{strategy}", idempotency)

        def identity(case, strategy=strategy):
            case.create(strategy, stock=10)
            identities = [("Case", "Alice"), ("case", "alice"), ("Café", "résumé"), ("Cafe", "resume"),
                          ("Trailing", "user"), ("Trailing ", "user ")]
            case.bulk([payload(request_id, user=user_id) for request_id, user_id in identities])
            snapshot = case.settled(len(identities))
            case.check("case_accent_and_trailing_space_identity_preserved",
                       {(r["requestId"], r["userId"]) for r in snapshot.get("requests", [])} == set(identities),
                       [[r["requestId"], r["userId"]] for r in snapshot.get("requests", [])], identities)
            conflict = case.submit("Case", "alice")
            case.check("exact_request_id_still_rejects_payload_conflict", conflict["status"] == 409,
                       conflict["status"], 409)
            case.invariants(case.snapshot("identity_after_conflict"), expected_issued=len(identities))
        runner.case(f"correctness-case-and-accent-sensitive-identity-{strategy}", identity)


def faults(runner):
    if "SPLIT_REDIS" in runner.args.strategies:
        def split_gap(case):
            case.create("SPLIT_REDIS", stock=10)
            event = case.submit("split-add", "victim", fault="AFTER_ADD", allowed=(503,))
            case.check("injected_failure_visible", event["status"] == 503, event["status"], 503)
            snapshot = case.snapshot("after_sadd_before_decr")
            coupon = snapshot["perCoupon"][0]
            case.check("split_operations_leave_user_without_decrement", coupon["redisStock"] == 10 and coupon["redisUsers"] == 1,
                       coupon, "redisStock=10, redisUsers=1", observation=True)
            case.submit("split-new-attempt", "victim")
            snapshot = case.snapshot("same_user_retry_new_request")
            case.check("orphaned_user_blocks_issue", snapshot["issued"] == 0, snapshot["issued"], 0, observation=True)
            case.invariants(snapshot, expected_issued=0, redis_consistent=False, complete=False)
        runner.case("fault-split-after-sadd", split_gap)

    for strategy in runner.args.strategies:
        def reserve_gap(case, strategy=strategy):
            case.create(strategy, stock=10)
            case.submit("reserve-gap", fault="AFTER_RESERVE", allowed=(202, 503))
            if strategy in {"SPLIT_REDIS", "LUA_REDIS"}:
                time.sleep(0.5)
                snapshot = case.snapshot("unpublished_reservation")
                coupon = snapshot["perCoupon"][0]
                case.check("reservation_publication_gap_reproduced", coupon["redisStock"] == 9 and snapshot["issued"] == 0,
                           coupon, "redisStock=9, issued=0", observation=True)
                case.check("request_remains_pending_without_durable_relay", snapshot.get("pending") == 1, snapshot.get("pending"), 1, observation=True)
                case.invariants(snapshot, expected_issued=0, complete=False)
                case.submit("reserve-gap")
                recovered = case.settled(1)
                case.check("explicit_same_request_retry_resumes_publication", recovered.get("issued") == 1,
                           recovered.get("issued"), 1, observation=True)
                case.invariants(recovered, expected_issued=1)
            else:
                snapshot = case.settled(1)
                case.check("durable_path_recovers_reserve_gap", snapshot.get("issued") == 1, snapshot.get("issued"), 1, observation=True)
                case.invariants(snapshot, expected_issued=1)
            request = next((r for r in snapshot.get("requests", []) if r["requestId"] == "reserve-gap"), {})
            case.check("after_reserve_fault_was_triggered", request.get("faultConsumed") is True, request.get("faultConsumed"), True)
        runner.case(f"fault-after-reserve-{strategy}", reserve_gap)

        def send_ack(case, strategy=strategy):
            case.create(strategy, stock=10)
            case.submit("unknown-outcome", fault="AFTER_SEND", allowed=(202, 503))
            snapshot = case.settled(1)
            replay = case.submit("unknown-outcome")
            case.check("client_retry_returns_existing_result", replay["status"] == 200, replay["status"], 200)
            case.call("POST", case.path + "/replay", {"requestId": "unknown-outcome"}, (200, 202))
            before = snapshot.get("replays", 0)
            snapshot = case.poll(lambda s: s.get("replays", 0) > before, label="duplicate_delivery_handled")
            request = next((r for r in snapshot.get("requests", []) if r["requestId"] == "unknown-outcome"), {})
            case.check("after_send_fault_was_triggered", request.get("faultConsumed") is True, request.get("faultConsumed"), True)
            case.invariants(snapshot, expected_issued=1)
        runner.case(f"fault-after-send-and-redelivery-{strategy}", send_ack)

        def db_commit(case, strategy=strategy):
            case.create(strategy, stock=10)
            case.submit("committed-before-ack", fault="AFTER_DB", allowed=(202, 503))
            snapshot = case.poll(lambda s: s.get("issued") == 1 and s.get("replays", 0) >= 1, label="database_commit_redelivery")
            case.check("db_commit_before_offset_ack_replayed", snapshot.get("replays", 0) >= 1, snapshot.get("replays"), ">=1", observation=True)
            case.check("consumer_fault_observed", snapshot.get("consumerErrors", 0) >= 1, snapshot.get("consumerErrors"), ">=1")
            case.invariants(snapshot, expected_issued=1)
        runner.case(f"fault-after-db-before-offset-commit-{strategy}", db_commit)

        def paused(case, strategy=strategy):
            case.create(strategy, stock=20)
            case.control(paused=True)
            case.bulk([payload(f"pending-{i}") for i in range(20)])
            snapshot = case.snapshot("consumer_paused")
            case.check("accepted_does_not_mean_issued", snapshot.get("issued") == 0 and snapshot.get("pending") == 20,
                       {"issued": snapshot.get("issued"), "pending": snapshot.get("pending")}, {"issued": 0, "pending": 20}, observation=True)
            status = case.call("GET", case.path + "/requests/pending-0", allowed=(200,))
            case.check("individual_poll_shows_pending", isinstance(status["response"], dict) and status["response"].get("status") not in TERMINAL,
                       status["response"], "RECEIVED|RESERVED|PENDING")
            case.control(paused=False)
            snapshot = case.settled(20)
            case.invariants(snapshot, expected_issued=20)
        runner.case(f"fault-pause-poll-resume-{strategy}", paused)

        def metadata_lookup(case, strategy=strategy):
            case.create(strategy, stock=10, partitions=1, workers=1)
            case.control(paused=True)
            case.submit("lookup-fails-first", fault="READ_BEFORE_HANDLER")
            case.submit("after-lookup-failure")
            case.control(paused=False)
            snapshot = case.settled(2)
            rows = {r["requestId"]: r for r in snapshot.get("requests", [])}
            first, second = rows["lookup-fails-first"], rows["after-lookup-failure"]
            case.check("metadata_lookup_fault_triggered", first.get("faultConsumed") is True,
                       first.get("faultConsumed"), True)
            case.check("metadata_lookup_failure_observed", snapshot.get("consumerErrors", 0) >= 1,
                       snapshot.get("consumerErrors"), ">=1")
            case.check("failed_metadata_record_retried_before_next_record", first["completionSeq"] < second["completionSeq"],
                       [first["completionSeq"], second["completionSeq"]], "first < second")
            case.invariants(snapshot, expected_issued=2)
        runner.case(f"fault-metadata-lookup-before-handler-{strategy}", metadata_lookup)

    if "DB_OUTBOX" in runner.args.strategies:
        def cancel(case):
            case.create("DB_OUTBOX", stock=10)
            case.control(paused=True)
            case.submit("cancel-me")
            case.call("POST", case.path + "/cancel", {"requestId": "cancel-me"}, (200, 202))
            snapshot = case.snapshot("cancelled_before_publication")
            case.check("cancel_restores_reservation", snapshot.get("cancelled") == 1 and snapshot["perCoupon"][0]["dbRemaining"] == 10,
                       snapshot["perCoupon"][0], "cancelled=1, dbRemaining=10")
            case.control(paused=False)
            time.sleep(0.3)
            snapshot = case.snapshot("cancelled_stays_unissued")
            case.invariants(snapshot, expected_issued=0)
        runner.case("fault-outbox-atomic-cancel", cancel)


def ordering(runner):
    def append_order(case):
        case.create("KAFKA_FIRST", stock=10, partitions=1, workers=1)
        case.control(paused=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(case.submit, "arrived-first", publishDelayMs=700)
            case.poll(lambda s: any(r["requestId"] == "arrived-first" for r in s.get("requests", [])), label="first_request_received")
            second = pool.submit(case.submit, "arrived-second")
            first.result()
            second.result()
        case.control(paused=False)
        snapshot = case.settled(2)
        rows = {r["requestId"]: r for r in snapshot["requests"]}
        first, second = rows["arrived-first"], rows["arrived-second"]
        case.check("first_api_received_no_later", epoch_ms(first["receivedAt"]) <= epoch_ms(second["receivedAt"]), [first["receivedAt"], second["receivedAt"]], "first <= second")
        case.check("api_arrival_and_kafka_append_can_differ", first["offset"] > second["offset"], [first["offset"], second["offset"]], "first offset > second offset", observation=True)
        case.check("blocking_completion_follows_append", second["completionSeq"] < first["completionSeq"], [second["completionSeq"], first["completionSeq"]], "second completes first")
        case.invariants(snapshot, expected_issued=2)
    runner.case("ordering-arrival-versus-kafka-append", append_order)

    for mode in ("BLOCKING", "RETRY_LATER"):
        def processing(case, mode=mode):
            case.create("KAFKA_FIRST", stock=10, partitions=1, workers=3, ordering=mode)
            case.control(paused=True)
            case.submit("slow-first", processingDelayMs=1000)
            case.submit("fast-second")
            case.control(paused=False)
            snapshot = case.settled(2)
            rows = {r["requestId"]: r for r in snapshot["requests"]}
            first, second = rows["slow-first"], rows["fast-second"]
            case.check("first_appended_first", first["offset"] < second["offset"], [first["offset"], second["offset"]], "first < second")
            ordered = first["completionSeq"] < second["completionSeq"]
            case.check("blocking_preserves_or_retry_later_reorders", ordered == (mode == "BLOCKING"),
                       [first["completionSeq"], second["completionSeq"]], "first < second" if mode == "BLOCKING" else "second < first", observation=mode == "RETRY_LATER")
            case.invariants(snapshot, expected_issued=2)
        runner.case(f"ordering-delayed-first-{mode}", processing)

    def rebalance(case):
        count = runner.args.rebalance_requests
        case.create("KAFKA_FIRST", stock=count, coupons=6, partitions=3, workers=1)
        case.control(paused=True)
        case.bulk([payload(f"rebalance-{i}", coupon=i % 6 + 1, processingDelayMs=8) for i in range(count)])
        case.control(paused=False)
        case.poll(lambda s: 0 < s.get("issued", 0) < count, label="in_flight_before_rebalance")
        case.control(workers=3)
        snapshot = case.settled(count)
        case.invariants(snapshot, expected_issued=count)
        evidence = {}
        for partition in range(3):
            rows = sorted((r for r in snapshot["requests"] if r.get("partition") == partition and r.get("status") == "ISSUED"), key=lambda r: r["offset"])
            completed = [epoch_ms(r["completedAt"]) for r in rows]
            evidence[str(partition)] = [{"requestId": r["requestId"], "couponId": r["couponId"], "offset": r["offset"], "couponCompletionSeq": r["completionSeq"], "completedAt": r["completedAt"]} for r in rows]
            case.check(f"partition_{partition}_completion_time_follows_offsets", completed == sorted(completed), completed, "nondecreasing completion timestamps by offset (millisecond resolution)")
        for coupon in range(1, 7):
            rows = sorted((r for r in snapshot["requests"] if r["couponId"] == coupon and r.get("status") == "ISSUED"), key=lambda r: r["completionSeq"])
            offsets = [r["offset"] for r in rows]
            case.check(f"coupon_{coupon}_completion_offset_order", offsets == sorted(offsets), offsets, "ascending offsets within coupon")
        case.data["metrics"]["rebalanceOrderEvidence"] = evidence
    runner.case("ordering-rebalance-while-processing", rebalance)


def distribution(runner):
    combinations = list(itertools.product(
        runner.args.strategies, ("uniform20", "hot90", "hot100"), runner.args.partitions, runner.args.workers,
    ))
    random.Random(runner.args.seed).shuffle(combinations)
    for strategy, workload, partitions, workers in combinations:
        def sample(case, strategy=strategy, workload=workload, partitions=partitions, workers=workers):
            count = runner.args.distribution_requests
            coupons = 20
            case.create(strategy, stock=count + coupons, coupons=coupons, partitions=partitions, workers=workers)
            case.bulk([payload(f"warmup-{i}", coupon=i + 1) for i in range(coupons)])
            warmup = case.settled(coupons)
            case.check("warmup_completed", warmup.get("issued") == coupons, warmup.get("issued"), coupons)
            case.measured_request_prefix = "distribution-"
            randomizer = random.Random(runner.args.seed)
            if workload == "uniform20":
                ids = [i % coupons + 1 for i in range(count)]
            elif workload == "hot90":
                hot_count = round(count * 0.9)
                ids = [1] * hot_count + [i % (coupons - 1) + 2 for i in range(count - hot_count)]
            else:
                ids = [1] * count
            randomizer.shuffle(ids)
            case.data["parameters"].update(workload=workload, requests=count, warmupRequests=coupons, seed=runner.args.seed, concurrency=runner.args.concurrency)
            started = time.perf_counter()
            case.bulk([payload(f"distribution-{i}", coupon=cid) for i, cid in enumerate(ids)])
            snapshot = case.settled(count + coupons)
            wall = time.perf_counter() - started
            case.invariants(snapshot, expected_issued=count + coupons)
            case.data["metrics"]["submissionThroughSettlementWallMs"] = wall * 1000
            case.data["metrics"]["issuedPerWallSecond"] = max(0, snapshot.get("issued", 0) - coupons) / wall
            case.data["metrics"]["couponRequestCounts"] = {str(cid): ids.count(cid) for cid in range(1, coupons + 1)}
            if workload == "hot100":
                observed = case.data["metrics"]["partitionRequestCounts"]
                case.check("one_coupon_uses_one_partition", len(observed) == 1, observed, "one nonempty partition", observation=True)
        runner.case(f"distribution-{strategy}-{workload}-p{partitions}-w{workers}", sample)


def arguments():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://localhost:28080")
    parser.add_argument("--output", required=True, help="New or existing directory; case JSON and summary.json are written here")
    parser.add_argument("--gzip-cases", action="store_true", help="Compress full individual case JSON; summary.json remains plain JSON")
    parser.add_argument("--scenario", choices=("correctness", "faults", "ordering", "distribution", "all"), default="all")
    parser.add_argument("--case-filter", help="Only run case names containing this substring, for focused reproduction")
    parser.add_argument("--strategies", nargs="+", choices=STRATEGIES, default=list(STRATEGIES))
    parser.add_argument("--users", type=int, default=1000, help="Distinct users in each correctness saturation experiment")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--distribution-requests", type=int, default=300)
    parser.add_argument("--rebalance-requests", type=int, default=300)
    parser.add_argument("--partitions", nargs="+", type=int, default=[1, 3, 6])
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 3, 6])
    parser.add_argument("--seed", type=int, default=20261022)
    parser.add_argument("--http-timeout", type=float, default=30.0)
    parser.add_argument("--settle-timeout", type=float, default=60.0)
    args = parser.parse_args()
    for field in ("users", "concurrency", "distribution_requests", "rebalance_requests", "http_timeout", "settle_timeout"):
        if getattr(args, field) <= 0:
            parser.error(f"--{field.replace('_', '-')} must be positive")
    if any(n <= 0 for n in args.partitions + args.workers):
        parser.error("--partitions and --workers values must be positive")
    return args


def main():
    args = arguments()
    runner = Runner(args)
    runner.write_summary()
    scenarios = {"correctness": correctness, "faults": faults, "ordering": ordering, "distribution": distribution}
    try:
        for name, function in scenarios.items():
            if args.scenario in (name, "all"):
                function(runner)
    except KeyboardInterrupt:
        runner.report["interrupted"] = True
    finally:
        runner.report["finishedAt"] = utc_now()
        runner.write_summary()
    print(json.dumps({"summary": str(runner.output / "summary.json"), "passedCases": runner.report["passedCases"], "failedCases": runner.report["failedCases"]}), flush=True)
    return 1 if runner.report["failedCases"] or runner.report.get("interrupted") or not runner.report["cases"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
