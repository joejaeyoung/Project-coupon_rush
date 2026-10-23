#!/usr/bin/env python3
"""Observe all five request paths during a real outage of two isolated Kafka nodes.

Create topics/runs while healthy, stop only coupon-lab kafka2/kafka3, submit once to
each strategy, inspect durable reservation state, restore the brokers, and retry
uncertain direct publications with the same request IDs. This script never resets
MySQL or Redis and always attempts to restore brokers and stop its run workers.
"""

import argparse
import concurrent.futures
from datetime import datetime, timezone
import json
from pathlib import Path
import runpy
import time


ROOT = Path(__file__).resolve().parent
SUPPORT = runpy.run_path(str(ROOT / "ha-experiments.py"), run_name="coupon_outage_support")
api, infra, command = SUPPORT["api"], SUPPORT["infra"], SUPPORT["command"]
COMPOSE = SUPPORT["COMPOSE"]
STRATEGIES = ("SPLIT_REDIS", "LUA_REDIS", "KAFKA_FIRST", "DB_OUTBOX", "REDIS_STREAM")
DIRECT = {"SPLIT_REDIS", "LUA_REDIS", "KAFKA_FIRST"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def save(path, result):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def require(reply, statuses=(200,)):
    if reply["http"] not in statuses:
        raise AssertionError(f"Unexpected API response: {reply}")
    return reply["body"]


def wait_issued(base, case, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = require(api(base, "GET", f"/lab/runs/{case['runId']}"))
        if (last["issued"] == 1 and last["pending"] == 0 and last.get("ledgerIssued") == 1
                and last["outboxPending"] == 0 and last["streamPending"] == 0):
            return last
        time.sleep(.3)
    raise AssertionError(f"{case['strategy']} did not drain after {timeout}s: {last}")


def submit(base, case):
    return api(base, "POST", f"/lab/runs/{case['runId']}/requests", case["payload"])


def observe(args, result):
    cases = result["cases"]
    try:
        result["initialBrokerRestore"] = infra("kafka-start", "all")
        for strategy in STRATEGIES:
            created = require(api(args.base_url, "POST", "/lab/runs", {
                "strategy": strategy, "stock": 5, "coupons": 1, "partitions": 3,
                "workers": 1, "ordering": "BLOCKING"}), (200, 201))
            case = {"strategy": strategy, "runId": created["runId"], "topic": created["topic"],
                    "payload": {"requestId": "actual-kafka-outage", "couponId": 1,
                                "userId": "actual-kafka-outage-user"}, "passed": False}
            cases.append(case)
            # Capture the actual broker configuration, not just the API's defaults.
            case["topicBeforeOutage"] = command(*COMPOSE, "exec", "-T", "kafka1",
                "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka1:9092",
                "--describe", "--topic", case["topic"])
            case["beforeOutage"] = require(api(args.base_url, "GET", f"/lab/runs/{case['runId']}"))
        save(args.output, result)
        result["outageStartedAt"] = utc_now()
        result["stopOutput"] = command(*COMPOSE, "stop", "-t", "2", "kafka2", "kafka3")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(cases)) as pool:
            futures = {pool.submit(submit, args.base_url, case): case for case in cases}
            for future in concurrent.futures.as_completed(futures):
                case = futures[future]
                try:
                    case["duringOutageSubmission"] = future.result()
                except Exception as error:
                    case["submissionTransportError"] = repr(error)
        for case in cases:
            case["duringOutage"] = require(api(args.base_url, "GET", f"/lab/runs/{case['runId']}"))
        save(args.output, result)
        result["brokerRestore"] = infra("kafka-start", "all")
        result["brokersRestoredAt"] = utc_now()
        # Durable Outbox and Stream work must recover without a client retry.
        for case in cases:
            if case["strategy"] not in DIRECT:
                case["automaticRecovery"] = wait_issued(args.base_url, case, args.timeout)
        for case in cases:
            if case["strategy"] in DIRECT:
                # A timed-out publish is uncertain: it may already have reached Kafka.
                # Record that outcome without treating an eventual delivery as failure.
                case["beforeExplicitRetry"] = require(api(args.base_url, "GET", f"/lab/runs/{case['runId']}"))
                case["sameIdRetry"] = submit(args.base_url, case)
                require(case["sameIdRetry"], (200, 202))
            case["final"] = wait_issued(args.base_url, case, args.timeout)
            outage = case["duringOutage"]
            final = case["final"]
            response = case.get("duringOutageSubmission", {}).get("http")
            coupon = final["perCoupon"][0]
            checks = {
                "real_topic_replication_factor_3": "ReplicationFactor: 3" in case["topicBeforeOutage"],
                "real_topic_min_isr_2": "min.insync.replicas=2" in case["topicBeforeOutage"],
                "outage_response_matches_durability": (response is not None and response >= 500) if case["strategy"] in DIRECT else response == 202,
                "one_request_identity": final["received"] == 1 and len(final["requests"]) == 1,
                "one_final_issuance": final["issued"] == final.get("ledgerIssued") == coupon.get("ledgerIssued") == 1,
                "no_duplicate_recipients": final.get("ledgerDuplicateUsers") == 0,
                "no_pending_or_rejected": final["pending"] == final["rejected"] == 0,
                "final_database_capacity": coupon["dbRemaining"] == 4 and coupon["total"] == 5,
                "relay_drained": final["outboxPending"] == 0 and final["streamPending"] == 0,
            }
            if case["strategy"] in {"SPLIT_REDIS", "LUA_REDIS", "REDIS_STREAM"}:
                during_coupon = outage["perCoupon"][0]
                checks["reservation_survived_actual_kafka_outage"] = (
                    outage["reserved"] == 1 and during_coupon["redisStock"] == 4
                    and during_coupon["redisUsers"] == 1)
                checks["no_double_redis_reservation_after_retry"] = coupon["redisStock"] == 4 and coupon["redisUsers"] == 1
            if case["strategy"] == "DB_OUTBOX":
                checks["atomic_database_reservation_and_outbox_during_outage"] = (
                    outage["activeDbReservations"] == 1 and outage["outboxPending"] == 1
                    and outage["perCoupon"][0]["dbRemaining"] == 4)
            if case["strategy"] == "REDIS_STREAM":
                checks["stream_contains_recoverable_work_during_outage"] = outage["streamPending"] == 1
            if case["strategy"] not in DIRECT:
                checks["recovered_without_client_retry"] = "automaticRecovery" in case and "sameIdRetry" not in case
            case["checks"] = checks
            case["passed"] = all(checks.values())
            save(args.output, result)
    except Exception as error:
        result["error"] = repr(error)
    finally:
        try:
            result["finallyBrokerRestore"] = infra("kafka-start", "all")
        except Exception as error:
            result["restoreError"] = repr(error)
        for case in cases:
            try:
                case["cleanup"] = api(args.base_url, "POST", f"/lab/runs/{case['runId']}/control",
                                      {"paused": True, "workers": 0})
                if case["cleanup"]["http"] != 200:
                    case["passed"] = False
            except Exception as error:
                case.update(passed=False, cleanupError=repr(error))
        result["finishedAt"] = utc_now()
        result["passed"] = (len(cases) == len(STRATEGIES) and all(case["passed"] for case in cases)
                            and "error" not in result and "restoreError" not in result)
        save(args.output, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:28080")
    parser.add_argument("--output", type=Path, default=ROOT / "results/kafka-outage-paths.json")
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    result = {"experiment": "five_architectures_during_real_kafka_outage", "startedAt": utc_now(),
              "passed": False, "cases": [], "configuration": {
                  "project": "coupon-lab", "stoppedServices": ["kafka2", "kafka3"],
                  "replicationFactor": 3, "minInSyncReplicas": 2, "acks": "all",
                  "producerDeliveryTimeoutMs": 10000},
              "interpretation": [
                  "The two stopped nodes are both brokers and KRaft controllers; the experiment removes controller quorum as well as sufficient ISR and does not isolate min.insync.replicas alone.",
                  "A producer timeout does not prove that no record was committed. Direct modes are observed after restoration and retried with the original request identity.",
                  "The final SQL unique constraints and conditional capacity transaction prevent duplicate issuance after uncertain or duplicated publication.",
                  "Durable DB Outbox and Redis Stream acceptance is verified before Kafka restoration; Redis Stream durability is limited by the separate Redis persistence and replication behavior."
              ]}
    observe(args, result)
    print(json.dumps({"experiment": result["experiment"], "passed": result["passed"],
                      "cases": [{"strategy": case["strategy"], "passed": case["passed"]} for case in result["cases"]],
                      "error": result.get("error")}), flush=True)
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
