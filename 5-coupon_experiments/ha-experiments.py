#!/usr/bin/env python3
"""Real node-failure experiments. Only operates on the isolated coupon-lab project."""
import argparse
import concurrent.futures
import json
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COMPOSE = ["docker", "compose", "-p", "coupon-lab", "-f", str(ROOT / "compose.yml")]


def command(*args, timeout=90):
    p = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f"{' '.join(args)}: {p.stderr[-2500:]}")
    return p.stdout.strip()


def infra(*args):
    result = command("python3", str(ROOT / "infra" / "lab.py"), *args, timeout=180)
    try:
        return json.loads(result)
    except json.JSONDecodeError:
        return {"output": result}


def redis(port, *args):
    values = [str(x).encode() for x in args]
    payload = b"*%d\r\n" % len(values)
    for value in values:
        payload += b"$%d\r\n" % len(value) + value + b"\r\n"
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        connection.sendall(payload)
        reader = connection.makefile("rb")

        def read():
            prefix, line = reader.read(1), reader.readline().rstrip(b"\r\n")
            if prefix == b"-":
                raise RuntimeError(line.decode())
            if prefix == b"+":
                return line.decode()
            if prefix == b":":
                return int(line)
            if prefix == b"$":
                length = int(line)
                if length < 0:
                    return None
                value = reader.read(length)
                reader.read(2)
                return value.decode()
            if prefix == b"*":
                return [read() for _ in range(int(line))]
            raise RuntimeError(f"Bad Redis response {prefix!r}: {line!r}")

        return read()


def api(base, method, path, data=None):
    req = urllib.request.Request(base + path, method=method,
                                 data=None if data is None else json.dumps(data).encode(),
                                 headers={"Content-Type": "application/json"})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=40) as response:
            status, body = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read()
    return {"http": status, "body": json.loads(body), "ms": (time.monotonic() - started) * 1000}


def wait_snapshot(base, run_id, issued, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = api(base, "GET", f"/lab/runs/{run_id}")
        if last["http"] == 200 and last["body"].get("issued", 0) >= issued:
            return last["body"]
        time.sleep(.3)
    raise AssertionError(f"Run {run_id} did not reach {issued} issued: {last}")


def redis_failover():
    key = f"lab:ha:{uuid.uuid4().hex}"
    baseline = key + ":baseline"
    record = {"name": "redis_acknowledged_write_loss", "kind": "real_process_and_replication_failure"}
    # Docker Desktop may temporarily lose a published-port route on network reconnect.
    # Read through scoped docker exec so a host routing issue is not mistaken for data loss.
    def cli(service, *args):
        return json.loads(command(*COMPOSE, "exec", "-T", service, "redis-cli", "--json", *args))
    try:
        infra("redis-restore")
        cli("redis-primary", "SET", baseline, "replicated")
        deadline = time.monotonic() + 20
        while cli("redis-replica", "GET", baseline) != "replicated":
            if time.monotonic() > deadline:
                raise AssertionError("Replica did not receive baseline")
            time.sleep(.2)
        record["freeze"] = infra("redis-freeze-replica")
        record["primary_ack"] = cli("redis-primary", "SET", key, "accepted-before-crash")
        record["promotion"] = infra("redis-promote")
        record["baseline_after_promotion"] = cli("redis-replica", "GET", baseline)
        record["accepted_write_after_promotion"] = cli("redis-replica", "GET", key)
        record["passed"] = (record["primary_ack"] == "OK"
                            and record["baseline_after_promotion"] == "replicated"
                            and record["accepted_write_after_promotion"] is None)
        record["meaning"] = "An acknowledged primary write was absent on the deliberately isolated promoted replica; this is a counterexample to failover implying no loss."
    except Exception as error:
        record.update(passed=False,error=repr(error))
    finally:
        record["restore"] = infra("redis-restore")
    return record


def kafka_failover(base):
    record = {"name": "kafka_leader_failure", "kind": "real_broker_sigkill"}
    run_id = None
    try:
        infra("kafka-start", "all")
        created = api(base, "POST", "/lab/runs", {"strategy": "KAFKA_FIRST", "stock": 100,
                       "coupons": 1, "partitions": 3, "workers": 3})
        assert created["http"] in (200, 201), created
        run_id, topic = created["body"]["runId"], created["body"]["topic"]
        record.update(runId=run_id, topic=topic)
        api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True})
        seed = api(base, "POST", f"/lab/runs/{run_id}/requests",
                   {"requestId": "seed", "couponId": 1, "userId": "seed"})
        assert seed["http"] in (200, 202), seed
        first = api(base, "GET", f"/lab/runs/{run_id}")["body"]
        assert first["issued"] == 0 and first["pending"] == 1, first
        assert first["requests"][0]["published"], first
        record["acknowledgedUnconsumedBeforeKill"] = first
        partition = next(r["partition"] for r in first["requests"] if r["requestId"] == "seed")
        record["failure"] = infra("kafka-stop-leader", "--topic", topic, "--partition", str(partition))
        api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": False})
        def send(i):
            return api(base, "POST", f"/lab/runs/{run_id}/requests",
                       {"requestId": f"failover-{i}", "couponId": 1, "userId": f"failover-{i}"})
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            replies = list(pool.map(send, range(40)))
        record["responses"] = replies
        # Retry uncertain submissions with the same request IDs after all brokers return.
        infra("kafka-start", "all")
        retries = [send(i) for i, r in enumerate(replies) if r["http"] not in (200, 202)]
        record["retries"] = retries
        final = wait_snapshot(base, run_id, 41)
        record["snapshot"] = final
        issued = [r for r in final["requests"] if r["status"] == "ISSUED"]
        record["passed"] = (final["issued"] == final["ledgerIssued"] == 41
                            and final["pending"] == 0 and final["ledgerDuplicateUsers"] == 0
                            and len({(r["couponId"], r["userId"]) for r in issued}) == 41)
    finally:
        infra("kafka-start", "all")
        if run_id:
            api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
    return record


def kafka_insufficient_isr(base):
    record = {"name": "kafka_insufficient_isr", "kind": "real_two_broker_shutdown"}
    run_id = None
    try:
        infra("kafka-start", "all")
        created = api(base, "POST", "/lab/runs", {"strategy": "KAFKA_FIRST", "stock": 10,
                       "coupons": 1, "partitions": 3, "workers": 1})
        assert created["http"] in (200, 201), created
        run_id = created["body"]["runId"]
        record["runId"] = run_id
        command(*COMPOSE, "stop", "-t", "2", "kafka2", "kafka3")
        submitted = api(base, "POST", f"/lab/runs/{run_id}/requests",
                        {"requestId": "isr", "couponId": 1, "userId": "isr"})
        record["without_quorum"] = submitted
        infra("kafka-start", "all")
        retry = api(base, "POST", f"/lab/runs/{run_id}/requests",
                    {"requestId": "isr", "couponId": 1, "userId": "isr"})
        record["retry"] = retry
        final = wait_snapshot(base, run_id, 1)
        record["snapshot"] = final
        record["passed"] = (submitted["http"] >= 500 and retry["http"] in (200, 202)
                            and final["issued"] == final["ledgerIssued"] == 1
                            and final["pending"] == 0 and final["ledgerDuplicateUsers"] == 0)
        record["meaning"] = "With two combined broker/controller nodes down, both controller quorum and ISR are insufficient; this does not isolate min.insync.replicas alone."
    finally:
        infra("kafka-start", "all")
        if run_id:
            api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
    return record


def database_recovery(base):
    record = {"name": "mysql_outage_consumer_recovery", "kind": "real_mysql_process_stop"}
    run_id = None
    try:
        created = api(base, "POST", "/lab/runs", {"strategy": "KAFKA_FIRST", "stock": 10,
                       "coupons": 1, "partitions": 3, "workers": 1})
        assert created["http"] in (200, 201), created
        run_id = created["body"]["runId"]
        record["runId"] = run_id
        api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True})
        record["submissions"] = [api(base, "POST", f"/lab/runs/{run_id}/requests",
            {"requestId": f"db-recovery-{i}", "couponId": 1, "userId": f"db-recovery-{i}",
             "processingDelayMs": 1000 if i == 0 else 0}) for i in range(10)]
        assert all(r["http"] in (200, 202) for r in record["submissions"]), record
        api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": False})
        record["stoppedAt"] = datetime.now(timezone.utc).isoformat()
        command(*COMPOSE, "stop", "-t", "1", "mysql")
        time.sleep(2)
        record["queryDuringOutage"] = api(base, "GET", f"/lab/runs/{run_id}")
        command(*COMPOSE, "up", "-d", "--wait", "mysql", timeout=120)
        record["restartedAt"] = datetime.now(timezone.utc).isoformat()
        record["snapshot"] = wait_snapshot(base, run_id, 10)
        ordered = sorted(record["snapshot"]["requests"], key=lambda r: r["offset"])
        sequences = [r["completionSeq"] for r in ordered]
        record["blockingOrderPreserved"] = sequences == sorted(sequences) and len(set(sequences)) == 10
        record["passed"] = (record["queryDuringOutage"]["http"] >= 500
                            and record["snapshot"]["issued"] == record["snapshot"]["ledgerIssued"] == 10
                            and record["snapshot"]["pending"] == 0
                            and record["snapshot"].get("ledgerDuplicateUsers") == 0
                            and record["blockingOrderPreserved"])
    finally:
        command(*COMPOSE, "up", "-d", "--wait", "mysql", timeout=120)
        if run_id:
            api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:28080")
    parser.add_argument("--output", default=str(ROOT / "results" / "ha.json"))
    parser.add_argument("--scenario", choices=["all", "redis", "kafka", "isr", "db"], default="all")
    args = parser.parse_args()
    result = {"startedAt": datetime.now(timezone.utc).isoformat(), "scenarios": [], "passed": False}
    tests = {"redis": redis_failover, "kafka": lambda: kafka_failover(args.base_url),
             "isr": lambda: kafka_insufficient_isr(args.base_url), "db": lambda: database_recovery(args.base_url)}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    for name, test in tests.items():
        if args.scenario not in ("all", name):
            continue
        try:
            record = test()
        except Exception as error:
            record = {"name": name, "passed": False, "error": repr(error)}
        result["scenarios"].append(record)
        output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
        print(json.dumps({"name": record["name"], "passed": record["passed"], "error": record.get("error")}))
    result["finishedAt"] = datetime.now(timezone.utc).isoformat()
    result["passed"] = all(r["passed"] for r in result["scenarios"])
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
