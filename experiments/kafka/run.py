#!/usr/bin/env python3
"""Real HTTP/MySQL/Redis/Kafka experiments; Python 3.10+, Java 17+, Docker Compose.

Run after `docker compose -f experiments/kafka/compose.yml up -d --wait`
and `./gradlew labBootJar` in 3-coupon_redis_kafka/spring.
Every assertion is fail-closed; partial evidence survives failed runs.
"""
import argparse
import concurrent.futures
import csv
import datetime as dt
import gzip
import hashlib
import http.client
import json
import math
import os
import platform
import random
import signal
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "experiments/kafka/compose.yml"
PORT = 58080
BASE = f"http://127.0.0.1:{PORT}"


def now_ms():
    return time.time_ns() / 1_000_000


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def write_trace(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def api(path, method="GET", payload=None):
    data = None if method == "GET" else json.dumps(payload or {}).encode()
    request = urllib.request.Request(BASE + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=12) as response:
        body = response.read()
        return json.loads(body) if body else None


def wait_for(action, condition, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = action()
        if condition(last):
            return last
        time.sleep(0.1)
    raise AssertionError(f"Condition timed out: {str(last)[:500]}")


def command(args):
    return subprocess.run(args, cwd=ROOT, check=True, text=True, capture_output=True).stdout


def compose(*args):
    return command(["docker", "compose", "-f", str(COMPOSE), *args])


class App:
    def __init__(self, args, case, partitions, consumers, topic=None, group=None, log_name="app.log"):
        self.case = case
        self.partitions = partitions
        self.topic = topic or "lab-" + case.name.replace("_", "-")
        self.group = group or self.topic + "-group"
        self.marker = case / "fault-checkpoint.json"
        self.log = (case / log_name).open("w")
        env = os.environ.copy()
        env.update({
            "SPRING_PROFILES_ACTIVE": "kafka-lab",
            "SERVER_ADDRESS": "127.0.0.1", "SERVER_PORT": str(PORT),
            "SPRING_DATASOURCE_URL": "jdbc:mysql://localhost:53306/coupon_kafka_lab?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8",
            "SPRING_DATASOURCE_USERNAME": "coupon", "SPRING_DATASOURCE_PASSWORD": "coupon",
            "SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE": "10",
            "SPRING_JPA_SHOW_SQL": "false", "SPRING_JPA_PROPERTIES_HIBERNATE_FORMAT_SQL": "false",
            "SPRING_DATA_REDIS_HOST": "localhost", "SPRING_DATA_REDIS_PORT": "56379",
            "SPRING_KAFKA_BOOTSTRAP_SERVERS": "localhost:59092",
            "SPRING_KAFKA_CONSUMER_GROUP_ID": self.group,
            "SPRING_KAFKA_CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS": "10000",
            "SPRING_KAFKA_CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS": "3000",
            "SPRING_KAFKA_CONSUMER_MAX_POLL_RECORDS": "500",
            "COUPON_KAFKA_TOPIC": self.topic, "COUPON_KAFKA_PARTITIONS": str(partitions),
            "COUPON_KAFKA_CONCURRENCY": str(consumers), "LAB_FAULT_MARKER": str(self.marker),
            "LOGGING_LEVEL_ROOT": "WARN", "SPRING_MAIN_BANNER_MODE": "off",
        })
        self.started_at = now_ms()
        self.process = subprocess.Popen([args.java, "-Xms512m", "-Xmx512m", "-XX:ActiveProcessorCount=4",
                                         "-Duser.timezone=UTC", "-jar", str(args.jar)],
                                        cwd=ROOT, env=env, stdout=self.log, stderr=subprocess.STDOUT)
        try:
            self.ready = wait_for(self.status_or_starting, lambda s: s.get("running") and
                                  sum(len(v) for v in s["assignments"].values()) == partitions, timeout=100)
            config = api("/lab/config")
            assert config["consumer"]["enable.auto.commit"] is False
            assert config["consumer"]["session.timeout.ms"] == 10000, config
            assert config["consumer"]["max.poll.records"] == 500, config
            assert config["ackMode"] == "BATCH", config
            write_json(case / ("config.json" if log_name == "app.log" else "restarted-config.json"), config)
        except BaseException:
            self.close()
            raise

    def status_or_starting(self):
        if self.process.poll() is not None:
            raise AssertionError(f"App exited {self.process.returncode}; see {self.case.name}/app.log")
        try:
            return api("/lab/status")
        except (OSError, ValueError):
            return {}

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.log.close()


def seed(count, total):
    assert total % count == 0
    return api("/lab/seed", "POST", {"couponCount": count, "stockPerCoupon": total // count})


def load(ids, total, workers, user_start=1_000_000):
    barrier = threading.Barrier(workers)

    def worker(index):
        connection = http.client.HTTPConnection("127.0.0.1", PORT, timeout=30)
        rows = []
        try:
            barrier.wait(timeout=15)
            for number in range(index, total, workers):
                coupon_id, user_id = ids[number % len(ids)], user_start + number
                body = json.dumps({"userId": user_id})
                start = now_ms()
                connection.request("POST", f"/api/coupons/{coupon_id}/issue", body,
                                   {"Content-Type": "application/json"})
                response = connection.getresponse()
                response.read()
                end = now_ms()
                rows.append({"couponId": coupon_id, "userId": user_id, "startedAtMs": start,
                             "httpEndedAtMs": end, "httpDurationMs": end - start, "status": response.status})
        finally:
            connection.close()
        return rows

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, index) for index in range(workers)]
        return [row for future in futures for row in future.result()]


def drained(expected):
    return wait_for(lambda: api("/lab/lag"), lambda s: s["completed"] == expected and s["lag"] == 0)


def check_state(ids, expected):
    state = api("/lab/state?" + urllib.parse.urlencode({"ids": ",".join(map(str, ids))}))
    assert state["duplicateGroups"] == [], state
    assert sum(c["db_rows"] for c in state["coupons"]) == expected, state
    assert all(c["db_rows"] == c["issued_quantity"] == c["total_quantity"] for c in state["coupons"]), state
    assert sum(int(c["users"]) for c in state["redis"]) == expected, state
    assert all(int(c["stock"]) == 0 for c in state["redis"]), state
    return state


def percentile(values, quantile):
    ordered = sorted(values)
    at = (len(ordered) - 1) * quantile
    low, high = math.floor(at), math.ceil(at)
    return ordered[low] + (ordered[high] - ordered[low]) * (at - low)


def benchmark(args, case, partitions, consumers, workload, repeat):
    app = App(args, case, partitions, consumers)
    try:
        warm_ids = seed(30, args.warmup)
        warm = load(warm_ids, args.warmup, args.workers, user_start=9_000_000)
        assert all(r["status"] == 202 for r in warm)
        drained(args.warmup)
        check_state(warm_ids, args.warmup)
        api("/lab/reset-metrics", "POST")
        ids = seed(1 if workload == "hot" else 30, args.records)
        samples, sampling_errors = [], []
        stop = threading.Event()

        def sample():
            while not stop.is_set():
                try:
                    samples.append(api("/lab/lag"))
                except Exception as ex:
                    sampling_errors.append(str(ex))
                stop.wait(0.1)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            requests = load(ids, args.records, args.workers)
            write_trace(case / "requests.jsonl.gz", requests)
            assert all(r["status"] == 202 for r in requests), Counter(r["status"] for r in requests)
            final = drained(args.records)
            samples.append(final)
        finally:
            stop.set()
            sampler.join(timeout=15)
            write_json(case / "lag.json", {"samples": samples, "errors": sampling_errors})
        completions = api("/lab/completions")
        write_trace(case / "completions.jsonl.gz", completions)
        assert not sampling_errors, sampling_errors
        assert final["failures"] == 0
        assert len(completions) == args.records
        by_key = {(r["couponId"], r["userId"]): r for r in completions}
        assert len(by_key) == args.records
        latencies = [by_key[(r["couponId"], r["userId"])]["completedAtMs"] - r["startedAtMs"] for r in requests]
        assert min(latencies) >= -1, "Clock discontinuity or identity mismatch"
        state = check_state(ids, args.records)
        write_json(case / "state.json", state)
        last_http = max(r["httpEndedAtMs"] for r in requests)
        clear_samples = [s["atMs"] for s in samples if s["atMs"] >= last_http and s["lag"] == 0 and s["completed"] == args.records]
        duration = max(r["completedAtMs"] for r in completions) - min(r["startedAtMs"] for r in requests)
        result = {
            "case": case.name, "repeat": repeat, "partitions": partitions, "consumers": consumers,
            "workload": workload, "requests": args.records, "accepted": len(requests),
            "db_rows": args.records, "duplicate_groups": 0, "oversold": 0,
            "http_p95_ms": percentile([r["httpDurationMs"] for r in requests], .95),
            "request_to_db_p50_ms": percentile(latencies, .5),
            "request_to_db_p95_ms": percentile(latencies, .95),
            "request_to_db_p99_ms": percentile(latencies, .99),
            "request_to_db_max_ms": max(latencies),
            "db_completion_rate_per_s": args.records * 1000 / duration,
            "observed_max_lag": max(s["lag"] for s in samples),
            "lag_clear_after_last_http_ms": max(0, min(clear_samples) - last_http),
            "partition_records": dict(Counter(r["partition"] for r in completions)),
            "thread_records": dict(Counter(r["thread"] for r in completions)),
            "assignments": app.ready["assignments"], "final_offsets": final["offsets"],
            "topic": app.topic, "group": app.group,
        }
        write_json(case / "result.json", result)
        return result
    finally:
        app.close()


def external_db(coupon_id):
    assert isinstance(coupon_id, int)
    sql = (f"SELECT c.issued_quantity, COUNT(i.id), COUNT(DISTINCT i.user_id) FROM coupon c "
           f"LEFT JOIN issued_coupon i ON c.id=i.coupon_id WHERE c.id={coupon_id} GROUP BY c.id;")
    output = compose("exec", "-T", "mysql", "mysql", "--batch", "--raw", "--skip-column-names",
                     "-ucoupon", "-pcoupon", "coupon_kafka_lab", "-e", sql)
    quantity, rows, users = map(int, output.strip().split())
    return {"issued_quantity": quantity, "db_rows": rows, "distinct_users": users}


def external_offsets(topic, group):
    output = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                     "--bootstrap-server", "kafka:9094", "--group", group, "--describe")
    rows = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 6 and fields[0] == group and fields[1] == topic:
            rows.append({"partition": int(fields[2]), "committed": int(fields[3]),
                         "end": int(fields[4]), "lag": int(fields[5])})
    assert rows, output
    return {"rows": rows, "cli_stdout": output}


def recovery(args, case, repeat):
    app = App(args, case, 1, 1)
    replacement = None
    try:
        ids = seed(1, 51)
        warm = load(ids, 1, 1, user_start=100_000)
        assert warm[0]["status"] == 202
        seed_offset = drained(1)
        api("/lab/pause", "POST")
        wait_for(lambda: api("/lab/status"), lambda s: s["paused"])
        queued = load(ids, 50, 1, user_start=200_000)
        assert all(r["status"] == 202 for r in queued)
        backlog = wait_for(lambda: api("/lab/lag"), lambda s: s["lag"] == 50)
        before_resume = external_db(ids[0])
        assert before_resume == {"issued_quantity": 1, "db_rows": 1, "distinct_users": 1}
        api("/lab/arm?userId=200000", "POST")
        api("/lab/resume", "POST")

        def marker():
            try:
                return json.loads(app.marker.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                return None

        checkpoint = wait_for(marker, lambda value: value is not None, timeout=30)
        before_kill_db = external_db(ids[0])
        before_kill_offsets = external_offsets(app.topic, app.group)
        assert checkpoint["completion"]["offset"] == 1
        assert checkpoint["committedOffset"] == 1
        assert before_kill_db == {"issued_quantity": 2, "db_rows": 2, "distinct_users": 2}
        assert before_kill_offsets["rows"] == [{"partition": 0, "committed": 1, "end": 51, "lag": 50}]
        kill_at = now_ms()
        app.process.kill()  # POSIX SIGKILL: no JVM hooks, listener stop, or LeaveGroup.
        exit_code = app.process.wait(timeout=10)
        assert exit_code == -signal.SIGKILL, exit_code
        killed_db = external_db(ids[0])
        killed_offsets = external_offsets(app.topic, app.group)
        assert killed_db == before_kill_db
        assert killed_offsets["rows"] == before_kill_offsets["rows"]
        write_json(case / "killed-state.json", {"db": killed_db, "offsets": killed_offsets,
                                                "exit_code": exit_code, "kill_at_ms": kill_at})
        replacement = App(args, case, 1, 1, app.topic, app.group, log_name="restarted-app.log")
        final = drained(50)
        recovered_at = now_ms()
        completions = api("/lab/completions")
        state = check_state(ids, 51)
        assert final["failures"] == 0
        replayed = [c for c in completions if c["partition"] == 0 and c["offset"] == 1]
        assert len(replayed) == 1 and replayed[0]["userId"] == 200_000
        assert len(completions) == 50
        assert final["offsets"] == [{"partition": 0, "end": 51, "committed": 51}]
        write_trace(case / "requests.jsonl.gz", warm + queued)
        write_trace(case / "recovered-completions.jsonl.gz", completions)
        write_json(case / "state.json", state)
        result = {"case": case.name, "repeat": repeat, "topic": app.topic, "group": app.group,
                  "seed_offsets": seed_offset, "paused_backlog": backlog, "before_resume_db": before_resume,
                  "checkpoint": checkpoint, "before_kill_db": before_kill_db,
                  "before_kill_offsets": before_kill_offsets, "exit_code": exit_code,
                  "killed_db": killed_db, "killed_offsets": killed_offsets,
                  "replayed_same_offset": replayed[0], "recovered_deliveries": len(completions),
                  "final_db_rows": 51, "final_issued_quantity": 51, "duplicate_groups": 0,
                  "final_offsets": final["offsets"], "kill_to_recovered_ms": recovered_at - kill_at}
        write_json(case / "result.json", result)
        return result
    finally:
        app.close()
        if replacement:
            replacement.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["all", "benchmark", "recovery", "smoke"], default="all")
    parser.add_argument("--records", type=int, default=6000)
    parser.add_argument("--warmup", type=int, default=1500)
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--java", default="java")
    parser.add_argument("--jar", type=Path, default=ROOT / "3-coupon_redis_kafka/spring/build/libs/coupon-0.0.1-SNAPSHOT-kafka-lab.jar")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    args.out = (args.out or ROOT / "experiments/kafka/out" / stamp).resolve()
    args.jar = args.jar.resolve()
    assert args.jar.exists(), args.jar
    if args.mode == "smoke":
        args.records, args.warmup, args.repeats = 300, 300, 1
    assert args.records > 0 and args.records % 30 == 0 and args.warmup % 30 == 0
    assert args.repeats > 0 and 0 < args.workers <= args.records
    args.out.mkdir(parents=True, exist_ok=False)
    try:
        with urllib.request.urlopen(BASE + "/actuator/health", timeout=1):
            raise AssertionError(f"Port {PORT} already hosts an app")
    except OSError:
        pass
    meta = {"started_at_utc": stamp, "git_commit": command(["git", "rev-parse", "HEAD"]).strip(),
            "working_tree_diff_sha256": hashlib.sha256(command(["git", "diff", "HEAD"]).encode()).hexdigest(),
            "jar_sha256": hashlib.sha256(args.jar.read_bytes()).hexdigest(), "platform": platform.platform(),
            "mode": args.mode, "records": args.records, "warmup": args.warmup,
            "workers": args.workers, "repeats": args.repeats,
            "jvm_heap": "512m", "jvm_active_processor_count": 4,
            "java_version": subprocess.run([args.java, "-version"], text=True, capture_output=True, check=True).stderr.strip(),
            "docker_resources": json.loads(command(["docker", "info", "--format",
                '{"NCPU":{{.NCPU}},"MemTotal":{{.MemTotal}},"Architecture":"{{.Architecture}}","ServerVersion":"{{.ServerVersion}}"}'])),
            "sampling_interval_after_response_ms": 100}
    # Keep only hardware/resource information, never Docker client/user configuration.
    meta["docker_resources"] = {k: meta["docker_resources"][k] for k in ("NCPU", "MemTotal", "Architecture", "ServerVersion")}
    write_json(args.out / "run.json", meta)
    benchmarks, recoveries = [], []
    try:
        if args.mode in ("all", "benchmark", "smoke"):
            configurations = [(1, 1)] if args.mode == "smoke" else [(1, 1), (3, 1), (3, 3)]
            for repeat in range(1, args.repeats + 1):
                schedule = [(p, c, w) for p, c in configurations for w in ("hot", "spread")]
                random.Random(20260913 + repeat).shuffle(schedule)
                for p, c, workload in schedule:
                    case = args.out / f"{stamp.lower()}-r{repeat}-p{p}c{c}-{workload}"
                    case.mkdir()
                    print(f"RUN {case.name}", flush=True)
                    result = benchmark(args, case, p, c, workload, repeat)
                    benchmarks.append(result)
                    write_json(args.out / "benchmark-results.json", benchmarks)
                    print(f"PASS DB={result['db_rows']} p95={result['request_to_db_p95_ms']:.1f}ms lag={result['observed_max_lag']} partitions={result['partition_records']}", flush=True)
        if args.mode in ("all", "recovery", "smoke"):
            for repeat in range(1, args.repeats + 1):
                case = args.out / f"{stamp.lower()}-recovery-r{repeat}"
                case.mkdir()
                print(f"RUN {case.name}", flush=True)
                result = recovery(args, case, repeat)
                recoveries.append(result)
                write_json(args.out / "recovery-results.json", recoveries)
                print(f"PASS SIGKILL={result['exit_code']} replay=partition0/offset1 DB=51 duplicates=0", flush=True)
        if benchmarks:
            fields = [k for k, v in benchmarks[0].items() if not isinstance(v, (dict, list))]
            with (args.out / "benchmark.csv").open("w", newline="") as file:
                writer = csv.DictWriter(file, fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(benchmarks)
        write_json(args.out / "completion.json", {"passed": True, "benchmark_cases": len(benchmarks),
                                                  "recovery_cases": len(recoveries)})
        print(f"COMPLETE {args.out}", flush=True)
    except BaseException as ex:
        write_json(args.out / "failure.json", {"type": type(ex).__name__, "message": str(ex)})
        raise


if __name__ == "__main__":
    main()
