#!/usr/bin/env python3
"""Small real-server counterexamples: Lua rollback and Kafka key remapping."""
import json
from pathlib import Path
import re
import subprocess
import uuid

ROOT = Path(__file__).resolve().parent
BASE = ["docker", "compose", "-p", "coupon-lab", "-f", str(ROOT / "compose.yml"), "exec", "-T"]


def run(service, *args, stdin=None):
    p = subprocess.run([*BASE, service, *args], input=stdin, capture_output=True, text=True, timeout=90)
    if p.returncode:
        raise RuntimeError(p.stdout + p.stderr)
    return p.stdout.strip()


def lua_error():
    key = "lab:lua-error:" + uuid.uuid4().hex
    script = "redis.call('SET', KEYS[1], 'written-before-error'); return redis.call('SADD', KEYS[1], 'user')"
    error = run("redis-primary", "redis-cli", "--json", "EVAL", script, "1", key)
    value = run("redis-primary", "redis-cli", "--json", "GET", key)
    return {"name": "lua_runtime_error_does_not_rollback_prior_write", "script": script, "key": key,
            "error": error, "valueAfterError": value,
            "passed": "WRONGTYPE" in error and json.loads(value) == "written-before-error"}


def partition_growth():
    topic = "coupon-lab-key-growth-" + uuid.uuid4().hex
    admin = ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka1:9092"]
    run("kafka1", *admin, "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "3", "--config", "min.insync.replicas=2")
    producer = ["/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "kafka1:9092", "--topic", topic,
                "--property", "parse.key=true", "--property", "key.separator=:", "--producer-property", "acks=all"]
    for phase in ("before", "after"):
        if phase == "after":
            run("kafka1", *admin, "--alter", "--topic", topic, "--partitions", "6")
        run("kafka1", *producer, stdin="".join(f"coupon-{i}:{phase}\n" for i in range(12)))
    output = run("kafka1", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka1:9092", "--topic", topic,
                 "--from-beginning", "--max-messages", "24", "--timeout-ms", "15000",
                 "--property", "print.partition=true", "--property", "print.key=true")
    observations = {}
    for line in output.splitlines():
        match = re.fullmatch(r"Partition:(\d+)\s+(coupon-\d+)\s+(before|after)", line)
        if match:
            partition, key, phase = match.groups()
            observations.setdefault(key, {})[phase] = int(partition)
    remapped = [key for key, value in observations.items() if value.get("before") != value.get("after")]
    passed = (len(observations) == 12 and all(set(v) == {"before", "after"} for v in observations.values())
              and all(v["before"] == 0 for v in observations.values()) and bool(remapped))
    return {"name": "key_mapping_after_partition_growth", "topic": topic, "oldPartitions": 1, "newPartitions": 6,
            "records": observations, "remappedKeys": remapped, "rawConsumerOutput": output, "passed": passed,
            "scope": "A fresh producer is used after expansion so it observes the new partition metadata; existing records remain where originally appended."}


def main():
    root = ROOT / "results"
    root.mkdir(exist_ok=True)
    results = []
    for name, fn in [("lua-error-semantics", lua_error), ("partition-growth", partition_growth)]:
        try:
            result = fn()
        except Exception as error:
            result = {"name": name, "passed": False, "error": repr(error)}
        (root / (name + ".json")).write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
        print(json.dumps(result, ensure_ascii=False))
        results.append(result)
    raise SystemExit(0 if all(r["passed"] for r in results) else 1)


if __name__ == "__main__":
    main()
