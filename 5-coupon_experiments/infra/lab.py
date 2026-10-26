#!/usr/bin/env python3
"""Scoped local infrastructure controls. All successful actions emit one JSON object."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

LAB = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-p", "coupon-lab", "-f", str(LAB / "compose.yml")]
EVIDENCE = Path(os.environ.get("COUPON_LAB_EVIDENCE", LAB / "results" / "infra"))
SENTINELS = ["sentinel1", "sentinel2", "sentinel3"]


def command(args: list[str], *, check: bool = True, timeout: int = 180) -> str:
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(args)}\n{result.stderr}\n{result.stdout}")
    return result.stdout.strip()


def compose(*args: str, check: bool = True, timeout: int = 180) -> str:
    return command([*COMPOSE, *args], check=check, timeout=timeout)


def redis(service: str, *args: str) -> str:
    return compose("exec", "-T", service, "redis-cli", "--raw", "-e", *args)


def info(service: str) -> dict[str, str]:
    return dict(line.split(":", 1) for line in redis(service, "INFO", "replication").splitlines() if ":" in line)


def container_id(service: str) -> str:
    value = compose("ps", "-a", "-q", service)
    if not value:
        raise RuntimeError(f"Service {service} has no coupon-lab container")
    return value


def state(service: str) -> dict:
    return json.loads(command(["docker", "inspect", "--format", "{{json .State}}", container_id(service)]))


def wait_until(check, message: str, timeout: int = 120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            last = str(error)
        time.sleep(0.5)
    raise RuntimeError(f"Timed out: {message}; last={last}")


def wait_replication(service: str = "redis-replica") -> dict:
    def synchronized():
        details = info(service)
        return details if details.get("master_link_status") == "up" and details.get("master_sync_in_progress") == "0" else None
    return wait_until(synchronized, f"Redis replication on {service}")


def snapshot() -> list[dict]:
    text = compose("ps", "-a", "--format", "json")
    if not text:
        return []
    records = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines()]
    return [{key: item.get(key) for key in ("Service", "Name", "State", "Health", "Image", "Ports", "ExitCode")} for item in records]


def save_logs(label: str) -> str:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    filename = EVIDENCE / f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}-{label}.log"
    result = subprocess.run([*COMPOSE, "logs", "--no-color", "--timestamps", "--tail", "1000"], capture_output=True, text=True)
    filename.write_text(result.stdout + result.stderr)
    return str(filename.relative_to(LAB)) if filename.is_relative_to(LAB) else str(filename)


def kafka_description(topic: str) -> str:
    for service in ("kafka1", "kafka2", "kafka3"):
        if state(service).get("Running"):
            return compose("exec", "-T", service, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka1:9092,kafka2:9092,kafka3:9092", "--describe", "--topic", topic, timeout=60)
    raise RuntimeError("No running coupon-lab Kafka broker")


def unpause_replica():
    if state("redis-replica").get("Paused"):
        command(["docker", "unpause", container_id("redis-replica")])


def replica_networks() -> dict:
    return json.loads(command(["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", container_id("redis-replica")]))


def reconnect_replica():
    if "coupon-lab_default" not in replica_networks():
        command(["docker", "network", "connect", "--alias", "redis-replica", "coupon-lab_default", container_id("redis-replica")])


def stop_sentinels():
    compose("--profile", "sentinel", "stop", *SENTINELS)


def run(args) -> dict:
    action = args.action
    if action == "up":
        compose("config", "-q")
        compose("up", "-d", "--wait", "--wait-timeout", "240", timeout=360)
        return {"services": snapshot(), "redis_replication": wait_replication()}
    if action == "cluster-up":
        services = ["cluster1", "cluster2", "cluster3"]
        compose("--profile", "cluster", "up", "-d", "--wait", *services, timeout=180)
        details = redis("cluster1", "CLUSTER", "INFO")
        if "cluster_slots_assigned:0" in details:
            redis("cluster1", "--cluster", "create", "cluster1:6379", "cluster2:6379", "cluster3:6379", "--cluster-replicas", "0", "--cluster-yes")
        wait_until(lambda: all("cluster_state:ok" in redis(service, "CLUSTER", "INFO") for service in services), "Redis Cluster slot coverage")
        return {"cluster_masters": [{"service": service, "endpoint": f"127.0.0.1:{27379 + index}", "node_id": redis(service, "CLUSTER", "MYID")} for index, service in enumerate(services)], "replicas": 0, "slots": redis("cluster1", "CLUSTER", "NODES")}
    if action == "versions":
        images = {}
        for image in ("apache/kafka:3.8.1", "mysql:8.0", "redis:7-alpine"):
            images[image] = json.loads(command(["docker", "image", "inspect", "--format", "{\"id\":{{json .Id}},\"digests\":{{json .RepoDigests}}}", image]))
        return {"images": images, "docker_server": command(["docker", "version", "--format", "{{.Server.Version}}"]), "mysql": compose("exec", "-T", "mysql", "mysql", "--version"), "redis": compose("exec", "-T", "redis-primary", "redis-server", "--version"), "kafka": compose("exec", "-T", "kafka1", "/opt/kafka/bin/kafka-topics.sh", "--version")}
    if action == "status":
        return {"services": snapshot(), "endpoints": {"mysql": "127.0.0.1:23306", "redis_primary": "127.0.0.1:26379", "redis_replica": "127.0.0.1:26380", "kafka": "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094"}}
    if action == "logs":
        return {"logs": save_logs("snapshot")}
    if action == "down":
        logfile = save_logs("before-down")
        # Never remove volumes: repeated experiments keep their broker/database evidence.
        compose("--profile", "sentinel", "--profile", "cluster", "down", timeout=180)
        return {"logs": logfile, "volumes_preserved": True}
    if action == "kafka-stop-leader":
        before = kafka_description(args.topic)
        match = re.search(rf"Partition:\s*{args.partition}\s+Leader:\s*(\d+)", before)
        if not match or match.group(1) not in {"1", "2", "3"}:
            raise RuntimeError(f"No valid leader for {args.topic}/{args.partition}: {before}")
        leader = int(match.group(1))
        compose("kill", "-s", "SIGKILL", f"kafka{leader}")
        return {"leader": leader, "service": f"kafka{leader}", "before": before, "logs": save_logs("kafka-leader-killed")}
    if action == "kafka-start":
        services = [f"kafka{i}" for i in (1, 2, 3)] if args.broker == "all" else [f"kafka{args.broker}"]
        compose("up", "-d", "--wait", "--wait-timeout", "180", *services, timeout=240)
        return {"started": services, "services": snapshot()}
    if action == "redis-freeze-replica":
        stop_sentinels()
        details = wait_replication()
        # Disconnect before writing, then close the old replication socket. Pausing
        # alone could leave replication bytes buffered and replay them on resume.
        command(["docker", "network", "disconnect", "--force", "coupon-lab_default", container_id("redis-replica")])
        redis("redis-primary", "CLIENT", "KILL", "TYPE", "REPLICA")
        wait_until(lambda: info("redis-primary").get("connected_slaves") == "0", "replica disconnected")
        return {"replica_network_isolated": True, "before": details, "primary_after": info("redis-primary"), "write_to": "127.0.0.1:26379"}
    if action == "redis-promote":
        stop_sentinels()
        compose("kill", "-s", "SIGKILL", "redis-primary")
        unpause_replica()
        redis("redis-replica", "REPLICAOF", "NO", "ONE")
        reconnect_replica()
        return {"promoted": "127.0.0.1:26380", "promoted_service": "redis-replica", "read_via": "docker compose -p coupon-lab -f compose.yml exec -T redis-replica redis-cli --raw", "replication": info("redis-replica"), "logs": save_logs("redis-manual-failover")}
    if action == "redis-restore":
        stop_sentinels()
        unpause_replica()
        reconnect_replica()
        compose("up", "-d", "--wait", "redis-primary", "redis-replica", timeout=180)
        replica_info = info("redis-replica")
        if replica_info.get("role") == "master":
            # The promoted replica is authoritative. Discard divergent old-primary data
            # through a real full/partial replication sync before swapping roles back.
            redis("redis-primary", "REPLICAOF", "redis-replica", "6379")
            wait_replication("redis-primary")
            redis("redis-replica", "WAIT", "1", "5000")
            redis("redis-primary", "REPLICAOF", "NO", "ONE")
            redis("redis-replica", "REPLICAOF", "redis-primary", "6379")
        else:
            redis("redis-primary", "REPLICAOF", "NO", "ONE")
            redis("redis-replica", "REPLICAOF", "redis-primary", "6379")
        details = wait_replication()
        return {"primary": "127.0.0.1:26379", "replica": "127.0.0.1:26380", "replication": details, "logs": save_logs("redis-restored")}
    if action == "sentinel-up":
        compose("--profile", "sentinel", "up", "-d", "--wait", *SENTINELS, timeout=180)
        return {"sentinels": ["127.0.0.1:26381", "127.0.0.1:26382", "127.0.0.1:26383"], "quorum": 2}
    if action == "sentinel-failover":
        compose("--profile", "sentinel", "up", "-d", "--wait", *SENTINELS, timeout=180)
        # Wait for all Sentinels to discover one another before killing the primary.
        wait_until(lambda: int(dict(zip(*[iter(redis("sentinel1", "-p", "26379", "SENTINEL", "MASTER", "coupon-primary").splitlines())] * 2)).get("num-other-sentinels", "0")) >= 2, "Sentinel discovery")
        before = info("redis-primary")
        start = time.monotonic()
        compose("kill", "-s", "SIGKILL", "redis-primary")
        promoted = wait_until(lambda: info("redis-replica") if info("redis-replica").get("role") == "master" else None, "Sentinel automatic promotion", timeout=90)
        return {"promoted": "127.0.0.1:26380", "failover_seconds": round(time.monotonic() - start, 3), "before": before, "after": promoted, "logs": save_logs("redis-sentinel-failover")}
    raise ValueError(action)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ["up", "status", "logs", "down", "redis-freeze-replica", "redis-promote", "redis-restore", "sentinel-up", "sentinel-failover", "cluster-up", "versions"]:
        sub.add_parser(name)
    kill = sub.add_parser("kafka-stop-leader")
    kill.add_argument("--topic", required=True)
    kill.add_argument("--partition", type=int, default=0)
    start = sub.add_parser("kafka-start")
    start.add_argument("broker", nargs="?", choices=["1", "2", "3", "all"], default="all")
    args = parser.parse_args()
    try:
        result = {"action": args.action, "project": "coupon-lab", "at": datetime.now(timezone.utc).isoformat(), "ok": True, **run(args)}
    except Exception as error:
        result = {"action": args.action, "project": "coupon-lab", "at": datetime.now(timezone.utc).isoformat(), "ok": False, "error": str(error)}
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    with (EVIDENCE / "events.jsonl").open("a") as stream:
        stream.write(json.dumps(result, ensure_ascii=False) + "\n")
    print(json.dumps(result, ensure_ascii=False))
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
