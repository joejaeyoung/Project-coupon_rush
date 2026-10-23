#!/usr/bin/env python3
"""Measure real Redis Cluster Lua roundtrips, ownership and key-slot constraints."""
from __future__ import annotations

import argparse
import binascii
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
import socket
import statistics
import time


class RedisError(RuntimeError):
    pass


class Redis:
    def __init__(self, port: int):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=15)
        self.stream = self.socket.makefile("rb")

    def close(self):
        self.stream.close()
        self.socket.close()

    def read(self):
        prefix = self.stream.read(1)
        line = self.stream.readline().rstrip(b"\r\n")
        if prefix == b"+":
            return line.decode()
        if prefix == b"-":
            raise RedisError(line.decode())
        if prefix == b":":
            return int(line)
        if prefix == b"$":
            size = int(line)
            if size < 0:
                return None
            result = self.stream.read(size)
            self.stream.read(2)
            return result.decode()
        if prefix == b"*":
            return [self.read() for _ in range(int(line))]
        raise RedisError(f"Unexpected RESP prefix: {prefix!r}")

    def call(self, *args):
        encoded = [str(arg).encode() for arg in args]
        wire = b"*" + str(len(encoded)).encode() + b"\r\n"
        wire += b"".join(b"$" + str(len(arg)).encode() + b"\r\n" + arg + b"\r\n" for arg in encoded)
        self.socket.sendall(wire)
        return self.read()


LUA = """
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then return 0 end
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then return -1 end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
""".strip()
SHA = hashlib.sha1(LUA.encode()).hexdigest()


def slot(tag: str) -> int:
    return binascii.crc_hqx(tag.encode(), 0) % 16384


def command_calls(client: Redis) -> int:
    for line in client.call("INFO", "commandstats").splitlines():
        if line.startswith("cmdstat_evalsha:"):
            return int(line.split("calls=", 1)[1].split(",", 1)[0])
    return 0


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operations", type=int, default=12000)
    parser.add_argument("--workers", type=int, default=18)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent.parent / "results" / "redis-cluster.json"))
    args = parser.parse_args()
    ports = [27379, 27380, 27381]
    clients = [Redis(port) for port in ports]
    ids = [client.call("CLUSTER", "MYID") for client in clients]
    ranges = []
    for item in clients[0].call("CLUSTER", "SLOTS"):
        ranges.append({"start": item[0], "end": item[1], "owner": ids.index(item[2][2]), "node_id": item[2][2]})
    for client in clients:
        assert client.call("SCRIPT", "LOAD", LUA) == SHA

    def owner(tag):
        index = slot(tag)
        return next(item["owner"] for item in ranges if item["start"] <= index <= item["end"])

    run_id = str(time.time_ns())
    tags = [[], [], []]
    candidate = 0
    while min(map(len, tags)) < 36:
        tag = f"cluster-experiment-{run_id}-{candidate}"
        target = owner(tag)
        if len(tags[target]) < 36:
            tags[target].append(tag)
        candidate += 1

    first, second = tags[0][0], tags[1][0]
    try:
        clients[0].call("EVAL", "return redis.call('EXISTS', KEYS[1], KEYS[2])", "2", f"stock:{{{first}}}", f"users:{{{second}}}")
        crossslot = {"rejected": False}
    except RedisError as error:
        crossslot = {"rejected": str(error).startswith("CROSSSLOT"), "error": str(error), "slots": [slot(first), slot(second)]}
    same_slot_keys = [f"stock:{{{first}}}", f"users:{{{first}}}"]
    clients[0].call("SET", same_slot_keys[0], "1")
    same_slot = {"keys": same_slot_keys, "slots": [clients[0].call("CLUSTER", "KEYSLOT", key) for key in same_slot_keys], "lua_result": clients[0].call("EVAL", LUA, "2", *same_slot_keys, "proof-user")}
    clients[0].call("DEL", *same_slot_keys)
    assert crossslot["rejected"] and same_slot["lua_result"] == 1

    rows = []
    rng = random.Random(20261022)
    cases = [(repeat, masters, distribution) for repeat in range(args.repeats) for masters in [1, 3] for distribution in ["hot", "uniform", "90hot"]]
    rng.shuffle(cases)
    for repeat, masters, distribution in cases:
        # Same number of coupon keys and worker connections in both ownership cases.
        coupons = tags[0][:36] if masters == 1 else [tags[index % 3][index // 3] for index in range(36)]
        keys = [(f"stock:{{{tag}}}", f"users:{{{tag}}}") for tag in coupons]
        counts = [0] * len(coupons)
        order = []
        schedule = random.Random(932)
        for index in range(args.operations):
            coupon = 0 if distribution == "hot" else index % 36 if distribution == "uniform" else 0 if schedule.random() < 0.9 else schedule.randrange(1, 36)
            counts[coupon] += 1
            order.append(coupon)
        for index, (stock, users) in enumerate(keys):
            client = clients[owner(coupons[index])]
            client.call("DEL", stock, users)
            client.call("SET", stock, counts[index])
        before = [command_calls(client) for client in clients]

        def worker(worker_index):
            connections = [Redis(port) for port in ports]
            latencies, accepted = [], 0
            try:
                for index in range(worker_index, args.operations, args.workers):
                    coupon = order[index]
                    connection = connections[owner(coupons[coupon])]
                    start = time.perf_counter()
                    answer = connection.call("EVALSHA", SHA, "2", *keys[coupon], f"user-{index}")
                    latencies.append((time.perf_counter() - start) * 1000)
                    accepted += answer == 1
            finally:
                for connection in connections:
                    connection.close()
            return latencies, accepted

        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            parts = list(executor.map(worker, range(args.workers)))
        elapsed = time.perf_counter() - started
        after = [command_calls(client) for client in clients]
        remaining, users = 0, 0
        for index, (stock, user_set) in enumerate(keys):
            client = clients[owner(coupons[index])]
            remaining += int(client.call("GET", stock))
            users += client.call("SCARD", user_set)
            client.call("DEL", stock, user_set)
        latencies = [value for latency, _ in parts for value in latency]
        accepted = sum(count for _, count in parts)
        row = {"repeat": repeat + 1, "available_masters": 3, "coupon_owners": masters, "distribution": distribution, "operations": args.operations, "workers": args.workers, "elapsed_seconds": round(elapsed, 6), "operations_per_second": round(args.operations / elapsed, 2), "roundtrip_p50_ms": round(percentile(latencies, 0.5), 4), "roundtrip_p95_ms": round(percentile(latencies, 0.95), 4), "evalsha_calls_by_port": {str(port): end - begin for port, begin, end in zip(ports, before, after)}, "accepted": accepted, "unique_users": users, "remaining_stock": remaining}
        row["pass"] = accepted == args.operations == users == sum(row["evalsha_calls_by_port"].values()) and remaining == 0
        assert row["pass"], row
        rows.append(row)
        print(json.dumps(row), flush=True)
    summary = []
    for distribution in ["hot", "uniform", "90hot"]:
        for masters in [1, 3]:
            subset = [row for row in rows if row["distribution"] == distribution and row["coupon_owners"] == masters]
            summary.append({"distribution": distribution, "coupon_owners": masters, "median_operations_per_second": round(statistics.median(row["operations_per_second"] for row in subset), 2), "median_roundtrip_p95_ms": round(statistics.median(row["roundtrip_p95_ms"] for row in subset), 4)})
    result = {"at": datetime.now(timezone.utc).isoformat(), "method": "Persistent TCP RESP clients; identical Lua stock/duplicate-check operation; equal operations, workers and connection count; randomized case order; no injected work or latency; three masters always running, keys placed on one or three masters", "limits": ["Command RTT only, not HTTP end-to-end latency", "Local Docker shares CPU with other services; client and network can bottleneck", "A single coupon keeps both keys in one slot and cannot spread its atomic Lua work over masters", "Three masters without replicas test distribution, not high availability", "Host clients explicitly map slots to exposed ports; no host-visible cluster redirection"], "crossslot": crossslot, "same_hash_tag": same_slot, "slot_ranges": ranges, "summary": summary, "measurements": rows, "pass": all(row["pass"] for row in rows)}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    for client in clients:
        client.close()
    print(json.dumps({"output": str(output), "summary": summary, "pass": result["pass"]}), flush=True)


if __name__ == "__main__":
    main()
