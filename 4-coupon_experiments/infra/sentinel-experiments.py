#!/usr/bin/env python3
"""Verify real Sentinel promotion and restore the normal coupon-lab Redis roles."""
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone
import uuid

import lab


def control(action):
    result = subprocess.run([sys.executable, str(Path(__file__).with_name("lab.py")), action], capture_output=True, text=True, timeout=240)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return json.loads(result.stdout)


def main():
    record = {"name": "redis_sentinel_automatic_failover", "startedAt": datetime.now(timezone.utc).isoformat(), "passed": False}
    key = f"lab:sentinel:{uuid.uuid4().hex}"
    try:
        record["initial_restore"] = control("redis-restore")
        record["seed_ack"] = lab.redis("redis-primary", "SET", key, "replicated-before-crash")
        lab.wait_until(lambda: lab.redis("redis-replica", "GET", key) == "replicated-before-crash", "Sentinel seed replication")
        record["failover"] = control("sentinel-failover")
        record["seed_on_promoted_replica"] = lab.redis("redis-replica", "GET", key)
        record["promoted_write_ack"] = lab.redis("redis-replica", "SET", key, "accepted-after-promotion")
    except Exception as error:
        record["error"] = repr(error)
    finally:
        try:
            record["final_restore"] = control("redis-restore")
            record["value_on_restored_primary"] = lab.redis("redis-primary", "GET", key)
            lab.redis("redis-primary", "DEL", key)
        except Exception as error:
            record["restore_error"] = repr(error)
    record["passed"] = ("error" not in record and "restore_error" not in record
                        and record.get("seed_ack") == "OK"
                        and record.get("seed_on_promoted_replica") == "replicated-before-crash"
                        and record.get("promoted_write_ack") == "OK"
                        and record.get("value_on_restored_primary") == "accepted-after-promotion")
    record["finishedAt"] = datetime.now(timezone.utc).isoformat()
    record["scope"] = "Three Sentinel processes, quorum2, actual primary SIGKILL and automatic replica promotion; docker-exec clients verify data; does not test Spring Sentinel client redirection."
    output = lab.LAB / "results" / "redis-sentinel.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"passed": record["passed"], "output": str(output), "failover_seconds": record.get("failover", {}).get("failover_seconds"), "error": record.get("error"), "restore_error": record.get("restore_error")}))
    raise SystemExit(0 if record["passed"] else 1)


if __name__ == "__main__":
    main()
