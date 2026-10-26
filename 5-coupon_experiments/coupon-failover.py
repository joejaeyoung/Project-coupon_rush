#!/usr/bin/env python3
"""Real Redis failover of coupon stock/dedup state; only controls coupon-lab services.

Run separately from other workloads: the script kills and restores the lab Redis
primary. It intentionally demonstrates lost reservation state, then verifies the
shared MySQL final issuance cap. It does not claim to repair the lost Redis state.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import runpy
import time

ROOT = Path(__file__).resolve().parent
support = runpy.run_path(str(ROOT / "ha-experiments.py"), run_name="coupon_failover_support")
api, infra, command, redis = (support[name] for name in ("api", "infra", "command", "redis"))
COMPOSE = support["COMPOSE"]


def cli(service, *args):
    return json.loads(command(*COMPOSE, "exec", "-T", service, "redis-cli", "--json", "-e", *args))


def wait_for(predicate, description, timeout=45):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as error:
            last = repr(error)
        time.sleep(.2)
    raise AssertionError(f"Timeout waiting for {description}; last={last}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:28080")
    parser.add_argument("--output", default=str(ROOT / "results" / "coupon-failover.json"))
    parser.add_argument("--skip-capacity-probe", action="store_true", help="Skip the two additional users after the lost-reservation proof")
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    record = {"name": "coupon_reservation_loss_during_redis_failover", "startedAt": datetime.now(timezone.utc).isoformat(),
              "strategy": "LUA_REDIS", "initialStock": 2, "checks": [], "expectedObservations": [], "http": [], "passed": False,
              "scope": ["Real Lua coupon reservation, actual replica network isolation, actual primary SIGKILL, manual replica promotion",
                        "HTTP AFTER_RESERVE is an injected API exception after Redis reservation and its SQL tracking update",
                        "The lost reservation is SQL reserved=true; this Lua strategy does not create a DB Outbox reservation",
                        "MySQL shared final capacity guard limits issuance but does not automatically rebuild Redis membership",
                        "A passed expected observation means a specified inconsistency was reproduced, not that failover is lossless"]}
    run_id = None

    def save():
        temporary = output.with_name(output.name + ".tmp")
        temporary.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")
        temporary.replace(output)

    def check(name, passed, actual=None, expected=None, observation=False):
        entry = {"name": name, "passed": bool(passed), "actual": actual, "expected": expected}
        record["expectedObservations" if observation else "checks"].append(entry)
        save()
        if not passed:
            raise AssertionError(f"{name}: actual={actual!r}; expected={expected!r}")

    def call(method, suffix, body=None, allowed=(200, 202)):
        result = api(args.base_url, method, suffix, body)
        record["http"].append({"method": method, "path": suffix, "payload": body, **result})
        save()
        if result["http"] not in allowed:
            raise AssertionError(f"Unexpected HTTP {result['http']}: {suffix}: {result['body']}")
        return result

    def snapshot():
        response = api(args.base_url, "GET", f"/lab/runs/{run_id}")
        if response["http"] != 200:
            raise AssertionError(response)
        return response["body"]

    def state(service):
        prefix = "lab:{" + run_id + ":1}:"
        stock = cli(service, "GET", prefix + "stock")
        return {"service": service, "stock": None if stock is None else int(stock),
                "users": sorted(cli(service, "SMEMBERS", prefix + "users")),
                "lostRequestSequence": cli(service, "HGET", prefix + "requests", "lost-reservation")}

    def issued_snapshot(count):
        current = snapshot()
        return current if current["issued"] == count and current["pending"] == 0 else None

    try:
        record["initialRestore"] = infra("redis-restore")
        wait_for(lambda: redis(26379, "PING") == "PONG", "initial primary host endpoint")
        created = call("POST", "/lab/runs", {"strategy": "LUA_REDIS", "stock": 2, "coupons": 1, "partitions": 3, "workers": 1}, (200, 201))
        run_id = record["runId"] = created["body"]["runId"]
        path = f"/lab/runs/{run_id}"
        call("POST", path + "/control", {"paused": True})
        baseline = wait_for(lambda: (value if (value := state("redis-replica"))["stock"] == 2 else None), "initial stock replicated")
        record["replicaBeforeIsolation"] = baseline
        check("initial_stock_replicated", baseline["stock"] == 2 and baseline["users"] == [], baseline, "stock2, no users")

        record["isolation"] = infra("redis-freeze-replica")
        call("POST", path + "/requests", {"requestId": "lost-reservation", "couponId": 1, "userId": "lost-user", "fault": "AFTER_RESERVE"}, (503,))
        record["primaryBeforeKill"] = state("redis-primary")
        record["snapshotBeforeKill"] = snapshot()
        pending = record["snapshotBeforeKill"]
        lost_row = next(row for row in pending["requests"] if row["requestId"] == "lost-reservation")
        check("lua_reservation_preceded_failure", record["primaryBeforeKill"]["stock"] == 1
              and record["primaryBeforeKill"]["users"] == ["lost-user"]
              and record["primaryBeforeKill"]["lostRequestSequence"] is not None,
              record["primaryBeforeKill"], "stock1, lost-user recorded, request sequence recorded")
        check("sql_tracks_unpublished_reservation", pending["issued"] == 0 and pending["pending"] == 1
              and lost_row["reserved"] is True and lost_row["published"] is False
              and lost_row["faultConsumed"] is True,
              {"issued": pending["issued"], "pending": pending["pending"], "request": lost_row}, "reserved pending request; nothing issued or published")

        record["promotion"] = infra("redis-promote")
        record["promotedRedis"] = state("redis-replica")
        # The API snapshot still reads SQL while its original Redis address is down;
        # any redisError in that snapshot is a connection observation, not data loss.
        record["sqlWhilePrimaryDown"] = snapshot()
        promoted = record["promotedRedis"]
        check("promoted_replica_lost_stock_and_dedup", promoted["stock"] == 2
              and promoted["users"] == [] and promoted["lostRequestSequence"] is None,
              promoted, "stock2, no user or request reservation", observation=True)
        check("sql_pending_survives_redis_failure", record["sqlWhilePrimaryDown"]["issued"] == 0
              and record["sqlWhilePrimaryDown"]["pending"] == 1,
              {"issued": record["sqlWhilePrimaryDown"]["issued"], "pending": record["sqlWhilePrimaryDown"]["pending"]}, "issued0,pending1")

        record["restoreAfterPromotion"] = infra("redis-restore")
        wait_for(lambda: redis(26379, "PING") == "PONG", "restored Redis host endpoint")
        wait_for(lambda: (value if (value := snapshot())["perCoupon"][0].get("redisStock") == 2 else None), "Spring Redis reconnection")
        call("POST", path + "/control", {"paused": False})
        call("POST", path + "/requests", {"requestId": "lost-reservation", "couponId": 1, "userId": "lost-user"})
        record["afterExplicitRetry"] = wait_for(lambda: issued_snapshot(1), "lost request issuance after explicit retry")
        record["redisAfterExplicitRetry"] = state("redis-primary")
        after = record["afterExplicitRetry"]
        check("retry_issues_once_in_mysql", after["issued"] == after["ledgerIssued"] == 1
              and after["ledgerDuplicateUsers"] == 0 and after["perCoupon"][0]["dbRemaining"] == 1,
              {"issued": after["issued"], "ledgerIssued": after["ledgerIssued"], "dbRemaining": after["perCoupon"][0]["dbRemaining"]}, "one issuance, one remaining")
        check("retry_does_not_rebuild_lost_redis_reservation", record["redisAfterExplicitRetry"]["stock"] == 2
              and record["redisAfterExplicitRetry"]["users"] == [], record["redisAfterExplicitRetry"],
              "Redis still stock2/users0 while MySQL issued1", observation=True)

        if not args.skip_capacity_probe:
            call("POST", path + "/requests", {"requestId": "additional-1", "couponId": 1, "userId": "additional-user-1"})
            wait_for(lambda: issued_snapshot(2), "second final issuance")
            call("POST", path + "/requests", {"requestId": "additional-2", "couponId": 1, "userId": "additional-user-2"}, (200, 202, 409))
            record["afterCapacityProbe"] = wait_for(lambda: (value if (value := snapshot())["pending"] == 0
                                                    and value["received"] == 3 else None), "capacity rejection settled")
            record["redisAfterCapacityProbe"] = state("redis-primary")
            final = record["afterCapacityProbe"]
            rejected = [row for row in final["requests"] if row["status"] == "REJECTED"]
            issued_users = sorted(row["userId"] for row in final["requests"] if row["status"] == "ISSUED")
            record["finalIssuedUsers"] = issued_users
            check("final_mysql_capacity_guard", final["issued"] == final["ledgerIssued"] == 2
                  and final["rejected"] == 1 and final["perCoupon"][0]["dbRemaining"] == 0
                  and final["ledgerDuplicateUsers"] == 0,
                  {"issued": final["issued"], "rejected": final["rejected"], "ledgerIssued": final["ledgerIssued"],
                   "dbRemaining": final["perCoupon"][0]["dbRemaining"], "ledgerDuplicateUsers": final["ledgerDuplicateUsers"]}, "issued2,rejected1,remaining0,no duplicates")
            check("excess_redis_reservation_rejected_by_db", len(rejected) == 1
                  and rejected[0]["requestId"] == "additional-2" and rejected[0]["reason"] == "DB_CAPACITY_GUARD",
                  rejected, "additional-2 rejected by final DB capacity guard")
            check("equal_counts_do_not_prove_membership_consistency", len(record["redisAfterCapacityProbe"]["users"]) == len(issued_users)
                  and record["redisAfterCapacityProbe"]["users"] != issued_users,
                  {"redisUsers": record["redisAfterCapacityProbe"]["users"], "issuedUsers": issued_users},
                  "same count but different Redis and issued user identities", observation=True)
    except Exception as error:
        record["error"] = repr(error)
    finally:
        if run_id:
            try:
                record["stopWorkers"] = api(args.base_url, "POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
                if record["stopWorkers"]["http"] != 200:
                    raise RuntimeError(record["stopWorkers"])
            except Exception as error:
                record["stopError"] = repr(error)
        try:
            record["finalRestore"] = infra("redis-restore")
            record["restoredHostReachable"] = wait_for(lambda: redis(26379, "PING") == "PONG", "final original primary endpoint")
        except Exception as error:
            record["restoreError"] = repr(error)
        record["finishedAt"] = datetime.now(timezone.utc).isoformat()
        checks = record["checks"] + record["expectedObservations"]
        record["passed"] = bool(checks) and all(item["passed"] for item in checks) and not any(
            field in record for field in ("error", "stopError", "restoreError"))
        save()
    print(json.dumps({"passed": record["passed"], "runId": run_id, "output": str(output),
                      "checks": len(record["checks"]), "expectedObservations": len(record["expectedObservations"]),
                      "error": record.get("error"), "restoreError": record.get("restoreError")}))
    raise SystemExit(0 if record["passed"] else 1)


if __name__ == "__main__":
    main()
