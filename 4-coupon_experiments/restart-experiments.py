#!/usr/bin/env python3
"""SIGKILL/restart recovery and concurrent cancellation against the real local lab."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import runpy
import subprocess
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
support = runpy.run_path(str(ROOT / "ha-experiments.py"), run_name="coupon_ha_support")
api, wait_snapshot = support["api"], support["wait_snapshot"]
ACTIVE_RUNS = set()


def stream_command(run_id, *args):
    key = "lab:{" + run_id + ":1}:events"
    translated = [key if a == "STREAM_KEY" else a for a in args]
    text = support["command"](*support["COMPOSE"], "exec", "-T", "redis-primary", "redis-cli", "--json", *translated)
    return json.loads(text)


def lifecycle(action):
    result = subprocess.run(["python3", str(ROOT / "app.py"), action], capture_output=True, text=True, timeout=90)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout.strip()


def recovery(base):
    cases = []
    strategies = ["SPLIT_REDIS", "LUA_REDIS", "KAFKA_FIRST", "DB_OUTBOX", "REDIS_STREAM"]
    for strategy in strategies:
        created = api(base, "POST", "/lab/runs", {"strategy": strategy, "stock": 10, "coupons": 1, "workers": 1, "partitions": 3})
        assert created["http"] in (200, 201), created
        run_id = created["body"]["runId"]
        ACTIVE_RUNS.add(run_id)
        api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True})
        body = {"requestId": "restart-pending", "couponId": 1, "userId": "restart-user"}
        if strategy != "KAFKA_FIRST":
            body["fault"] = "AFTER_RESERVE"
        submitted = api(base, "POST", f"/lab/runs/{run_id}/requests", body)
        assert submitted["http"] in (202, 503), submitted
        case = {"strategy": strategy, "runId": run_id, "submission": submitted,
                "beforeKill": api(base, "GET", f"/lab/runs/{run_id}")["body"]}
        if strategy == "REDIS_STREAM":
            case["deliveredWithoutAck"] = stream_command(run_id, "XREADGROUP", "GROUP", "relay", "lab-relay", "COUNT", "1", "STREAMS", "STREAM_KEY", ">")
            case["pendingBeforeKill"] = stream_command(run_id, "XPENDING", "STREAM_KEY", "relay")
            assert case["pendingBeforeKill"][0] == 1, case["pendingBeforeKill"]
        cases.append(case)
    process = {"kill": lifecycle("crash"), "restart": lifecycle("start")}
    for case in cases:
        run_id = case["runId"]
        case["afterRestartBeforeResume"] = api(base, "GET", f"/lab/runs/{run_id}")["body"]
        restored = case["afterRestartBeforeResume"]
        assert restored["paused"] is True and restored["workers"] == 1 and restored["issued"] == 0, restored
        api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": False})
    time.sleep(5)
    for case in cases:
        run_id = case["runId"]
        reliable = case["strategy"] in {"KAFKA_FIRST", "DB_OUTBOX", "REDIS_STREAM"}
        after = (wait_snapshot(base, run_id, 1) if reliable else api(base, "GET", f"/lab/runs/{run_id}")["body"])
        case["afterRecovery"] = after
        case["passed"] = (after["issued"] == after["ledgerIssued"] == (1 if reliable else 0)
                          and after["ledgerDuplicateUsers"] == 0
                          and after["pending"] == (0 if reliable else 1))
        if case["strategy"] == "REDIS_STREAM":
            deadline = time.monotonic() + 10
            while True:
                case["pendingAfterRecovery"] = stream_command(run_id, "XPENDING", "STREAM_KEY", "relay")
                if case["pendingAfterRecovery"][0] == 0 or time.monotonic() > deadline:
                    break
                time.sleep(.1)
            case["passed"] = case["passed"] and case["pendingAfterRecovery"][0] == 0
        case["expectedObservation"] = "automatically recovered" if reliable else "reservation remains pending until explicit retry/reconciliation"
        if not reliable:
            case["retry"] = api(base, "POST", f"/lab/runs/{run_id}/requests",
                                 {"requestId": "restart-pending", "couponId": 1, "userId": "restart-user"})
            case["afterExplicitRetry"] = wait_snapshot(base, run_id, 1)
            retried = case["afterExplicitRetry"]
            case["passed"] = (case["passed"] and retried["issued"] == retried["ledgerIssued"] == 1
                              and retried["pending"] == 0 and retried["ledgerDuplicateUsers"] == 0)
        api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
    return {"name": "jvm_sigkill_recovery", "process": process, "cases": cases, "passed": all(c["passed"] for c in cases)}


def cancel_race(base):
    count = 60
    created = api(base, "POST", "/lab/runs", {"strategy": "DB_OUTBOX", "stock": count, "coupons": 1, "partitions": 3, "workers": 3})
    assert created["http"] in (200, 201), created
    run_id = created["body"]["runId"]
    ACTIVE_RUNS.add(run_id)
    def race(index):
        request_id = f"cancel-race-{index}"
        issue = api(base, "POST", f"/lab/runs/{run_id}/requests",
                    {"requestId": request_id, "couponId": 1, "userId": request_id})
        # Different client cancellation times allow both sides of the real DB race to win.
        if index % 2:
            time.sleep(.6)
        cancel = api(base, "POST", f"/lab/runs/{run_id}/cancel", {"requestId": request_id})
        return {"requestId": request_id, "issue": issue, "cancel": cancel}
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        observations = list(pool.map(race, range(count)))
    deadline = time.monotonic() + 60
    while True:
        snapshot = api(base, "GET", f"/lab/runs/{run_id}")["body"]
        if snapshot["pending"] == 0:
            break
        if time.monotonic() > deadline:
            raise AssertionError("Cancellation race never settled")
        time.sleep(.2)
    statuses = {r["requestId"]: r["status"] for r in snapshot["requests"]}
    cancelled_ids = {r["requestId"] for r in observations if r["cancel"]["http"] == 200}
    checks = {
        "all_submissions_accepted": all(r["issue"]["http"] in (200, 202) for r in observations),
        "expected_cancel_responses": all(r["cancel"]["http"] in (200, 409) for r in observations),
        "successful_cancel_never_issued": all(statuses[i] == "CANCELLED" for i in cancelled_ids),
        "every_request_terminal": snapshot["issued"] + snapshot["cancelled"] == count,
        "remaining_plus_issued_equals_total": snapshot["perCoupon"][0]["dbRemaining"] + snapshot["issued"] == count,
        "no_duplicate_recipients": snapshot.get("ledgerDuplicateUsers", -1) == 0,
        "ledger_matches_request_results": snapshot["ledgerIssued"] == snapshot["issued"],
    }
    api(base, "POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
    return {"name": "cancel_vs_issue_db_transaction", "runId": run_id, "observations": observations,
            "snapshot": snapshot, "checks": checks,
            "bothOutcomesObserved": snapshot["issued"] > 0 and snapshot["cancelled"] > 0,
            "passed": all(checks.values())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:28080")
    parser.add_argument("--output", default=str(ROOT / "results/process-recovery.json"))
    parser.add_argument("--scenario", choices=["all", "restart", "cancel"], default="all")
    args = parser.parse_args()
    result = {"startedAt": datetime.now(timezone.utc).isoformat(), "scenarios": []}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    for key, function in [("restart", recovery), ("cancel", cancel_race)]:
        if args.scenario not in ("all", key):
            continue
        try:
            row = function(args.base_url)
        except Exception as error:
            row = {"name": key, "passed": False, "error": repr(error)}
        finally:
            cleanup_errors = []
            try:
                lifecycle("start")
                for run_id in sorted(ACTIVE_RUNS):
                    response = api(args.base_url, "POST", f"/lab/runs/{run_id}/control", {"paused": True, "workers": 0})
                    if response["http"] != 200:
                        cleanup_errors.append(response)
            except Exception as error:
                cleanup_errors.append(repr(error))
            ACTIVE_RUNS.clear()
        row["cleanupErrors"] = cleanup_errors
        if cleanup_errors:
            row["passed"] = False
        result["scenarios"].append(row)
        output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
        print(json.dumps({"name": row["name"], "passed": row["passed"], "error": row.get("error")}))
    result["passed"] = all(row["passed"] for row in result["scenarios"])
    result["finishedAt"] = datetime.now(timezone.utc).isoformat()
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
