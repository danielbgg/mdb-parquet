#!/usr/bin/env python3
"""Replace estimated $out timings (01_load / 02b, measured by polling) with the
exact server-side duration from the ADF $queryHistory.

Matches each results/*.json whose `timings.main_is_estimate` is set with the
history entry that (a) was issued by appName ratings-poc, (b) writes to the
same target collection via $out.atlas and (c) started inside the run window.
Updates the JSON (timings.main, main_source, notes) and results/summary.csv.

    python scripts/reconcile_query_history.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.bench import RESULTS_DIR, SUMMARY_CSV, mongo_client  # noqa: E402
from common.config import load_config  # noqa: E402


def out_atlas_coll(q: dict) -> str | None:
    for st in q.get("query", []):
        if isinstance(st, dict) and "$out" in st and isinstance(st["$out"], dict):
            return st["$out"].get("atlas", {}).get("coll")
    return None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    cfg = load_config()
    cfg.require("ADF_URI")
    adf = mongo_client(cfg.adf_uri)
    history = list(adf["admin"].aggregate([{"$queryHistory": {}}, {"$match": {"appName": "ratings-poc"}}]))
    outs = [(q, out_atlas_coll(q)) for q in history]
    outs = [(q, c) for q, c in outs if c]
    print(f"{len(outs)} $out-to-Atlas entries in $queryHistory")

    updated = {}
    for f in sorted(RESULTS_DIR.glob("*_run*_*.json")):
        rec = json.loads(f.read_text(encoding="utf-8"))
        if not rec.get("timings", {}).get("main_is_estimate"):
            continue
        target_coll = rec["target"].split(".")[-1]
        start = datetime.fromisoformat(rec["started_at"])
        end = datetime.fromisoformat(rec["finished_at"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
            end = end.replace(tzinfo=timezone.utc)
        cands = [q for q, c in outs if c == target_coll and q.get("endTime")
                 and start - timedelta(seconds=30) <= q["startTime"].replace(tzinfo=timezone.utc) <= end]
        if len(cands) != 1:
            print(f"  {f.name}: {len(cands)} candidate(s) for {target_coll} in [{start:%H:%M:%S}, {end:%H:%M:%S}] — skipped")
            continue
        q = cands[0]
        exact = round((q["endTime"] - q["startTime"]).total_seconds(), 3)
        old = rec["timings"]["main"]
        print(f"  {f.name}: {target_coll} estimated {old}s -> exact {exact}s (ok={q['ok']}, error={q['error']!r})")
        rec["timings"]["main_polling_estimate"] = old
        rec["timings"]["main"] = exact
        rec["timings"]["main_source"] = "queryHistory"
        rec["timings"].pop("main_is_estimate", None)
        rec["seconds"] = exact
        rec["notes"] = "tempo exato do $queryHistory do ADF (reconciliado; estimativa original por polling: %.1fs)" % old
        rec["details"]["query_history"] = {k: (v.isoformat() if isinstance(v, datetime) else v)
                                           for k, v in q.items() if k != "query"}
        rec["details"]["query_history"]["opid"] = str(rec["details"]["query_history"].get("opid"))
        rec["details"]["query_history"]["_id"] = str(rec["details"]["query_history"].get("_id"))
        updated[f.name] = rec
        if not args.dry_run:
            f.write_text(json.dumps(rec, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")

    if updated and not args.dry_run:
        rows = list(csv.DictReader(SUMMARY_CSV.open(encoding="utf-8")))
        for r in rows:
            rec = updated.get(r["result_file"])
            if rec:
                r["seconds"] = rec["seconds"]
                r["notes"] = rec["notes"]
        with SUMMARY_CSV.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"summary.csv updated ({len(updated)} row(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())
