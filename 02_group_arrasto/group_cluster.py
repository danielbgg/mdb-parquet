#!/usr/bin/env python3
"""02a — group contratos_flat by customer on the Atlas cluster (+ simulated arrasto).

    db.contratos_flat.aggregate(grouping_pipeline + [{$out: "clientes_agrupados"}], {allowDiskUse: true})

Also runs explain("executionStats") on a small sample (pipeline without $out,
prefixed with $limit) and reports whether $group spilled to disk.

Usage:
    python 02_group_arrasto/group_cluster.py [--runs 3] [--drop-target] [--source-coll contratos_flat]
    python 02_group_arrasto/group_cluster.py --source-coll contratos_flat_1m --target clientes_agrupados_1m \
           --manifest data/sample_1m.manifest.json          # smoke test
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from bson import json_util
from pymongo.errors import PyMongoError

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(HERE))

from common.bench import RESULTS_DIR, Bench, ensure_target, mongo_client  # noqa: E402
from common.config import load_config, load_manifest  # noqa: E402
from rules import GROUPED_COLLECTION_CLUSTER, grouping_pipeline  # noqa: E402
from validate_grouped import example_mixed_customer, save_example, validate_grouped  # noqa: E402


def find_group_stats(explain: dict) -> dict:
    """Locate the $group stage in an explain output and pull its spill counters."""
    stages = explain.get("stages") or []
    if not stages and "shards" in explain:
        stages = next(iter(explain["shards"].values())).get("stages", [])
    for st in stages:
        if "$group" in st:
            keys = ("usedDisk", "spills", "spilledBytes", "spilledRecords", "spilledDataStorageSize",
                    "totalOutputDataSizeBytes", "maxUsedMemBytes", "nReturned", "executionTimeMillisEstimate")
            return {k: st[k] for k in keys if k in st}
    return {}


def explain_sample(cluster, db: str, coll: str, sample_rows: int) -> dict:
    pipeline = [{"$limit": sample_rows}] + grouping_pipeline()
    try:
        explain = cluster[db].command(
            "explain",
            {"aggregate": coll, "pipeline": pipeline, "cursor": {}, "allowDiskUse": True},
            verbosity="executionStats",
        )
    except PyMongoError as exc:
        return {"error": str(exc)}
    group = find_group_stats(explain)
    path = RESULTS_DIR / f"explain_02a_{coll}_{sample_rows}.json"
    path.write_text(json_util.dumps(explain, indent=2) + "\n", encoding="utf-8")
    return {"sample_rows": sample_rows, "group_stage": group, "file": path.name}


def run_once(args, cfg, manifest, cluster, run: int) -> bool:
    bench = Bench(
        scenario="02a_group_cluster",
        cfg=cfg,
        manifest=manifest,
        manifest_path=args.manifest,
        run=run,
        runs=args.runs,
        source=f"{cfg.target_db}.{args.source_coll}",
        target=f"{cfg.target_db}.{args.target}",
        notes=args.notes,
    )
    bench.capture_environment(cluster)
    ensure_target(cluster, cfg.target_db, args.target, args.drop_target)

    pipeline = grouping_pipeline({"$out": args.target})
    bench.details["pipeline"] = pipeline
    ok = bench.run_out(
        lambda: list(cluster[cfg.target_db][args.source_coll].aggregate(pipeline, allowDiskUse=True)),
        cluster, cfg.target_db, args.target, manifest["customers"],
    )
    if not ok:
        print(f"  ERROR: {bench.details.get('error')}")

    docs = None
    if ok:
        if run == 1 and not args.skip_explain:
            with bench.timed("explain_sample"):
                ex = explain_sample(cluster, cfg.target_db, args.source_coll, args.explain_rows)
            bench.details["explain_sample"] = ex
            g = ex.get("group_stage", {})
            print(f"  explain({args.explain_rows:,} rows) $group: usedDisk={g.get('usedDisk')} spills={g.get('spills')} "
                  f"spilledBytes={g.get('spilledBytes')} maxUsedMemBytes={g.get('maxUsedMemBytes')} -> {ex.get('file')}")
        with bench.timed("validation"):
            v = validate_grouped(cluster, cfg.target_db, args.target, manifest, args.source_coll)
        bench.details["validation"] = v
        ok = v["ok"]
        docs = v["count"]
        save_example(example_mixed_customer(cluster, cfg.target_db, args.target),
                     RESULTS_DIR / f"exemplo_cliente_misto_{args.target}.json")
    bench.save(ok, docs)
    return ok


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="02a: group by customer on the cluster.")
    p.add_argument("--source-coll", default="contratos_flat")
    p.add_argument("--target", default=GROUPED_COLLECTION_CLUSTER)
    p.add_argument("--manifest", default="data/manifest.json")
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--drop-target", action="store_true")
    p.add_argument("--explain-rows", type=int, default=200_000)
    p.add_argument("--skip-explain", action="store_true")
    p.add_argument("--notes", default="")
    p.add_argument("--config", default=None)
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    cfg.require("MONGODB_URI")
    cluster = mongo_client(cfg.mongodb_uri)
    manifest = load_manifest(args.manifest)
    print(f"pipeline: {json.dumps(grouping_pipeline({'$out': args.target}), default=str)[:600]}...")
    all_ok = True
    for run in range(1, args.runs + 1):
        all_ok &= run_once(args, cfg, manifest, cluster, run)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
