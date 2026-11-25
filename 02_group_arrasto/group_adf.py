#!/usr/bin/env python3
"""02b — load already grouped straight from S3 (steps 1 + 2 in one ADF aggregate).

    fedDB.s3import.aggregate(grouping_pipeline + [{$out: {atlas: {..., coll: "clientes_agrupados_adf"}}}],
                             {allowDiskUse: true})

Usage:
    python 02_group_arrasto/group_adf.py [--source single|split|1m] [--runs 3] [--drop-target]
    python 02_group_arrasto/group_adf.py --source 1m --manifest data/sample_1m.manifest.json   # smoke test
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO_ROOT / "01_load"))

from common.bench import RESULTS_DIR, Bench, ensure_target, mongo_client  # noqa: E402
from common.config import load_config, load_manifest  # noqa: E402
from load_flat import SOURCES, out_to_atlas  # noqa: E402
from rules import GROUPED_COLLECTION_ADF, grouping_pipeline  # noqa: E402
from validate_grouped import example_mixed_customer, save_example, validate_grouped  # noqa: E402


def run_once(args, cfg, manifest, adf, cluster, run: int) -> bool:
    source_coll = SOURCES[args.source]["coll"]
    bench = Bench(
        scenario=f"02b_group_adf_{args.source}",
        cfg=cfg,
        manifest=manifest,
        manifest_path=args.manifest,
        run=run,
        runs=args.runs,
        source=f"{cfg.adf_db}.{source_coll}",
        target=f"{cfg.target_db}.{args.target}",
        notes=args.notes,
    )
    bench.capture_environment(cluster, adf)
    ensure_target(cluster, cfg.target_db, args.target, args.drop_target)

    pipeline = grouping_pipeline(out_to_atlas(cfg, args.target))
    bench.details["pipeline"] = pipeline
    ok = bench.run_out(
        lambda: list(adf[cfg.adf_db][source_coll].aggregate(pipeline, allowDiskUse=True)),
        cluster, cfg.target_db, args.target, manifest["customers"],
    )
    if not ok:
        print(f"  ERROR (ADF): {bench.details.get('error')}")
        print("  If an operator is unsupported by Data Federation, see README 'Limitações do ADF' for the fallback.")

    docs = None
    if ok:
        with bench.timed("validation"):
            v = validate_grouped(cluster, cfg.target_db, args.target, manifest, args.flat_coll)
        bench.details["validation"] = v
        ok = v["ok"]
        docs = v["count"]
        save_example(example_mixed_customer(cluster, cfg.target_db, args.target),
                     RESULTS_DIR / f"exemplo_cliente_misto_{args.target}.json")
    bench.save(ok, docs)
    return ok


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="02b: grouped load S3 -> Atlas through Data Federation.")
    p.add_argument("--source", choices=sorted(SOURCES), default="single")
    p.add_argument("--target", default=GROUPED_COLLECTION_ADF)
    p.add_argument("--flat-coll", default="contratos_flat", help="flat collection used for the storage comparison")
    p.add_argument("--manifest", default="data/manifest.json")
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--drop-target", action="store_true")
    p.add_argument("--notes", default="")
    p.add_argument("--config", default=None)
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    cfg.require("MONGODB_URI", "ADF_URI", "ATLAS_CLUSTER")
    adf = mongo_client(cfg.adf_uri)
    cluster = mongo_client(cfg.mongodb_uri)
    manifest = load_manifest(args.manifest)
    print(f"pipeline: {json.dumps(grouping_pipeline(out_to_atlas(cfg, args.target)), default=str)[:600]}...")
    all_ok = True
    for run in range(1, args.runs + 1):
        all_ok &= run_once(args, cfg, manifest, adf, cluster, run)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
