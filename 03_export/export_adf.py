#!/usr/bin/env python3
"""03_export — export to a single Parquet file on S3 through Atlas Data Federation.

Scenarios (all run on the ADF, reading the Atlas cluster collections mirrored
in the federated database, writing to s3://<S3_BUCKET>/output/<scenario>/<ts>/):

  03a        contratos_flat      -> $replaceWith(15 cols)            -> $out s3 parquet, maxFileSize 20GiB
  03b        clientes_agrupados  -> $unwind + $replaceWith(17 cols)  -> $out s3 parquet, maxFileSize 20GiB
  03b-multi  same as 03b with the default maxFileSize (many files)
  ensaio     03b on clientes_agrupados_1m (smoke test)

The aggregate is launched with background=True; duration comes from
$queryHistory (server-side start/end). After each run scripts/validate_output.py
is executed against the produced file.

Usage:
    python 03_export/export_adf.py --scenario ensaio
    python 03_export/export_adf.py --scenario 03a --runs 3
    python 03_export/export_adf.py --scenario 03b --runs 3
    python 03_export/export_adf.py --scenario 03b-multi
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(HERE))

from common.bench import RESULTS_DIR, Bench, human_bytes, mongo_client  # noqa: E402
from common.config import load_config, load_manifest  # noqa: E402
from export_common import (  # noqa: E402
    list_prefix,
    out_s3,
    pipeline_flat,
    pipeline_grouped,
    run_background_out,
    s3_client,
)

SINGLE_FILE = "20GiB"

SCENARIOS = {
    "03a": {"coll": "contratos_flat", "pipeline": pipeline_flat, "max_file_size": SINGLE_FILE, "grouped": False,
            "label": "03a plano via ADF (15 colunas)"},
    "03b": {"coll": "clientes_agrupados", "pipeline": pipeline_grouped, "max_file_size": SINGLE_FILE, "grouped": True,
            "label": "03b agrupado via ADF (17 colunas, 1 arquivo)"},
    "03b-multi": {"coll": "clientes_agrupados", "pipeline": pipeline_grouped, "max_file_size": None, "grouped": True,
                  "label": "03b-multi agrupado via ADF (maxFileSize padrão)"},
    "ensaio": {"coll": "clientes_agrupados_1m", "pipeline": pipeline_grouped, "max_file_size": SINGLE_FILE, "grouped": True,
               "label": "ensaio 03b com 1M"},
}


def run_once(args, cfg, manifest, adf, cluster, s3, run: int) -> bool:
    sc = SCENARIOS[args.scenario]
    coll = args.source_coll or sc["coll"]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"output/{args.scenario}/{ts}/"
    filename = prefix + "ratings_export"  # ADF appends .N.parquet
    scenario_name = f"03_export_{args.scenario.replace('-', '_')}"
    bench = Bench(scenario=scenario_name, cfg=cfg, manifest=manifest, manifest_path=args.manifest, run=run,
                  runs=args.runs, source=f"{cfg.target_db}.{coll}", target=f"s3://{cfg.s3_bucket}/{prefix}",
                  notes=args.notes)
    bench.capture_environment(cluster, adf)

    pipeline = sc["pipeline"]() + [out_s3(cfg.s3_bucket, cfg.aws_region, filename, args.max_file_size or sc["max_file_size"],
                                          args.compression, args.max_row_group_size)]
    bench.details["pipeline"] = pipeline
    bench.details["s3_prefix"] = prefix
    print(f"[{scenario_name} run {run}/{args.runs}] {cfg.target_db}.{coll} -> s3://{cfg.s3_bucket}/{prefix}")
    print(f"  $out: {json.dumps(pipeline[-1]['$out'])}")

    res = run_background_out(adf, cfg.target_db, coll, pipeline, filename, s3, cfg.s3_bucket, poll_seconds=args.poll_seconds)
    bench.timings["main"] = res.seconds
    bench.timings["main_source"] = res.source
    if res.source != "queryHistory":
        bench.timings["main_is_estimate"] = True
        bench.notes = (bench.notes + "; " if bench.notes else "") + f"tempo estimado ({res.source})"
    bench.details["query_history"] = res.history
    if not res.ok:
        bench.details["error"] = res.error
        print(f"  ERROR: {res.error}")
        bench.save(False, None)
        return False
    print(f"[{scenario_name} run {run}/{args.runs}] main: {res.seconds:,.1f}s (source: {res.source}; "
          f"{res.started_at.isoformat(timespec='seconds') if res.started_at else '?'} -> "
          f"{res.ended_at.isoformat(timespec='seconds') if res.ended_at else '?'})")

    objs = list_prefix(s3, cfg.s3_bucket, prefix)
    files = [{"key": o["Key"], "size_bytes": o["Size"]} for o in objs]
    bench.details["files"] = files
    total = sum(f["size_bytes"] for f in files)
    print(f"  S3: {len(files)} file(s), {human_bytes(total)}")
    for f in files[:10]:
        print(f"    {f['key']}  {human_bytes(f['size_bytes'])}")
    if len(files) > 10:
        print(f"    ... (+{len(files) - 10})")

    ok = True
    docs = None
    if not args.skip_validation:
        report = RESULTS_DIR / f"validate_output_{args.scenario}_{ts}.json"
        cmd = [sys.executable, str(REPO_ROOT / "scripts" / "validate_output.py"),
               "--s3-prefix", f"s3://{cfg.s3_bucket}/{prefix}", "--manifest", args.manifest,
               "--input", args.input_parquet, "--report", str(report)]
        if sc["grouped"]:
            cmd.append("--grouped")
        if args.allow_multiple_files or sc["max_file_size"] is None:
            cmd.append("--allow-multiple-files")
        if args.skip_row_compare:
            cmd.append("--skip-row-compare")
        with bench.timed("validation"):
            print("  $ " + " ".join(cmd[1:]))
            rc = subprocess.call(cmd)
        if report.exists():
            v = json.loads(report.read_text(encoding="utf-8"))
            bench.details["validation"] = v
            docs = v.get("rows")
            ok = v.get("ok", False) and rc == 0
        else:
            ok = False
            bench.details["error"] = "validation report not produced"
    bench.save(ok, docs)
    return ok


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="03: export to Parquet on S3 via Data Federation $out (background).")
    p.add_argument("--scenario", choices=sorted(SCENARIOS), required=True)
    p.add_argument("--source-coll", default=None)
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--manifest", default=None, help="default: data/manifest.json (ensaio: data/sample_1m.manifest.json)")
    p.add_argument("--input-parquet", default=None, help="local input Parquet for the row-by-row comparison")
    p.add_argument("--max-file-size", default=None, help="override maxFileSize (e.g. 20GiB)")
    p.add_argument("--max-row-group-size", default=None, help="override maxRowGroupSize (default: ADF default 128MiB)")
    p.add_argument("--compression", default="snappy")
    p.add_argument("--poll-seconds", type=float, default=2.0)
    p.add_argument("--allow-multiple-files", action="store_true")
    p.add_argument("--skip-validation", action="store_true")
    p.add_argument("--skip-row-compare", action="store_true")
    p.add_argument("--notes", default="")
    p.add_argument("--config", default=None)
    args = p.parse_args(argv)

    if args.manifest is None:
        args.manifest = "data/sample_1m.manifest.json" if args.scenario == "ensaio" else "data/manifest.json"
    if args.input_parquet is None:
        args.input_parquet = "data/sample_1m.parquet" if args.scenario == "ensaio" else "data/ratings_input_synthetic.parquet"

    cfg = load_config(args.config)
    cfg.require("MONGODB_URI", "ADF_URI", "S3_BUCKET", "AWS_REGION")
    adf = mongo_client(cfg.adf_uri)
    cluster = mongo_client(cfg.mongodb_uri)
    s3 = s3_client(cfg.aws_region)
    manifest = load_manifest(args.manifest)
    all_ok = True
    for run in range(1, args.runs + 1):
        all_ok &= run_once(args, cfg, manifest, adf, cluster, s3, run)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
