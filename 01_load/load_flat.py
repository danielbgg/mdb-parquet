#!/usr/bin/env python3
"""01_load — flat load S3 (Parquet) -> Atlas cluster through Atlas Data Federation.

Runs, on the federated database (ADF_URI):

    db.<source>.aggregate([{ $out: { atlas: { projectId, clusterName, db, coll } } }],
                          { allowDiskUse: true })

and validates the target collection on the cluster (MONGODB_URI) against the
generator manifest. Also produces results/type_mapping.md with the BSON type
of every Parquet column.

Usage:
    python 01_load/load_flat.py --source single              # dbimport.s3import      -> contratos_flat
    python 01_load/load_flat.py --source split               # dbimport.s3import_split -> contratos_flat
    python 01_load/load_flat.py --source 1m --manifest data/sample_1m.manifest.json   # smoke test
    python 01_load/load_flat.py --check                      # connectivity / virtual collections only
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.bench import RESULTS_DIR, Bench, coll_stats, ensure_target, human_bytes, mongo_client  # noqa: E402
from common.config import load_config, load_manifest, rel  # noqa: E402
from generator.schema import COLUMN_ORDER  # noqa: E402

SOURCES = {
    "single": {"coll": "s3import", "target": "contratos_flat"},
    "split": {"coll": "s3import_split", "target": "contratos_flat"},
    "1m": {"coll": "s3import_1m", "target": "contratos_flat_1m"},
}
SUM_TOLERANCE = 0.01


# ---------------------------------------------------------------------------
# ADF
# ---------------------------------------------------------------------------


def out_to_atlas(cfg, coll: str) -> dict:
    stage = {"atlas": {"clusterName": cfg.atlas_cluster, "db": cfg.target_db, "coll": coll}}
    if cfg.atlas_project_id:
        stage["atlas"]["projectId"] = cfg.atlas_project_id
    return {"$out": stage}


def check_sources(adf: MongoClient, cfg) -> None:
    db = adf[cfg.adf_db]
    print(f"ADF database '{cfg.adf_db}' collections: {db.list_collection_names()}")
    for name, src in SOURCES.items():
        coll = src["coll"]
        try:
            one = db[coll].find_one()
            print(f"  --source {name:<6} -> {cfg.adf_db}.{coll}: sample doc keys={list(one.keys()) if one else None}")
        except PyMongoError as exc:
            print(f"  --source {name:<6} -> {cfg.adf_db}.{coll}: ERROR {exc}")


# ---------------------------------------------------------------------------
# Validation on the cluster
# ---------------------------------------------------------------------------


def validate_flat(cluster: MongoClient, db: str, coll: str, manifest: dict, check_distinct: bool) -> dict:
    c = cluster[db][coll]
    checks = []

    def check(ok: bool, label: str, detail: str = "") -> None:
        checks.append({"ok": bool(ok), "check": label, "detail": detail})
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail else ""))

    count = c.count_documents({})
    check(count == manifest["rows"], "countDocuments == manifest.rows", f"{count:,} vs {manifest['rows']:,}")

    sums = next(
        c.aggregate(
            [
                {
                    "$group": {
                        "_id": None,
                        "sum_saldo": {"$sum": {"$toDecimal": "$saldo"}},
                        "sum_saldo_limite": {"$sum": {"$toDecimal": "$saldo_limite"}},
                        "n": {"$sum": 1},
                    }
                }
            ],
            allowDiskUse=True,
        ),
        {},
    )
    for key in ("sum_saldo", "sum_saldo_limite"):
        got = float(sums[key].to_decimal()) if sums.get(key) is not None else float("nan")
        exp = float(manifest["checksums"][key])
        check(abs(got - exp) <= SUM_TOLERANCE, f"{key} == manifest (±{SUM_TOLERANCE})", f"{got:,.2f} vs {exp:,.2f}")

    n_customers = None
    if check_distinct:
        # Without an index on co_cpf_cnpj this is a 30M-row hash $group spilling
        # to disk (>20 min on an M40); with --create-index it becomes a DISTINCT_SCAN.
        distinct = list(
            c.aggregate([{"$group": {"_id": "$co_cpf_cnpj"}}, {"$count": "n"}], allowDiskUse=True)
        )
        n_customers = distinct[0]["n"] if distinct else 0
        check(n_customers == manifest["customers"], "distinct co_cpf_cnpj == manifest.customers", f"{n_customers:,} vs {manifest['customers']:,}")
    else:
        print("  [SKIP] distinct co_cpf_cnpj == manifest.customers — validated by 02 (countDocuments of the grouped collection); use --check-distinct or --create-index")

    return {
        "count": count,
        "sum_saldo": str(sums.get("sum_saldo")),
        "sum_saldo_limite": str(sums.get("sum_saldo_limite")),
        "distinct_customers": n_customers,
        "checks": checks,
        "ok": all(ch["ok"] for ch in checks),
    }


def create_customer_index(cluster: MongoClient, db: str, coll: str) -> dict:
    """Index on co_cpf_cnpj (the grouping key of step 02); the build time is
    reported separately because it is part of what an 'import' step costs."""
    name = cluster[db][coll].create_index([("co_cpf_cnpj", 1)], name="co_cpf_cnpj_1")
    stats = coll_stats(cluster, db, coll)
    return {"name": name, "size_bytes": (stats.get("index_sizes") or {}).get(name)}


def type_mapping(cluster: MongoClient, db: str, coll: str, sample: int) -> dict:
    """BSON type distribution per Parquet column over a random sample + field order."""
    c = cluster[db][coll]
    facets = {col: [{"$sortByCount": f"$types.{col}"}] for col in COLUMN_ORDER}
    pipeline = [
        {"$sample": {"size": sample}},
        {"$project": {"types": {col: {"$type": f"${col}"} for col in COLUMN_ORDER}}},
        {"$facet": facets},
    ]
    result = next(c.aggregate(pipeline, allowDiskUse=True))
    per_column = {}
    for col in COLUMN_ORDER:
        rows = result[col]
        total = sum(r["count"] for r in rows) or 1
        per_column[col] = {r["_id"]: {"count": r["count"], "pct": round(100.0 * r["count"] / total, 3)} for r in rows}

    first = c.find_one()
    keys = list(first.keys()) if first else []
    present = [k for k in keys if k != "_id"]
    # A field may be legitimately absent (Parquet null -> missing); check relative order only.
    order_preserved = present == [col for col in COLUMN_ORDER if col in present]
    full = c.find_one({"vr_renegociado": {"$exists": True}})
    full_keys = [k for k in full.keys() if k != "_id"] if full else []
    full_order_preserved = full_keys == list(COLUMN_ORDER) if full else None
    vr = per_column["vr_renegociado"]
    if "null" in vr and "missing" not in vr:
        vr_empty = "null"
    elif "missing" in vr and "null" not in vr:
        vr_empty = "missing (campo ausente)"
    elif "missing" in vr and "null" in vr:
        vr_empty = "misto: null e campo ausente"
    else:
        vr_empty = "nenhum vazio na amostra"
    return {
        "sample_size": sample,
        "per_column": per_column,
        "field_order": keys,
        "field_order_preserved": order_preserved,
        "field_order_full_doc": full_keys,
        "field_order_full_doc_preserved": full_order_preserved,
        "vr_renegociado_empty_as": vr_empty,
        "id_type": type(first.get("_id")).__name__ if first else None,
    }


def write_type_mapping_md(path: Path, tm: dict, manifest: dict, db: str, coll: str, env: dict) -> None:
    schema = {s["name"]: s for s in manifest.get("parquet_schema", [])}
    lines = [
        "# Mapeamento de tipos Parquet → BSON (coleção plana)",
        "",
        f"- Coleção: `{db}.{coll}`",
        f"- Gerado em: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"- Manifest: `{manifest.get('file')}` (perfil `{manifest.get('type_profile')}`, seed {manifest.get('seed')})",
        f"- MongoDB: {env.get('mongo_version')} ({env.get('tier')}, {env.get('cloud_region')}); ADF: {env.get('adf_version')}",
        f"- Amostra: {tm['sample_size']:,} documentos (`$sample` + `$type`)",
        "",
        "| # | Coluna | Tipo Parquet (físico / lógico) | Tipo(s) BSON na amostra |",
        "|---|---|---|---|",
    ]
    for i, col in enumerate(COLUMN_ORDER, start=1):
        s = schema.get(col, {})
        ptype = s.get("type", "?")
        if s.get("converted_type"):
            ptype += f" / {s['converted_type']}"
        if s.get("logical_type"):
            ptype += f" / {s['logical_type']}"
        types = ", ".join(f"`{t}` {v['pct']:.1f}%" for t, v in tm["per_column"][col].items())
        lines.append(f"| {i} | `{col}` | {ptype} | {types} |")
    lines += [
        "",
        "## Observações",
        "",
        f"- `_id`: gerado pelo ADF/cluster como `{tm['id_type']}` (não existe no Parquet).",
        f"- `vr_renegociado` vazio chega como **{tm['vr_renegociado_empty_as']}**.",
        f"- Ordem dos campos preservada (igual ao header do Parquet): **{'sim' if tm['field_order_preserved'] else 'NÃO'}**.",
        f"  Ordem observada (1º documento): `{', '.join(tm['field_order'])}`",
        f"- Documento com `vr_renegociado` presente — 15 campos na ordem do header: "
        f"**{'sim' if tm.get('field_order_full_doc_preserved') else ('NÃO' if tm.get('field_order_full_doc_preserved') is False else 'n/a')}**.",
        "",
        "Implicações para o export (03): o `$replaceWith`/projeção final deve emitir as 15 colunas nesta ordem e com estes",
        "tipos BSON para que o Parquet de saída reproduza o schema de entrada; `vr_renegociado` precisa ser escrito",
        "explicitamente (`$ifNull`) se o valor vazio chegar como campo ausente.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  type mapping -> {rel(path)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_once(args, cfg, manifest, adf: MongoClient, cluster: MongoClient, run: int) -> bool:
    src = SOURCES[args.source]
    source_coll = src["coll"]
    target = args.target or src["target"]
    bench = Bench(
        scenario=f"01_load_{args.source}",
        cfg=cfg,
        manifest=manifest,
        manifest_path=args.manifest,
        run=run,
        runs=args.runs,
        source=f"{cfg.adf_db}.{source_coll}",
        target=f"{cfg.target_db}.{target}",
        notes=args.notes,
    )
    bench.capture_environment(cluster, adf)
    ensure_target(cluster, cfg.target_db, target, args.drop_target)

    pipeline = [out_to_atlas(cfg, target)]
    bench.details["pipeline"] = pipeline
    print(f"  pipeline: {json.dumps(pipeline)}")
    ok = bench.run_out(
        lambda: list(adf[cfg.adf_db][source_coll].aggregate(pipeline, allowDiskUse=True)),
        cluster, cfg.target_db, target, manifest["rows"],
    )
    if not ok:
        print(f"  ERROR during $out: {bench.details.get('error')}")

    docs = None
    if ok:
        if args.create_index:
            with bench.timed("index_build"):
                idx = create_customer_index(cluster, cfg.target_db, target)
            bench.details["index"] = idx
            print(f"  index {idx['name']}: {human_bytes(idx['size_bytes'])}")
        with bench.timed("validation"):
            v = validate_flat(cluster, cfg.target_db, target, manifest, args.check_distinct or args.create_index)
        bench.details["validation"] = v
        ok = v["ok"]
        docs = v["count"]
        with bench.timed("type_mapping"):
            tm = type_mapping(cluster, cfg.target_db, target, args.type_sample)
        bench.details["type_mapping"] = tm
        suffix = "" if args.source != "1m" else "_1m"
        write_type_mapping_md(RESULTS_DIR / f"type_mapping{suffix}.md", tm, manifest, cfg.target_db, target, bench.environment)
        stats = coll_stats(cluster, cfg.target_db, target)
        bench.details["coll_stats"] = stats
        print(
            f"  collStats: count={stats.get('count'):,} storageSize={human_bytes(stats.get('storage_size_bytes'))} "
            f"dataSize={human_bytes(stats.get('size_bytes'))} indexes={human_bytes(stats.get('total_index_size_bytes'))} "
            f"avgObj={stats.get('avg_obj_size_bytes')} B"
        )
    bench.save(ok, docs)
    return ok


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Flat load S3 -> Atlas via Data Federation $out.")
    p.add_argument("--source", choices=sorted(SOURCES), default="single")
    p.add_argument("--target", default=None, help="target collection (default depends on --source)")
    p.add_argument("--manifest", default="data/manifest.json")
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--drop-target", action="store_true")
    p.add_argument("--type-sample", type=int, default=100_000)
    p.add_argument("--create-index", action="store_true",
                   help="after the $out, build an index on co_cpf_cnpj (time reported separately as index_build)")
    p.add_argument("--check-distinct", action="store_true",
                   help="validate distinct co_cpf_cnpj (slow without --create-index; step 02 validates it anyway)")
    p.add_argument("--notes", default="")
    p.add_argument("--config", default=None, help="path to config.env")
    p.add_argument("--check", action="store_true", help="only check connectivity and virtual collections")
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    cfg.require("MONGODB_URI", "ADF_URI", "ATLAS_CLUSTER")
    adf = mongo_client(cfg.adf_uri)
    cluster = mongo_client(cfg.mongodb_uri)

    if args.check:
        print(f"cluster ping: {cluster.admin.command('ping')}  version={cluster.server_info().get('version')}")
        check_sources(adf, cfg)
        return 0

    manifest = load_manifest(args.manifest)
    all_ok = True
    for run in range(1, args.runs + 1):
        all_ok &= run_once(args, cfg, manifest, adf, cluster, run)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
