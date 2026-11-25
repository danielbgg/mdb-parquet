#!/usr/bin/env python3
"""Validate a clientes_agrupados-style collection against the generator manifest.

Used by group_cluster.py (02a) and group_adf.py (02b); can also run standalone:

    python 02_group_arrasto/validate_grouped.py --coll clientes_agrupados [--manifest data/manifest.json]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from bson import json_util
from pymongo import MongoClient

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(HERE))

from common.bench import RESULTS_DIR, coll_stats, human_bytes, mongo_client  # noqa: E402
from common.config import load_config, load_manifest, rel  # noqa: E402
from rules import LETTERS, letter_expr, rank_expr  # noqa: E402

SUM_TOLERANCE = 0.01
MAX_DOC_BYTES = 16 * 1024 * 1024


def validate_grouped(cluster: MongoClient, db: str, coll: str, manifest: dict, flat_coll: str | None = None) -> dict:
    c = cluster[db][coll]
    checks = []

    def check(ok: bool, label: str, detail: str = "") -> None:
        checks.append({"ok": bool(ok), "check": label, "detail": detail})
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail else ""))

    count = c.count_documents({})
    check(count == manifest["customers"], "countDocuments == manifest.customers", f"{count:,} vs {manifest['customers']:,}")

    # One pass over the collection for all aggregate figures.
    contract_ranks = {"$map": {"input": "$contratos", "in": rank_expr("$$this.qt_dias_atraso")}}
    contract_days = "$contratos.qt_dias_atraso"
    agg = next(
        c.aggregate(
            [
                {
                    "$project": {
                        "qtd_contratos": 1,
                        "pior_rank": 1,
                        "rating_final": 1,
                        "n_array": {"$size": "$contratos"},
                        "saldo": {"$sum": {"$map": {"input": "$contratos", "in": {"$toDecimal": "$$this.saldo"}}}},
                        "saldo_limite": {"$sum": {"$map": {"input": "$contratos", "in": {"$toDecimal": "$$this.saldo_limite"}}}},
                        "max_rank": {"$max": contract_ranks},
                        "min_rank": {"$min": contract_ranks},
                        "min_days": {"$min": contract_days},
                        "max_days": {"$max": contract_days},
                        "letters_ok": {
                            "$allElementsTrue": {
                                "$map": {
                                    "input": "$contratos",
                                    "in": {"$eq": ["$$this.rating_contrato", letter_expr(rank_expr("$$this.qt_dias_atraso"))]},
                                }
                            }
                        },
                        "bytes": {"$bsonSize": "$$ROOT"},
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "sum_qtd_contratos": {"$sum": "$qtd_contratos"},
                        "sum_array_sizes": {"$sum": "$n_array"},
                        "sum_saldo": {"$sum": "$saldo"},
                        "sum_saldo_limite": {"$sum": "$saldo_limite"},
                        "bad_pior_rank": {"$sum": {"$cond": [{"$ne": ["$pior_rank", "$max_rank"]}, 1, 0]}},
                        "bad_rating_final": {
                            "$sum": {"$cond": [{"$ne": ["$rating_final", letter_expr("$pior_rank")]}, 1, 0]}
                        },
                        "bad_rating_contrato": {"$sum": {"$cond": ["$letters_ok", 0, 1]}},
                        "bad_qtd": {"$sum": {"$cond": [{"$ne": ["$qtd_contratos", "$n_array"]}, 1, 0]}},
                        "mixed_rank": {"$sum": {"$cond": [{"$ne": ["$min_rank", "$max_rank"]}, 1, 0]}},
                        "mixed_manifest_def": {
                            "$sum": {"$cond": [{"$and": [{"$eq": ["$min_days", 0]}, {"$gt": ["$max_days", 90]}]}, 1, 0]}
                        },
                        "max_bytes": {"$max": "$bytes"},
                        "avg_bytes": {"$avg": "$bytes"},
                        "max_contracts": {"$max": "$qtd_contratos"},
                    }
                },
            ],
            allowDiskUse=True,
        ),
        {},
    )

    check(agg.get("sum_qtd_contratos") == manifest["rows"], "sum(qtd_contratos) == manifest.rows",
          f"{agg.get('sum_qtd_contratos', 0):,} vs {manifest['rows']:,}")
    check(agg.get("bad_qtd", 1) == 0, "qtd_contratos == size(contratos) for every document", f"violations={agg.get('bad_qtd')}")
    for key in ("sum_saldo", "sum_saldo_limite"):
        got = float(agg[key].to_decimal()) if agg.get(key) is not None else float("nan")
        exp = float(manifest["checksums"][key])
        check(abs(got - exp) <= SUM_TOLERANCE, f"sum(contratos.{key[4:]}) == manifest (±{SUM_TOLERANCE})", f"{got:,.2f} vs {exp:,.2f}")
    check(agg.get("bad_pior_rank", 1) == 0, "pior_rank == max(rank of contratos) for every document", f"violations={agg.get('bad_pior_rank')}")
    check(agg.get("bad_rating_final", 1) == 0, "rating_final == letter(pior_rank) for every document", f"violations={agg.get('bad_rating_final')}")
    check(agg.get("bad_rating_contrato", 1) == 0, "rating_contrato == letter(rank(qt_dias_atraso)) for every contract",
          f"violations={agg.get('bad_rating_contrato')}")

    mixed_def = agg.get("mixed_manifest_def", 0)
    mixed_rank = agg.get("mixed_rank", 0)
    check(mixed_def == manifest["customers_mixed"], "mixed customers (min days 0 & max > 90) == manifest.customers_mixed",
          f"{mixed_def:,} vs {manifest['customers_mixed']:,} ({100.0 * mixed_def / max(count, 1):.2f}% of customers)")
    check(mixed_rank >= mixed_def, "customers with min rank != max rank (arrasto changes a contract) >= manifest mixed",
          f"{mixed_rank:,} ({100.0 * mixed_rank / max(count, 1):.2f}% of customers)")

    hist = {str(r["_id"]): r["n"] for r in c.aggregate(
        [{"$group": {"_id": "$qtd_contratos", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}], allowDiskUse=True)}
    check(hist == manifest["contracts_per_customer_histogram"], "qtd_contratos histogram == manifest",
          f"{len(hist)} buckets, max {agg.get('max_contracts')} contracts")

    max_bytes = agg.get("max_bytes", 0)
    check(max_bytes < MAX_DOC_BYTES * 0.1, "largest document << 16 MB",
          f"max={human_bytes(max_bytes)} ({100.0 * max_bytes / MAX_DOC_BYTES:.3f}% of 16 MB), avg={human_bytes(agg.get('avg_bytes'))}")

    stats = coll_stats(cluster, db, coll)
    out = {
        "count": count,
        "sum_qtd_contratos": agg.get("sum_qtd_contratos"),
        "sum_saldo": str(agg.get("sum_saldo")),
        "sum_saldo_limite": str(agg.get("sum_saldo_limite")),
        "mixed_manifest_definition": mixed_def,
        "mixed_rank_differs": mixed_rank,
        "pct_mixed_rank_differs": round(100.0 * mixed_rank / max(count, 1), 3),
        "histogram": hist,
        "max_doc_bytes": max_bytes,
        "avg_doc_bytes": agg.get("avg_bytes"),
        "coll_stats": stats,
        "checks": checks,
        "ok": all(ch["ok"] for ch in checks),
    }
    print(
        f"  collStats {db}.{coll}: storageSize={human_bytes(stats.get('storage_size_bytes'))} "
        f"dataSize={human_bytes(stats.get('size_bytes'))} indexes={human_bytes(stats.get('total_index_size_bytes'))} "
        f"avgObj={human_bytes(stats.get('avg_obj_size_bytes'))}"
    )
    if flat_coll:
        flat = coll_stats(cluster, db, flat_coll)
        out["flat_coll_stats"] = flat
        if flat.get("storage_size_bytes") and stats.get("storage_size_bytes"):
            ratio = stats["storage_size_bytes"] / flat["storage_size_bytes"]
            out["storage_ratio_grouped_vs_flat"] = round(ratio, 3)
            print(
                f"  storage grouped/flat: {human_bytes(stats['storage_size_bytes'])} / "
                f"{human_bytes(flat['storage_size_bytes'])} = {ratio:.2f}x"
            )
    return out


def example_mixed_customer(cluster: MongoClient, db: str, coll: str) -> dict | None:
    """A small customer whose worst rating (>= E) drags a current (A) contract."""
    pipeline = [
        {"$match": {"qtd_contratos": {"$gte": 2, "$lte": 3}, "pior_rank": {"$gte": 5}}},
        {"$match": {"$expr": {"$in": ["A", "$contratos.rating_contrato"]}}},
        {"$limit": 1},
    ]
    return next(cluster[db][coll].aggregate(pipeline), None)


def save_example(doc: dict | None, path: Path) -> None:
    if doc is None:
        print("  no mixed customer found for the example")
        return
    path.write_text(json_util.dumps(doc, indent=2, json_options=json_util.RELAXED_JSON_OPTIONS) + "\n", encoding="utf-8")
    print(f"  example mixed customer -> {rel(path)}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Validate a grouped customers collection.")
    p.add_argument("--coll", default="clientes_agrupados")
    p.add_argument("--flat-coll", default="contratos_flat")
    p.add_argument("--manifest", default="data/manifest.json")
    p.add_argument("--config", default=None)
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    cfg.require("MONGODB_URI")
    cluster = mongo_client(cfg.mongodb_uri)
    manifest = load_manifest(args.manifest)
    result = validate_grouped(cluster, cfg.target_db, args.coll, manifest, args.flat_coll)
    save_example(example_mixed_customer(cluster, cfg.target_db, args.coll), RESULTS_DIR / f"exemplo_cliente_misto_{args.coll}.json")
    print(json.dumps({k: v for k, v in result.items() if k not in ("checks", "histogram")}, indent=2, default=str))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
