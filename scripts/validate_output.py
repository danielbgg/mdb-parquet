#!/usr/bin/env python3
"""Validate a Parquet export produced by 03_export against the input Parquet
and the generator manifest (DuckDB + pyarrow).

    python scripts/validate_output.py --s3-prefix s3://bucket/output/03b/<ts>/ [--grouped]
    python scripts/validate_output.py --local data/output/03b/<ts>/ratings_export.1.parquet --grouped

Files are downloaded to data/output/<scenario>/<ts>/ (git-ignored) so both
DuckDB and pyarrow read them locally; the row-by-row comparison joins against
the local input file by chave_sequencial.
"""

from __future__ import annotations

import argparse
import json

import sys
import time
from pathlib import Path

import boto3
import duckdb
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.config import load_manifest, rel  # noqa: E402
from generator.schema import COLUMN_ORDER  # noqa: E402

RATING_COLUMNS = ("rating_contrato", "rating_final")
SCHEMA_FIELDS = ("name", "type", "converted_type", "logical_type", "repetition_type", "scale", "precision", "field_id")


class Report:
    def __init__(self):
        self.checks: list[dict] = []
        self.data: dict = {}

    def check(self, ok: bool, label: str, detail: str = "", warn_only: bool = False) -> None:
        self.checks.append({"ok": bool(ok), "check": label, "detail": detail, "warn_only": warn_only})
        tag = "PASS" if ok else ("WARN" if warn_only else "FAIL")
        print(f"  [{tag}] {label}" + (f" — {detail}" if detail else ""), flush=True)

    @property
    def ok(self) -> bool:
        return all(c["ok"] or c["warn_only"] for c in self.checks)


def download_prefix(s3_prefix: str, dest_root: Path) -> tuple[list[Path], list[dict]]:
    bucket, _, prefix = s3_prefix[5:].partition("/")
    s3 = boto3.client("s3")
    objs: list[dict] = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        objs += r.get("Contents", [])
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]
    dest = dest_root / prefix.rstrip("/").replace("output/", "", 1)
    dest.mkdir(parents=True, exist_ok=True)
    paths = []
    t0 = time.perf_counter()
    for o in objs:
        local = dest / Path(o["Key"]).name
        if not local.exists() or local.stat().st_size != o["Size"]:
            print(f"  downloading {o['Key']} ({o['Size'] / 1e6:,.1f} MB) ...", flush=True)
            s3.download_file(bucket, o["Key"], str(local))
        paths.append(local)
    print(f"  {len(paths)} object(s) under s3://{bucket}/{prefix} -> {rel(dest)} ({time.perf_counter() - t0:.1f}s)")
    return paths, [{"key": o["Key"], "size_bytes": o["Size"]} for o in objs]


def parquet_schema(con: duckdb.DuckDBPyConnection, path: str) -> list[dict]:
    rows = con.execute(
        f"""SELECT name, type, converted_type, logical_type, repetition_type, scale, precision, field_id, num_children
            FROM parquet_schema('{path}')"""
    ).fetchall()
    out = []
    for r in rows:
        d = dict(zip(SCHEMA_FIELDS + ("num_children",), r))
        d["logical_type"] = str(d["logical_type"]) if d["logical_type"] is not None else None
        out.append(d)
    return out


def canonical_type(c: dict) -> str:
    """Physical + logical type with legacy ConvertedType and modern LogicalType unified.

    INT64/INT_64/none, INT32/INT_32/none and UTF8/StringType are the same
    types written with different annotations (DuckDB writes legacy converted
    types; ADF writes modern logical types).
    """
    phys = c["type"]
    conv = c["converted_type"]
    logi = c["logical_type"]
    if phys in ("INT32", "INT64") and conv in (None, "INT_32", "INT_64") and logi in (None, "IntType(bitWidth=32, isSigned=true)", "IntType(bitWidth=64, isSigned=true)"):
        return phys
    if phys == "BYTE_ARRAY" and (conv == "UTF8" or logi == "StringType()"):
        return "STRING"
    if conv == "DECIMAL" or (logi or "").startswith("DecimalType"):
        return f"DECIMAL({c['precision']},{c['scale']})"
    if conv == "TIMESTAMP_MICROS" or (logi or "").startswith("TimestampType"):
        return f"TIMESTAMP({logi or conv})"
    return f"{phys}/{conv}/{logi}"


def schema_diff(inp: list[dict], out: list[dict]) -> tuple[list[str], list[str]]:
    """Compare the 15 input leaf columns with the first 15 output leaf columns.

    Returns (type_diffs, annotation_diffs): the first are real type changes
    (physical/logical meaning, scale/precision); the second are annotation or
    nullability (repetition) differences that do not change the data.
    """
    leaves_in = [c for c in inp if not c["num_children"]]
    leaves_out = [c for c in out if not c["num_children"]]
    type_diffs, annot_diffs = [], []
    for i, name in enumerate(COLUMN_ORDER):
        a = leaves_in[i] if i < len(leaves_in) else None
        b = leaves_out[i] if i < len(leaves_out) else None
        if a is None or b is None:
            type_diffs.append(f"col {i + 1} {name}: missing in {'input' if a is None else 'output'}")
            continue
        if a["name"] != b["name"]:
            type_diffs.append(f"col {i + 1}: name input={a['name']} output={b['name']}")
        ca, cb = canonical_type(a), canonical_type(b)
        if ca != cb:
            type_diffs.append(f"col {i + 1} {name}: type input={ca} output={cb}")
        for f in ("converted_type", "logical_type", "repetition_type"):
            if a[f] != b[f]:
                annot_diffs.append(f"col {i + 1} {name}.{f}: input={a[f]!r} output={b[f]!r}")
    return type_diffs, annot_diffs


def format_schema_table(inp: list[dict], out: list[dict]) -> str:
    leaves_in = {c["name"]: c for c in inp if not c["num_children"]}
    leaves_out = [c for c in out if not c["num_children"]]
    lines = ["| # | coluna | entrada: físico / converted / logical / repetition | saída: físico / converted / logical / repetition | tipo | anotação/nullability |",
             "|---|---|---|---|---|---|"]
    for i, b in enumerate(leaves_out, start=1):
        a = leaves_in.get(b["name"])
        fmt = lambda c: f"{c['type']} / {c['converted_type']} / {c['logical_type']} / {c['repetition_type']}"  # noqa: E731
        if a is None:
            lines.append(f"| {i} | `{b['name']}` | (não existe na entrada) | {fmt(b)} | — | — |")
            continue
        same_type = canonical_type(a) == canonical_type(b)
        same_annot = all(a[f] == b[f] for f in ("converted_type", "logical_type", "repetition_type"))
        lines.append(f"| {i} | `{b['name']}` | {fmt(a)} | {fmt(b)} | {'igual' if same_type else '**DIFERENTE**'} | {'igual' if same_annot else 'difere'} |")
    return "\n".join(lines)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Validate an exported Parquet file (03_export).")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--s3-prefix", help="s3://bucket/output/<scenario>/<ts>/")
    src.add_argument("--local", nargs="+", help="local parquet file(s)")
    p.add_argument("--manifest", default="data/manifest.json")
    p.add_argument("--input", default="data/ratings_input_synthetic.parquet", help="local input Parquet")
    p.add_argument("--grouped", action="store_true", help="output has rating_contrato/rating_final (03b)")
    p.add_argument("--allow-multiple-files", action="store_true")
    p.add_argument("--skip-row-compare", action="store_true")
    p.add_argument("--report", default=None, help="JSON report path")
    p.add_argument("--download-dir", default="data/output")
    p.add_argument("--memory-limit", default="8GB")
    args = p.parse_args(argv)

    manifest = load_manifest(args.manifest)
    rep = Report()
    t_start = time.perf_counter()

    if args.s3_prefix:
        paths, objs = download_prefix(args.s3_prefix, Path(args.download_dir))
    else:
        paths = [Path(x) for x in args.local]
        objs = [{"key": str(x), "size_bytes": x.stat().st_size} for x in paths]
    parquet_paths = [x for x in paths if x.suffix == ".parquet"]
    other = [x for x in paths if x.suffix != ".parquet"]
    rep.data["files"] = objs
    rep.check(not other, "no error/other files in the prefix", f"non-parquet objects: {[x.name for x in other]}" if other else f"{len(parquet_paths)} parquet file(s)")
    if args.allow_multiple_files:
        rep.check(len(parquet_paths) >= 1, "parquet files generated", f"{len(parquet_paths)}")
    else:
        rep.check(len(parquet_paths) == 1, "exactly 1 parquet file generated", f"{len(parquet_paths)} file(s)")
    if not parquet_paths:
        _save(rep, args, t_start)
        return 1
    rep.data["rows_expected"] = manifest["rows"]
    total_bytes = sum(x.stat().st_size for x in parquet_paths)
    rep.data["size_bytes"] = total_bytes

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    tmp = REPO_ROOT / "data" / ".duckdb_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{tmp.as_posix()}'")
    glob = "[" + ",".join(f"'{x.as_posix()}'" for x in parquet_paths) + "]"
    out = f"read_parquet({glob}, union_by_name = false)"
    inp = f"read_parquet('{Path(args.input).as_posix()}')"

    # --- schema ---------------------------------------------------------
    sch_in = parquet_schema(con, Path(args.input).as_posix())
    sch_out = parquet_schema(con, parquet_paths[0].as_posix())
    rep.data["schema_input"] = sch_in
    rep.data["schema_output"] = sch_out
    leaves_out = [c for c in sch_out if not c["num_children"]]
    names_out = [c["name"] for c in leaves_out]
    expected_cols = list(COLUMN_ORDER) + (list(RATING_COLUMNS) if args.grouped else [])
    rep.check(names_out == expected_cols, "column names and order", ", ".join(names_out))
    groups = [c["name"] for c in sch_out[1:] if c["num_children"]]
    rep.check(not groups, "no nested/polymorphic (group) columns", f"groups: {groups}" if groups else "all leaves")
    type_diffs, annot_diffs = schema_diff(sch_in, sch_out)
    rep.data["schema_type_diff"] = type_diffs
    rep.data["schema_annotation_diff"] = annot_diffs
    rep.data["schema_table_md"] = format_schema_table(sch_in, sch_out)
    rep.check(not type_diffs, "first 15 columns: same physical/logical type as input (normalized)",
              "; ".join(type_diffs) if type_diffs else "identical")
    rep.check(not annot_diffs, "first 15 columns: identical annotations and nullability (repetition)",
              f"{len(annot_diffs)} difference(s): " + "; ".join(annot_diffs) if annot_diffs else "identical", warn_only=True)
    print("\n" + rep.data["schema_table_md"] + "\n")

    # --- metadata -------------------------------------------------------
    meta = con.execute(
        f"""SELECT count(DISTINCT (file_name, row_group_id)), max(row_group_num_rows), any_value(compression),
                   sum(total_compressed_size)/1e6, sum(total_uncompressed_size)/1e6
            FROM parquet_metadata({glob})"""
    ).fetchone()
    rep.data["row_groups"] = meta[0]
    rep.data["max_rows_per_row_group"] = meta[1]
    rep.data["compression"] = meta[2]
    print(f"  metadata: {meta[0]} row groups (max {meta[1]:,} rows), compression={meta[2]}, "
          f"{meta[3]:,.1f} MB compressed / {meta[4]:,.1f} MB raw; file size {total_bytes / 1e9:.3f} GB")
    rep.check(str(meta[2]).upper() == "SNAPPY", "compression is snappy", str(meta[2]))

    # --- counts / keys ----------------------------------------------------
    n, kmin, kmax, kdist, cdist, vr_null, s1, s2 = con.execute(
        f"""SELECT count(*), min(chave_sequencial), max(chave_sequencial), count(DISTINCT chave_sequencial),
                   count(DISTINCT contrato), count(*) - count(vr_renegociado),
                   sum(CAST(saldo AS DECIMAL(18,2))), sum(CAST(saldo_limite AS DECIMAL(18,2)))
            FROM {out}"""
    ).fetchone()
    rep.data.update({"rows": n, "distinct_contrato": cdist, "vr_renegociado_nulls": vr_null,
                     "sum_saldo": str(s1), "sum_saldo_limite": str(s2)})
    rep.check(n == manifest["rows"], "count == manifest.rows", f"{n:,} vs {manifest['rows']:,}")
    rep.check(kmin == 1 and kmax == n and kdist == n, "chave_sequencial unique and covering 1..N", f"min={kmin} max={kmax} distinct={kdist:,}")
    rep.check(cdist == n, "contrato unique", f"distinct={cdist:,}")
    rep.check(str(s1) == manifest["checksums"]["sum_saldo"], "sum(saldo) == manifest", f"{s1} vs {manifest['checksums']['sum_saldo']}")
    rep.check(str(s2) == manifest["checksums"]["sum_saldo_limite"], "sum(saldo_limite) == manifest", f"{s2} vs {manifest['checksums']['sum_saldo_limite']}")
    vr_null_in = con.execute(f"SELECT count(*) - count(vr_renegociado) FROM {inp}").fetchone()[0]
    rep.check(vr_null == vr_null_in, "vr_renegociado null count == input", f"{vr_null:,} vs {vr_null_in:,}")
    null_cols = con.execute(
        "SELECT " + ", ".join(f"count(*) - count({c})" for c in expected_cols if c != "vr_renegociado") + f" FROM {out}"
    ).fetchone()
    bad_nulls = {c: v for c, v in zip([c for c in expected_cols if c != "vr_renegociado"], null_cols) if v}
    rep.check(not bad_nulls, "nulls only in vr_renegociado", str(bad_nulls) if bad_nulls else "ok")

    # --- row-by-row ---------------------------------------------------------
    if not args.skip_row_compare:
        # Compare (chave_sequencial, 64-bit hash of the 15 columns) in both directions.
        # chave_sequencial is unique, so this is an exact row-level equality test that
        # needs ~16 bytes/row instead of the full 15-column EXCEPT (which exceeds 8 GB).
        cols = ", ".join(COLUMN_ORDER)
        hashed_in = f"(SELECT chave_sequencial AS k, hash({cols}) AS h FROM {inp})"
        hashed_out = f"(SELECT chave_sequencial AS k, hash({cols}) AS h FROM {out})"
        t0 = time.perf_counter()
        missing = con.execute(f"SELECT count(*) FROM (SELECT * FROM {hashed_in} EXCEPT SELECT * FROM {hashed_out})").fetchone()[0]
        extra = con.execute(f"SELECT count(*) FROM (SELECT * FROM {hashed_out} EXCEPT SELECT * FROM {hashed_in})").fetchone()[0]
        rep.data["rows_missing_vs_input"] = missing
        rep.data["rows_extra_vs_input"] = extra
        rep.check(missing == 0 and extra == 0, "row-by-row: 15 columns identical to input (keyed hash EXCEPT both ways)",
                  f"missing={missing:,} extra={extra:,} ({time.perf_counter() - t0:.1f}s)")
        if missing or extra:
            sample = con.execute(
                f"""SELECT k FROM (SELECT * FROM {hashed_in} EXCEPT SELECT * FROM {hashed_out}) LIMIT 3"""
            ).fetchall()
            keys = [r[0] for r in sample]
            if keys:
                klist = ",".join(str(k) for k in keys)
                diff_rows = con.execute(
                    f"""SELECT 'input' src, * FROM {inp} WHERE chave_sequencial IN ({klist})
                        UNION ALL SELECT 'output', {cols} FROM {out} WHERE chave_sequencial IN ({klist})
                        ORDER BY chave_sequencial, src"""
                ).fetchall()
                print(f"  sample differences: {diff_rows}")
    else:
        print("  [SKIP] row-by-row comparison")

    # --- rating (03b) -----------------------------------------------------------
    if args.grouped:
        bad_const, bad_max, n_cust, rows_changed, cust_changed = con.execute(
            f"""WITH per AS (
                    SELECT co_cpf_cnpj, count(DISTINCT rating_final) nrf, any_value(rating_final) rf,
                           max(rating_contrato) worst,
                           count(*) FILTER (WHERE rating_final <> rating_contrato) changed
                    FROM {out} GROUP BY 1)
                SELECT count(*) FILTER (WHERE nrf <> 1), count(*) FILTER (WHERE rf <> worst), count(*),
                       sum(changed), count(*) FILTER (WHERE changed > 0)
                FROM per"""
        ).fetchone()
        rep.check(bad_const == 0, "rating_final constant per co_cpf_cnpj", f"violations={bad_const}")
        rep.check(bad_max == 0, "rating_final == worst rating_contrato of the customer", f"violations={bad_max}")
        pct_rows = 100.0 * rows_changed / n
        pct_cust = 100.0 * cust_changed / n_cust
        rep.data.update({"rows_rating_changed": rows_changed, "pct_rows_rating_changed": round(pct_rows, 3),
                         "customers_rating_changed": cust_changed, "pct_customers_rating_changed": round(pct_cust, 3)})
        rep.check(True, "rows with rating_final != rating_contrato (arrasto effect)",
                  f"{rows_changed:,} rows ({pct_rows:.2f}%) in {cust_changed:,} customers ({pct_cust:.2f}% — expected ≈19.2%, the share of customers with min rank != max rank in 02)")
        dist = con.execute(f"SELECT rating_final, count(*) FROM {out} GROUP BY 1 ORDER BY 1").fetchall()
        rep.data["rating_final_distribution"] = {k: v for k, v in dist}
        print(f"  rating_final distribution: {rep.data['rating_final_distribution']}")

    # --- pyarrow ------------------------------------------------------------------
    t0 = time.perf_counter()
    try:
        total = 0
        for x in parquet_paths:
            pf = pq.ParquetFile(x)
            for batch in pf.iter_batches(batch_size=1_000_000):
                total += batch.num_rows
            arrow_schema = pf.schema_arrow
        rep.data["pyarrow_schema"] = [f"{f.name}: {f.type}" for f in arrow_schema]
        rep.check(total == n, "pyarrow reads the whole file", f"{total:,} rows in {time.perf_counter() - t0:.1f}s; schema: {rep.data['pyarrow_schema']}")
    except Exception as exc:
        rep.check(False, "pyarrow reads the whole file", str(exc))

    con.close()
    return _save(rep, args, t_start)


def _save(rep: Report, args, t_start: float) -> int:
    rep.data["checks"] = rep.checks
    rep.data["ok"] = rep.ok
    rep.data["validation_seconds"] = round(time.perf_counter() - t_start, 1)
    n_fail = sum(1 for c in rep.checks if not c["ok"] and not c["warn_only"])
    print(f"\n{n_fail} failure(s) in {len(rep.checks)} checks ({rep.data['validation_seconds']}s)")
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(rep.data, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
        print(f"report -> {rel(Path(args.report))}")
    return 0 if rep.ok else 1


if __name__ == "__main__":
    sys.exit(main())
