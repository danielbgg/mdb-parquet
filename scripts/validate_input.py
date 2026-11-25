#!/usr/bin/env python3
"""Validate a synthetic ratings input Parquet file against its manifest.

Checks (DuckDB):
  - row count, chave_sequencial permutation of 1..rows, contrato uniqueness
  - column names/order, nulls only in vr_renegociado
  - contracts-per-customer histogram, mixed-customer share, carteira counts
    and checksums identical to the manifest
  - flag_obito constant per customer, qt_dias_atraso_acumulado >= qt_dias_atraso
  - documents are 11/14 digits and never pass the CPF/CNPJ check-digit test
  - file size within ±5% of the 1.10 GB target (warning only)
Then runs sql/perfil_parquet.sql on the file and saves the output.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from generator import schema as S  # noqa: E402


class Report:
    def __init__(self):
        self.failures = 0
        self.warnings = 0

    def check(self, ok: bool, label: str, detail: str = "") -> None:
        status = "PASS" if ok else "FAIL"
        if not ok:
            self.failures += 1
        print(f"[{status}] {label}" + (f" — {detail}" if detail else ""))

    def warn(self, ok: bool, label: str, detail: str = "") -> None:
        status = "OK  " if ok else "WARN"
        if not ok:
            self.warnings += 1
        print(f"[{status}] {label}" + (f" — {detail}" if detail else ""))


def cpf_valid(doc: str) -> bool:
    d = [int(c) for c in doc]
    for n in (9, 10):
        s = sum(d[i] * (n + 1 - i) for i in range(n))
        r = s % 11
        if d[n] != (0 if r < 2 else 11 - r):
            return False
    return True


def cnpj_valid(doc: str) -> bool:
    d = [int(c) for c in doc]
    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    w2 = [6] + w1
    for n, w in ((12, w1), (13, w2)):
        r = sum(d[i] * w[i] for i in range(n)) % 11
        if d[n] != (0 if r < 2 else 11 - r):
            return False
    return True


def format_table(columns: list[str], rows: list[tuple]) -> str:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    line = "  ".join(c.ljust(widths[i]) for i, c in enumerate(columns))
    sep = "  ".join("-" * w for w in widths)
    body = "\n".join("  ".join(r[i].ljust(widths[i]) for i in range(len(columns))) for r in cells)
    return "\n".join([line, sep, body, f"({len(rows)} rows)"])


def run_profile_sql(con: duckdb.DuckDBPyConnection, sql_path: Path, parquet: Path, out_path: Path) -> None:
    text = sql_path.read_text(encoding="utf-8")
    sections = []
    current_comment: list[str] = []
    current_stmt: list[str] = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            if not current_stmt:
                current_comment.append(line.strip())
            continue
        if not line.strip():
            continue
        current_stmt.append(line)
        if line.rstrip().endswith(";"):
            sections.append(("\n".join(current_comment), "\n".join(current_stmt)))
            current_comment, current_stmt = [], []
    out = [f"# perfil_parquet.sql sobre {parquet}"]
    for comment, stmt in sections:
        rel = con.execute(stmt.replace("ARQUIVO.parquet", parquet.as_posix()))
        rows = rel.fetchall()
        cols = [d[0] for d in rel.description]
        out.append("")
        out.append(comment)
        out.append(format_table(cols, rows))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"\nPerfil salvo em {out_path}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Validate the synthetic ratings input Parquet file.")
    p.add_argument("--file", default="data/ratings_input_synthetic.parquet")
    p.add_argument("--manifest", default="data/manifest.json")
    p.add_argument("--rows", type=int, default=None, help="expected rows (default: from manifest)")
    p.add_argument("--profile-sql", default="sql/perfil_parquet.sql")
    p.add_argument("--profile-out", default="results/perfil_sintetico.txt")
    p.add_argument("--skip-profile", action="store_true")
    p.add_argument("--doc-sample", type=int, default=200_000, help="documents sampled for the check-digit test")
    p.add_argument("--memory-limit", default="8GB")
    args = p.parse_args(argv)

    path = Path(args.file)
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    expected_rows = args.rows or manifest["rows"]
    f = f"read_parquet('{path.as_posix()}')"
    rep = Report()

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    print(f"Validating {path} against {args.manifest} (expected rows: {expected_rows:,})\n")

    # --- schema -----------------------------------------------------------
    desc = con.execute(f"DESCRIBE SELECT * FROM {f}").fetchall()
    names = [r[0] for r in desc]
    rep.check(tuple(names) == S.COLUMN_ORDER, "column names and order match the header", ", ".join(names))
    types = {r[0]: r[1] for r in desc}
    print("       types: " + ", ".join(f"{k}={v}" for k, v in types.items()))

    # --- counts and keys ---------------------------------------------------
    count = con.execute(f"SELECT count(*) FROM {f}").fetchone()[0]
    rep.check(count == expected_rows, "row count == rows", f"{count:,}")
    rep.check(count == manifest["rows"], "row count == manifest.rows", f"manifest: {manifest['rows']:,}")

    kmin, kmax, kdist = con.execute(
        f"SELECT min(CAST(chave_sequencial AS BIGINT)), max(CAST(chave_sequencial AS BIGINT)), "
        f"count(DISTINCT chave_sequencial) FROM {f}"
    ).fetchone()
    rep.check(
        kmin == 1 and kmax == count and kdist == count,
        "chave_sequencial is a permutation of 1..rows",
        f"min={kmin} max={kmax} distinct={kdist:,}",
    )
    cdist = con.execute(f"SELECT count(DISTINCT contrato) FROM {f}").fetchone()[0]
    rep.check(cdist == count, "contrato is unique", f"distinct={cdist:,}")
    cmin, cmax = con.execute(
        f"SELECT min(length(CAST(contrato AS VARCHAR))), max(length(CAST(contrato AS VARCHAR))) FROM {f}"
    ).fetchone()
    rep.check(7 <= cmin and cmax <= 12, "contrato has 7..12 digits", f"lengths {cmin}..{cmax}")

    # --- nulls -----------------------------------------------------------
    null_counts = con.execute(
        "SELECT " + ", ".join(f"count(*) - count({c})" for c in S.COLUMN_ORDER) + f" FROM {f}"
    ).fetchone()
    nulls = dict(zip(S.COLUMN_ORDER, null_counts))
    bad = {k: v for k, v in nulls.items() if v and k != "vr_renegociado"}
    rep.check(not bad, "nulls only in vr_renegociado", f"vr_renegociado nulls={nulls['vr_renegociado']:,}" + (f" OFFENDERS={bad}" if bad else ""))

    # --- per-customer consistency ----------------------------------------
    per_customer = con.execute(
        f"""SELECT n, count(*) FROM (SELECT co_cpf_cnpj, count(*) n FROM {f} GROUP BY 1) GROUP BY 1 ORDER BY 1"""
    ).fetchall()
    hist = {str(n): c for n, c in per_customer}
    rep.check(hist == manifest["contracts_per_customer_histogram"], "contracts-per-customer histogram == manifest",
              f"{sum(hist.values()):,} customers")

    mixed, multi, obito_bad = con.execute(
        f"""SELECT count(*) FILTER (WHERE mn = 0 AND mx > 90),
                   count(*) FILTER (WHERE n >= 2),
                   count(*) FILTER (WHERE obitos > 1)
            FROM (SELECT co_cpf_cnpj, min(CAST(qt_dias_atraso AS BIGINT)) mn, max(CAST(qt_dias_atraso AS BIGINT)) mx, count(*) n,
                         count(DISTINCT flag_obito) obitos
                  FROM {f} GROUP BY 1)"""
    ).fetchone()
    rep.check(mixed == manifest["customers_mixed"], "mixed customers (>90d and 0d) == manifest", f"{mixed:,}")
    pct_multi = 100.0 * mixed / multi if multi else 0.0
    rep.check(pct_multi >= 100 * S.MIN_MIXED_SHARE_OF_MULTI, "mixed share of multi-contract customers >= 15%", f"{pct_multi:.2f}%")
    rep.check(obito_bad == 0, "flag_obito constant per customer", f"violations={obito_bad}")

    acum_bad = con.execute(
        f"SELECT count(*) FROM {f} WHERE CAST(qt_dias_atraso_acumulado AS BIGINT) < CAST(qt_dias_atraso AS BIGINT)"
    ).fetchone()[0]
    rep.check(acum_bad == 0, "qt_dias_atraso_acumulado >= qt_dias_atraso", f"violations={acum_bad}")

    # --- documents ---------------------------------------------------------
    # lpad truncates longer strings, so only pad CPFs that lost leading zeros (BIGINT profiles).
    doc_expr = (
        "CASE WHEN length(CAST(co_cpf_cnpj AS VARCHAR)) < 11 THEN lpad(CAST(co_cpf_cnpj AS VARCHAR), 11, '0') "
        "ELSE CAST(co_cpf_cnpj AS VARCHAR) END"
    )
    lens = con.execute(f"SELECT length({doc_expr}) l, count(*) FROM {f} GROUP BY 1 ORDER BY 1").fetchall()
    rep.check(all(l in (11, 14) for l, _ in lens), "co_cpf_cnpj has 11 (CPF) or 14 (CNPJ) digits", str(dict(lens)))
    docs = con.execute(f"SELECT DISTINCT {doc_expr} FROM {f} LIMIT {args.doc_sample}").fetchall()
    valid = sum(1 for (d,) in docs if (cpf_valid(d) if len(d) == 11 else cnpj_valid(d)))
    rep.check(valid == 0, "no document passes the CPF/CNPJ check-digit test", f"sample={len(docs):,} valid={valid}")

    # --- checksums --------------------------------------------------------
    carteira = dict(con.execute(f"SELECT carteira, count(*) FROM {f} GROUP BY 1").fetchall())
    rep.check(carteira == manifest["carteira_counts"], "carteira counts == manifest", str(carteira))
    s1, s2 = con.execute(
        f"SELECT sum(CAST(saldo AS DECIMAL(18,2))), sum(CAST(saldo_limite AS DECIMAL(18,2))) FROM {f}"
    ).fetchone()
    rep.check(str(s1) == manifest["checksums"]["sum_saldo"], "sum(saldo) == manifest", str(s1))
    rep.check(str(s2) == manifest["checksums"]["sum_saldo_limite"], "sum(saldo_limite) == manifest", str(s2))

    # --- size -------------------------------------------------------------
    size = path.stat().st_size
    rep.check(size == manifest["size_bytes"], "file size == manifest", f"{size:,} bytes")
    row_groups = con.execute(f"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{path.as_posix()}')").fetchone()[0]
    rep.check(row_groups == manifest["row_groups"], "row groups == manifest", str(row_groups))
    target_bpr = S.TARGET_BYTES / S.REFERENCE_ROWS
    bpr = size / count
    # chave_sequencial is bit-packed over 1..N, so smaller samples under-estimate
    # the full-size file by ~log2(N_ref/N)/8 bytes per row.
    key_correction = max(0.0, (math.log2(S.REFERENCE_ROWS) - math.log2(count)) / 8) if count < S.REFERENCE_ROWS else 0.0
    projected = (bpr + key_correction) * S.REFERENCE_ROWS
    within = abs(projected / S.TARGET_BYTES - 1) <= S.TARGET_TOLERANCE
    note = "" if count == S.REFERENCE_ROWS else f" (projected from {count:,} rows, +{key_correction:.2f} B/row key-width correction)"
    rep.warn(within, f"size within ±{S.TARGET_TOLERANCE:.0%} of {S.TARGET_BYTES / 1e9:.2f} GB{note}",
             f"{bpr:.2f} bytes/row vs {target_bpr:.2f} target -> {projected / 1e9:.3f} GB ({projected / S.TARGET_BYTES - 1:+.1%})")

    if not args.skip_profile:
        run_profile_sql(con, Path(args.profile_sql), path, Path(args.profile_out))

    print(f"\n{rep.failures} failure(s), {rep.warnings} warning(s)")
    return 1 if rep.failures else 0


if __name__ == "__main__":
    sys.exit(main())
