#!/usr/bin/env python3
"""Synthetic generator for the ratings input Parquet file.

Design (customer first, then contracts):
  1. Build a ``customers`` table: each customer gets a number of contracts
     (configurable distribution rescaled to the target mean), a risk profile
     (clean / mixed / default), a document (CPF/CNPJ with invalid check digits)
     and a flag_obito value.
  2. Expand customers into contracts with ``unnest(range(n_contracts))`` and
     derive every per-row column from deterministic hash-based randomness.
  3. ``COPY ... TO`` Parquet in one streaming pass.

All randomness is ``hash(int)`` based, so the output is fully deterministic
for a given --seed regardless of the number of DuckDB threads.

Why DuckDB instead of numpy + pyarrow.ParquetWriter: the whole pipeline
(customer sizing, cumulative offsets, expansion, shuffling, Parquet writing)
is a single vectorized, multi-threaded, out-of-core SQL plan; no Python-side
chunk loop or memory management is needed, and the same engine is reused by
the validation step.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import shutil
import sys
import time
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from generator import schema as S  # noqa: E402

# ---------------------------------------------------------------------------
# Deterministic randomness helpers (SQL fragments)
# ---------------------------------------------------------------------------

# Named salts: every independent random stream gets its own id (< 1024).
SALTS = {
    name: i
    for i, name in enumerate(
        [
            "n_contracts",
            "n_contracts_range",
            "risk",
            "doc_type",
            "obito",
            "cpf_dv1",
            "cpf_dv2",
            "key",
            "order",
            "product",
            "days_z1",
            "days_z2",
            "mixed_extra",
            "acum",
            "acum_extra",
            "saldo_z1",
            "saldo_z2",
            "limite_z1",
            "limite_z2",
            "elegivel",
            "prejuizo",
            "reneg",
            "reneg_z1",
            "reneg_z2",
        ],
        start=1,
    )
}

TWO_POW_53 = float(1 << 53)


class Rng:
    """Builds SQL expressions for seeded, hash-based uniform/normal variates."""

    def __init__(self, seed: int):
        self.seed_offset = (seed * (1 << 40)) % (1 << 62)

    def uniform(self, key: str, salt: str) -> str:
        """Uniform [0, 1) derived from an integer key column and a salt name."""
        return (
            f"((hash(({key}) * 1024 + {SALTS[salt]} + {self.seed_offset}) >> 11)"
            f" / {TWO_POW_53!r})"
        )

    def normal(self, key: str, salt1: str, salt2: str) -> str:
        """Standard normal via Box-Muller from two independent uniforms."""
        u1 = self.uniform(key, salt1)
        u2 = self.uniform(key, salt2)
        return f"(sqrt(-2.0 * ln(greatest({u1}, 1e-300))) * cos(2.0 * pi() * {u2}))"


def lognormal(z_expr: str, median: float, sigma: float) -> str:
    return f"exp({math.log(median)!r} + {sigma!r} * {z_expr})"


def digit(n_expr: str, n_digits: int, position: int) -> str:
    """1-based digit position (left to right) of an integer with n_digits digits."""
    return f"(({n_expr}) // {10 ** (n_digits - position)} % 10)"


def check_digit(n_expr: str, n_digits: int, weights: list[int]) -> str:
    """Brazilian mod-11 check digit of an integer rendered with n_digits digits."""
    terms = " + ".join(f"{digit(n_expr, n_digits, i + 1)} * {w}" for i, w in enumerate(weights))
    return f"(CASE WHEN ({terms}) % 11 < 2 THEN 0 ELSE 11 - ({terms}) % 11 END)"


def coprime_multiplier(rnd: random.Random, modulus: int, upper: int | None = None) -> int:
    """Random multiplier coprime with modulus (gives a bijection x -> a*x mod m)."""
    upper = min(modulus - 1, upper or modulus - 1)
    lower = max(2, upper // 3)
    while True:
        a = rnd.randrange(lower, upper + 1)
        if math.gcd(a, modulus) == 1:
            return a


# ---------------------------------------------------------------------------
# Distribution configuration
# ---------------------------------------------------------------------------


def build_contracts_dist(raw: list, avg: float) -> list[tuple[int, int, float]]:
    """Normalize buckets and rescale so the expected contracts/customer == avg.

    The mass of every bucket with >= 2 contracts is multiplied by a single
    factor f and the single-contract bucket absorbs the remainder.
    """
    buckets = [(int(lo), int(hi), float(p)) for lo, hi, p in raw]
    for lo, hi, p in buckets:
        if lo < 1 or hi < lo or p < 0:
            raise ValueError(f"Invalid contracts bucket {[lo, hi, p]}")
    total = sum(p for _, _, p in buckets)
    buckets = [(lo, hi, p / total) for lo, hi, p in buckets]
    singles = [b for b in buckets if b[0] == 1 and b[1] == 1]
    multis = [b for b in buckets if not (b[0] == 1 and b[1] == 1)]
    if len(singles) != 1 or not multis:
        raise ValueError("contracts distribution needs exactly one [1,1] bucket and at least one other")
    s_p = sum(p for _, _, p in multis)
    s_m = sum(p * (lo + hi) / 2 for lo, hi, p in multis)
    f = (avg - 1.0) / (s_m - s_p)
    if f <= 0 or f * s_p >= 1:
        raise ValueError(f"avg={avg} is not reachable with the given buckets")
    rescaled = [(1, 1, 1.0 - f * s_p)] + [(lo, hi, p * f) for lo, hi, p in multis]
    return rescaled


def dist_mean(dist: list[tuple[int, int, float]]) -> float:
    return sum(p * (lo + hi) / 2 for lo, hi, p in dist)


def build_risk_mix(raw: dict, p_multi: float) -> dict:
    """Return per-group risk thresholds given the share of multi-contract customers."""
    mix = {k: float(raw.get(k, 0.0)) for k in ("clean", "mixed", "default")}
    total = sum(mix.values())
    if total <= 0:
        raise ValueError("risk mix must have positive mass")
    mix = {k: v / total for k, v in mix.items()}
    # "mixed" only exists for multi-contract customers.
    mixed_multi = max(mix["mixed"] / p_multi if p_multi > 0 else 0.0, S.MIN_MIXED_SHARE_OF_MULTI)
    mixed_multi = min(mixed_multi, 0.95)
    rest = mix["clean"] + mix["default"]
    clean_ratio = mix["clean"] / rest if rest > 0 else 1.0
    return {
        "input": mix,
        "single": {"clean": clean_ratio, "default": 1.0 - clean_ratio},
        "multi": {
            "clean": (1.0 - mixed_multi) * clean_ratio,
            "mixed": mixed_multi,
            "default": (1.0 - mixed_multi) * (1.0 - clean_ratio),
        },
    }


def n_contracts_case(dist: list[tuple[int, int, float]], u: str, u_range: str) -> str:
    parts = []
    cum = 0.0
    for i, (lo, hi, p) in enumerate(dist):
        cum += p
        value = str(lo) if lo == hi else f"{lo} + floor({u_range} * {hi - lo + 1})::INTEGER"
        if i == len(dist) - 1:
            parts.append(f"ELSE {value}")
        else:
            parts.append(f"WHEN {u} < {cum!r} THEN {value}")
    return "CASE " + " ".join(parts) + " END"


def product_lists() -> dict[str, str]:
    """1000-bucket lookup (quantized weights) and attribute lists as SQL literals."""
    total = sum(p.weight for p in S.PRODUCTS)
    buckets: list[int] = []
    acc = 0.0
    for idx, p in enumerate(S.PRODUCTS, start=1):
        acc += p.weight / total * 1000
        while len(buckets) < round(acc):
            buckets.append(idx)
    while len(buckets) < 1000:
        buckets.append(len(S.PRODUCTS))
    return {
        "bucket_to_idx": "[" + ",".join(map(str, buckets[:1000])) + "]",
        "codes": "[" + ",".join(str(p.cod_produto) for p in S.PRODUCTS) + "]",
        "modalidades": "[" + ",".join(str(p.modalidade) for p in S.PRODUCTS) + "]",
        "carteiras": "[" + ",".join(f"'{p.carteira}'" for p in S.PRODUCTS) + "]",
        "has_limit": "[" + ",".join("true" if p.has_limit else "false" for p in S.PRODUCTS) + "]",
    }


def contract_id_segments(rows: int, rnd: random.Random) -> list[dict]:
    """Split the permutation space [0, rows) into disjoint contract-id segments."""
    segments = []
    cum = 0
    shares = [s for s, _, _ in S.CONTRACT_ID_SEGMENTS]
    for i, (share, lo, hi) in enumerate(S.CONTRACT_ID_SEGMENTS):
        count = rows - cum if i == len(shares) - 1 else int(round(rows * share))
        size = hi - lo
        if count > size:
            raise ValueError(
                f"contract id segment [{lo},{hi}) has capacity {size} but needs {count} ids; "
                "adjust CONTRACT_ID_SEGMENTS in schema.py"
            )
        segments.append(
            {
                "p_lo": cum,
                "p_hi": cum + count,
                "lo": lo,
                "size": size,
                "mult": coprime_multiplier(rnd, size, upper=(1 << 62) // max(rows, 1)),
            }
        )
        cum += count
    return segments


def contract_id_case(segments: list[dict], p: str) -> str:
    parts = []
    for i, seg in enumerate(segments):
        value = f"{seg['lo']} + ((({p}) - {seg['p_lo']}) * {seg['mult']}) % {seg['size']}"
        if i == len(segments) - 1:
            parts.append(f"ELSE {value}")
        else:
            parts.append(f"WHEN {p} < {seg['p_hi']} THEN {value}")
    return "CASE " + " ".join(parts) + " END"


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


class GenerationConfig:
    def __init__(self, args: argparse.Namespace, rows: int, profile_name: str, out: Path, key_stride: int = 1):
        self.rows = rows
        # Calibration only: spreads chave_sequencial over 1..rows*key_stride so the
        # column has the bit width of the full-size file (its size grows with log2(N)).
        self.key_stride = key_stride
        self.seed = args.seed
        self.profile = S.PROFILES[profile_name]
        self.types = S.resolve_types(self.profile, args.decimal)
        self.decimal = args.decimal
        self.compression = args.compression
        self.row_group_size = args.row_group_size
        self.order = args.order
        self.data_referencia = args.data_referencia
        self.out = out
        self.avg = args.avg_contracts_per_customer
        raw_dist = json.loads(args.contracts_dist) if args.contracts_dist else S.DEFAULT_CONTRACTS_DIST
        self.contracts_dist = build_contracts_dist(raw_dist, self.avg)
        raw_mix = json.loads(args.risk_mix) if args.risk_mix else S.DEFAULT_RISK_MIX
        self.p_multi = 1.0 - self.contracts_dist[0][2]
        self.risk = build_risk_mix(raw_mix, self.p_multi)
        self.rnd = random.Random(self.seed)
        self.cpf_mult = coprime_multiplier(self.rnd, 10**9)
        self.cnpj_mult = coprime_multiplier(self.rnd, 9 * 10**7)
        self.perm_mult = coprime_multiplier(self.rnd, rows) if rows > 2 else 1
        self.perm_shift = self.rnd.randrange(rows)
        self.segments = contract_id_segments(rows, self.rnd)
        # 2% margin + fixed slack so the cumulative sum always reaches `rows`.
        self.customers_to_generate = int(math.ceil(rows / self.avg * 1.02)) + 1000


def customers_sql(cfg: GenerationConfig) -> str:
    rng = Rng(cfg.seed)
    cid = "customer_id"
    u_n = rng.uniform(cid, "n_contracts")
    u_nr = rng.uniform(cid, "n_contracts_range")
    n_case = n_contracts_case(cfg.contracts_dist, u_n, u_nr)

    cpf_base = f"(({cid} * {cfg.cpf_mult}) % 1000000000)"
    cnpj_root = f"(10000000 + ({cid} * {cfg.cnpj_mult}) % 90000000)"
    cnpj_base = f"({cnpj_root} * 10000 + 1)"  # root + branch '0001'
    cpf_dv = check_digit(cpf_base, 9, [10, 9, 8, 7, 6, 5, 4, 3, 2])
    cnpj_dv = check_digit(cnpj_base, 12, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    u_dv1 = rng.uniform(cid, "cpf_dv1")
    u_dv2 = rng.uniform(cid, "cpf_dv2")
    # First check digit is shifted by 1..9 so the document is always invalid.
    wrong_dv = lambda dv: f"(({dv} + 1 + floor({u_dv1} * 9)::INTEGER) % 10)"  # noqa: E731
    dv2 = f"floor({u_dv2} * 10)::INTEGER"
    doc = (
        f"CASE WHEN {rng.uniform(cid, 'doc_type')} < {S.CPF_SHARE!r} "
        f"THEN lpad({cpf_base}::VARCHAR, 9, '0') || {wrong_dv(cpf_dv)}::VARCHAR || ({dv2})::VARCHAR "
        f"ELSE {cnpj_base}::VARCHAR || {wrong_dv(cnpj_dv)}::VARCHAR || ({dv2})::VARCHAR END"
    )

    r = cfg.risk
    u_risk = "u_risk"
    risk_case = (
        f"CASE WHEN n_contracts = 1 THEN "
        f"(CASE WHEN {u_risk} < {r['single']['clean']!r} THEN 'clean' ELSE 'default' END) "
        f"ELSE (CASE WHEN {u_risk} < {r['multi']['clean']!r} THEN 'clean' "
        f"WHEN {u_risk} < {r['multi']['clean'] + r['multi']['mixed']!r} THEN 'mixed' "
        f"ELSE 'default' END) END"
    )

    return f"""
CREATE OR REPLACE TEMP TABLE customers AS
WITH sized AS (
    SELECT customer_id,
           {n_case} AS n_raw,
           {rng.uniform(cid, 'risk')} AS u_risk,
           ({rng.uniform(cid, 'obito')} < {S.FLAG_OBITO_RATE!r})::INTEGER AS flag_obito,
           {doc} AS co_cpf_cnpj
    FROM range(1, {cfg.customers_to_generate + 1}) t(customer_id)
), offsets AS (
    SELECT *, sum(n_raw) OVER (ORDER BY customer_id ROWS UNBOUNDED PRECEDING) - n_raw AS row_offset
    FROM sized
), truncated AS (
    SELECT customer_id, co_cpf_cnpj, flag_obito, u_risk, row_offset,
           least(n_raw, {cfg.rows} - row_offset)::INTEGER AS n_contracts
    FROM offsets
    WHERE row_offset < {cfg.rows}
)
SELECT customer_id, co_cpf_cnpj, flag_obito, row_offset, n_contracts,
       {risk_case} AS risk
FROM truncated
ORDER BY customer_id;
"""


def contracts_select_sql(cfg: GenerationConfig) -> str:
    rng = Rng(cfg.seed)
    row = "row_idx"
    plist = product_lists()
    days_median, days_sigma = S.DAYS_LATE_LOGNORMAL
    bad_median, bad_sigma = S.DAYS_LATE_BAD_LOGNORMAL
    z_days = rng.normal(row, "days_z1", "days_z2")
    tail_days = (
        f"least({S.DAYS_LATE_MAX}, greatest(1, round({lognormal(z_days, days_median, days_sigma)})))::INTEGER"
    )
    bad_days = (
        f"least({S.DAYS_LATE_MAX}, greatest({S.DAYS_LATE_BAD_MIN}, "
        f"round({lognormal(z_days, bad_median, bad_sigma)})))::INTEGER"
    )
    days_case = f"""CASE risk
            WHEN 'clean' THEN 0
            WHEN 'default' THEN {tail_days}
            ELSE CASE
                WHEN k = 0 THEN {bad_days}
                WHEN k = 1 THEN 0
                WHEN {rng.uniform(row, 'mixed_extra')} < {S.MIXED_EXTRA_CONTRACT_LATE_PROB!r} THEN {tail_days}
                ELSE 0 END
        END"""

    perm = f"((({row} - 1) * {cfg.perm_mult} + {cfg.perm_shift}) % {cfg.rows})"
    contrato = contract_id_case(cfg.segments, "perm_idx")

    saldo_med, saldo_sigma = S.SALDO_LOGNORMAL
    lim_med, lim_sigma = S.SALDO_LIMITE_LOGNORMAL
    ren_med, ren_sigma = S.VR_RENEGOCIADO_LOGNORMAL

    canonical = {
        "data_referencia": f"TIMESTAMP '{cfg.data_referencia}'",
        "chave_sequencial": "chave_sequencial",
        "co_cpf_cnpj": "co_cpf_cnpj",
        "contrato": "contrato",
        "qt_dias_atraso": "qt_dias_atraso",
        "saldo": "saldo",
        "saldo_limite": "saldo_limite",
        "ctr_elegivel": "ctr_elegivel",
        "carteira": "carteira",
        "cod_produto": "cod_produto",
        "flag_prejuizo_2682": "flag_prejuizo_2682",
        "flag_obito": "flag_obito",
        "qt_dias_atraso_acumulado": "qt_dias_atraso_acumulado",
        "modalidade": "modalidade",
        "vr_renegociado": "vr_renegociado",
    }
    rendered = ",\n       ".join(
        f"{S.render_column(name, canonical[name], cfg.types[name], cfg.profile)} AS {name}"
        for name in S.COLUMN_ORDER
    )
    order_by = rng.uniform(row, "order") if cfg.order == "random" else row

    key_scale = f" * {cfg.key_stride}" if cfg.key_stride != 1 else ""
    return f"""
WITH expanded AS (
    SELECT customer_id, co_cpf_cnpj, flag_obito, risk, row_offset,
           unnest(range(n_contracts)) AS k
    FROM customers
), base AS (
    SELECT *, (row_offset + k + 1)::BIGINT AS row_idx FROM expanded
), keyed AS (
    SELECT *, row_number() OVER (ORDER BY {rng.uniform(row, 'key')}){key_scale} AS chave_sequencial
    FROM base
), attrs AS (
    SELECT *,
           {perm} AS perm_idx,
           {plist['bucket_to_idx']}[floor({rng.uniform(row, 'product')} * 1000)::INTEGER + 1] AS product_idx,
           {days_case} AS qt_dias_atraso,
           {rng.normal(row, 'saldo_z1', 'saldo_z2')} AS z_saldo,
           {rng.normal(row, 'limite_z1', 'limite_z2')} AS z_limite,
           {rng.normal(row, 'reneg_z1', 'reneg_z2')} AS z_reneg
    FROM keyed
), derived AS (
    SELECT *,
           {contrato} AS contrato,
           {plist['codes']}[product_idx] AS cod_produto,
           {plist['modalidades']}[product_idx] AS modalidade,
           {plist['carteiras']}[product_idx] AS carteira,
           round(greatest(0.01, least({S.SALDO_MAX!r}, {lognormal('z_saldo', saldo_med, saldo_sigma)})), 2) AS saldo,
           CASE WHEN {plist['has_limit']}[product_idx]
                THEN round(greatest({S.SALDO_LIMITE_MIN!r}, {lognormal('z_limite', lim_med, lim_sigma)}), 2)
                ELSE 0.0 END AS saldo_limite,
           CASE WHEN {rng.uniform(row, 'elegivel')} < {S.CTR_ELEGIVEL_S_PROB!r} THEN 'S' ELSE 'N' END AS ctr_elegivel,
           (CASE WHEN qt_dias_atraso > {S.PREJUIZO_DAYS_THRESHOLD}
                 THEN {rng.uniform(row, 'prejuizo')} < {S.PREJUIZO_PROB_OVER_360!r}
                 ELSE {rng.uniform(row, 'prejuizo')} < {S.PREJUIZO_PROB_OTHERWISE!r} END)::INTEGER AS flag_prejuizo_2682,
           qt_dias_atraso + CASE WHEN {rng.uniform(row, 'acum')} < {S.ACUMULADO_GREATER_PROB!r}
                THEN 1 + floor({rng.uniform(row, 'acum_extra')} * {S.ACUMULADO_EXTRA_DAYS_MAX})::INTEGER
                ELSE 0 END AS qt_dias_atraso_acumulado,
           CASE WHEN {rng.uniform(row, 'reneg')} < {S.VR_RENEGOCIADO_PRESENT_PROB!r}
                THEN round({lognormal('z_reneg', ren_med, ren_sigma)}, 2) END AS vr_renegociado
    FROM attrs
)
SELECT {rendered}
FROM derived
ORDER BY {order_by}
"""


def copy_options(cfg: GenerationConfig) -> str:
    return f"(FORMAT PARQUET, COMPRESSION '{cfg.compression}', ROW_GROUP_SIZE {cfg.row_group_size})"


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    if args.threads:
        con.execute(f"SET threads = {args.threads}")
    tmp = Path(args.temp_dir)
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{tmp.as_posix()}'")
    con.execute("SET preserve_insertion_order = true")
    return con


def generate(con: duckdb.DuckDBPyConnection, cfg: GenerationConfig, log=print) -> dict:
    cfg.out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    con.execute(customers_sql(cfg))
    n_customers, total = con.execute("SELECT count(*), sum(n_contracts) FROM customers").fetchone()
    if total != cfg.rows:
        raise RuntimeError(f"customer expansion produced {total} rows, expected {cfg.rows}")
    t1 = time.perf_counter()
    log(f"  customers: {n_customers:,} ({t1 - t0:.1f}s)")

    con.execute(f"COPY ({contracts_select_sql(cfg)}) TO '{cfg.out.as_posix()}' {copy_options(cfg)}")
    t2 = time.perf_counter()
    log(f"  contracts written: {cfg.rows:,} rows -> {cfg.out} ({t2 - t1:.1f}s)")

    stats = {
        "seed": cfg.seed,
        "type_profile": cfg.profile.name,
        "decimal": cfg.decimal,
        "compression": cfg.compression,
        "row_group_size": cfg.row_group_size,
        "order": cfg.order,
        "data_referencia": cfg.data_referencia,
        "rows": cfg.rows,
        "customers": n_customers,
        "generation_seconds": round(t2 - t0, 2),
        "generation": {
            "avg_contracts_per_customer_target": cfg.avg,
            "contracts_dist_rescaled": [[lo, hi, round(p, 6)] for lo, hi, p in cfg.contracts_dist],
            "contracts_dist_mean": round(dist_mean(cfg.contracts_dist), 4),
            "risk_mix": cfg.risk,
        },
    }
    stats.update(customer_stats(con))
    stats.update(file_stats(con, cfg.out))
    return stats


def customer_stats(con: duckdb.DuckDBPyConnection) -> dict:
    hist = con.execute(
        "SELECT n_contracts, count(*) FROM customers GROUP BY 1 ORDER BY 1"
    ).fetchall()
    risk = dict(con.execute("SELECT risk, count(*) FROM customers GROUP BY 1").fetchall())
    customers = sum(risk.values())
    multi = con.execute("SELECT count(*) FROM customers WHERE n_contracts >= 2").fetchone()[0]
    mixed = risk.get("mixed", 0)
    return {
        "contracts_per_customer_histogram": {str(n): c for n, c in hist},
        "risk_profile_counts": risk,
        "customers_mixed": mixed,
        "pct_customers_mixed": round(100.0 * mixed / customers, 4),
        "customers_multi_contract": multi,
        "pct_multi_contract_customers_mixed": round(100.0 * mixed / multi, 4) if multi else 0.0,
    }


def file_stats(con: duckdb.DuckDBPyConnection, path: Path) -> dict:
    """Metrics read back from the written Parquet file."""
    p = path.as_posix()
    size = path.stat().st_size
    row_groups = con.execute(
        f"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{p}')"
    ).fetchone()[0]
    schema = con.execute(
        f"""SELECT name, type, converted_type, logical_type, scale, precision
            FROM parquet_schema('{p}') WHERE num_children IS NULL OR num_children = 0"""
    ).fetchall()
    carteira = con.execute(
        f"SELECT carteira, count(*) FROM read_parquet('{p}') GROUP BY 1 ORDER BY 1"
    ).fetchall()
    sums = con.execute(
        f"""SELECT sum(CAST(saldo AS DECIMAL(18,2))), sum(CAST(saldo_limite AS DECIMAL(18,2)))
            FROM read_parquet('{p}')"""
    ).fetchone()
    return {
        "file": p,
        "size_bytes": size,
        "bytes_per_row": round(size / max(1, con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0]), 3),
        "row_groups": row_groups,
        "parquet_schema": [
            {
                "name": r[0],
                "type": r[1],
                "converted_type": r[2],
                "logical_type": str(r[3]) if r[3] is not None else None,
                "scale": r[4],
                "precision": r[5],
            }
            for r in schema
        ],
        "carteira_counts": {c: n for c, n in carteira},
        "checksums": {
            "sum_saldo": str(sums[0]),
            "sum_saldo_limite": str(sums[1]),
        },
    }


def write_split(con: duckdb.DuckDBPyConnection, cfg: GenerationConfig, n: int, log=print) -> list[dict]:
    """Also write the same rows as N contiguous files (for parallel load tests)."""
    split_dir = cfg.out.parent / "split"
    if split_dir.exists():
        shutil.rmtree(split_dir)
    split_dir.mkdir(parents=True)
    chunk = math.ceil(cfg.rows / n)
    parts = []
    t0 = time.perf_counter()
    for i in range(n):
        part = split_dir / f"{cfg.out.stem}_part{i:03d}.parquet"
        con.execute(
            f"""COPY (SELECT * EXCLUDE (file_row_number)
                      FROM read_parquet('{cfg.out.as_posix()}', file_row_number = true)
                      WHERE file_row_number >= {i * chunk} AND file_row_number < {(i + 1) * chunk}
                      ORDER BY file_row_number)
                TO '{part.as_posix()}' {copy_options(cfg)}"""
        )
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{part.as_posix()}')").fetchone()[0]
        parts.append({"file": part.as_posix(), "rows": rows, "size_bytes": part.stat().st_size})
    log(f"  split: {n} files in {split_dir} ({time.perf_counter() - t0:.1f}s)")
    return parts


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------


def calibrate(args: argparse.Namespace) -> None:
    rows = args.calibrate_rows
    target_rows = args.rows
    results = []
    print(f"Calibration: {rows:,} rows per profile, projecting to {target_rows:,} rows")
    print(f"Target: {S.TARGET_BYTES / 1e9:.2f} GB ({S.TARGET_BYTES / S.REFERENCE_ROWS:.1f} bytes/row), tolerance ±{S.TARGET_TOLERANCE:.0%}")
    key_stride = max(1, target_rows // rows)
    print(f"chave_sequencial scaled by {key_stride}x to match the bit width of a {target_rows:,}-row file\n")
    calib_dir = Path(args.out).parent / "calibration"
    calib_dir.mkdir(parents=True, exist_ok=True)
    for name in S.CALIBRATION_PROFILES:
        out = calib_dir / f"calib_{name}{'_decimal' if args.decimal else ''}.parquet"
        cfg = GenerationConfig(args, rows, name, out, key_stride=key_stride)
        con = connect(args)
        print(f"[{name}]")
        stats = generate(con, cfg, log=print)
        con.close()
        bpr = stats["size_bytes"] / rows
        projected = bpr * target_rows
        results.append((name, bpr, projected, stats["generation_seconds"]))
        if not args.keep_calibration_files:
            out.unlink(missing_ok=True)

    print("\n{:<10} {:>12} {:>16} {:>10} {:>8}".format("profile", "bytes/row", "projected GB", "vs target", "secs"))
    best = None
    for name, bpr, projected, secs in results:
        diff = projected / S.TARGET_BYTES - 1
        print(f"{name:<10} {bpr:>12.2f} {projected / 1e9:>16.3f} {diff:>+9.1%} {secs:>8.1f}")
        if best is None or abs(diff) < abs(best[1]):
            best = (name, diff)
    name, diff = best
    print()
    if abs(diff) <= S.TARGET_TOLERANCE:
        print(f"Recommendation: --type-profile {name} ({diff:+.1%} vs target, within ±{S.TARGET_TOLERANCE:.0%})")
    else:
        print(
            f"Closest profile: {name} ({diff:+.1%} vs target) — outside ±{S.TARGET_TOLERANCE:.0%}; "
            "consider --decimal, another --compression/--row-group-size or editing PROFILE_REAL in schema.py"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a synthetic ratings input Parquet file with DuckDB.")
    p.add_argument("--rows", type=int, default=S.REFERENCE_ROWS)
    p.add_argument("--avg-contracts-per-customer", type=float, default=S.DEFAULT_AVG_CONTRACTS)
    p.add_argument("--contracts-dist", help='JSON list of [lo, hi, prob] buckets, e.g. \'[[1,1,0.5],[2,2,0.5]]\'')
    p.add_argument("--risk-mix", help='JSON object, e.g. \'{"clean":0.7,"mixed":0.15,"default":0.15}\'')
    p.add_argument("--type-profile", choices=sorted(S.PROFILES), default=S.DEFAULT_PROFILE)
    p.add_argument("--decimal", action="store_true", help="saldo/saldo_limite/vr_renegociado as DECIMAL(18,2) instead of DOUBLE")
    p.add_argument("--compression", default="snappy", choices=["snappy", "zstd", "gzip", "lz4", "uncompressed"])
    p.add_argument("--row-group-size", type=int, default=1_048_576)
    p.add_argument("--order", choices=["random", "by_customer"], default="random")
    p.add_argument("--data-referencia", default=S.DEFAULT_DATA_REFERENCIA, help="YYYY-MM-DD HH:MM:SS")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/ratings_input_synthetic.parquet")
    p.add_argument("--manifest", default=None, help="default: <out dir>/manifest.json")
    p.add_argument("--split", type=int, default=None, metavar="N", help="also write N contiguous files under <out dir>/split/")
    p.add_argument("--calibrate", action="store_true", help="measure bytes/row per type profile and recommend one")
    p.add_argument("--calibrate-rows", type=int, default=2_000_000)
    p.add_argument("--keep-calibration-files", action="store_true")
    p.add_argument("--memory-limit", default="8GB", help="DuckDB memory_limit (default keeps a 16 GB machine safe)")
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--temp-dir", default="data/.duckdb_tmp")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.rows < 2:
        print("--rows must be >= 2", file=sys.stderr)
        return 2
    if args.calibrate:
        calibrate(args)
        return 0

    out = Path(args.out)
    manifest_path = Path(args.manifest) if args.manifest else out.parent / "manifest.json"
    cfg = GenerationConfig(args, args.rows, args.type_profile, out)
    print(
        f"Generating {cfg.rows:,} rows | profile={cfg.profile.name} decimal={cfg.decimal} "
        f"compression={cfg.compression} row_group_size={cfg.row_group_size:,} order={cfg.order} seed={cfg.seed}"
    )
    print(
        f"  contracts/customer mean={dist_mean(cfg.contracts_dist):.3f}; "
        f"mixed share of multi-contract customers={cfg.risk['multi']['mixed']:.1%}"
    )
    con = connect(args)
    stats = generate(con, cfg)
    if args.split:
        stats["split_files"] = write_split(con, cfg, args.split)
    con.close()
    shutil.rmtree(args.temp_dir, ignore_errors=True)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(stats, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(
        f"Done: {stats['rows']:,} rows, {stats['customers']:,} customers, "
        f"{stats['size_bytes'] / 1e9:.3f} GB ({stats['bytes_per_row']} bytes/row), "
        f"{stats['row_groups']} row groups, {stats['generation_seconds']}s"
    )
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
