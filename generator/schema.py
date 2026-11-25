"""Single source of truth for the synthetic ratings input file.

Everything that describes the *shape* of the Parquet file lives here:
column names and order, per-profile physical types, how canonical values are
rendered into each type, the product catalog (cod_produto -> modalidade,
carteira) and the default statistical distributions used by the generator.

When the customer returns the real schema/profile, edit ``PROFILE_REAL`` (and,
if needed, the catalog/distributions) without touching the generator code.
"""

from __future__ import annotations

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Column definitions (exact header order of the customer's file)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    name: str
    # Canonical type produced by the generator SQL before profile rendering.
    canonical: str
    description: str
    nullable: bool = False


COLUMNS: tuple[Column, ...] = (
    Column("data_referencia", "TIMESTAMP", "Reference date, constant for the whole file"),
    Column("chave_sequencial", "BIGINT", "Unique permutation of 1..rows"),
    Column("co_cpf_cnpj", "VARCHAR", "Customer document (CPF 11 / CNPJ 14 digits, invalid check digits)"),
    Column("contrato", "BIGINT", "Contract id, unique per row, 7-12 digits"),
    Column("qt_dias_atraso", "INTEGER", "Days past due"),
    Column("saldo", "DOUBLE", "Outstanding balance, 2 decimals, > 0"),
    Column("saldo_limite", "DOUBLE", "Credit limit, 0 unless product has a limit"),
    Column("ctr_elegivel", "VARCHAR", "'S' / 'N'"),
    Column("carteira", "VARCHAR", "Portfolio 'C1'..'C5', derived from product"),
    Column("cod_produto", "INTEGER", "4-digit product code"),
    Column("flag_prejuizo_2682", "INTEGER", "0/1 write-off flag (Res. 2682)"),
    Column("flag_obito", "INTEGER", "0/1 deceased flag, constant per customer"),
    Column("qt_dias_atraso_acumulado", "INTEGER", ">= qt_dias_atraso"),
    Column("modalidade", "INTEGER", "3-digit modality, derived from product"),
    Column("vr_renegociado", "DOUBLE", "Renegotiated amount, null in ~95%", nullable=True),
)

COLUMN_ORDER: tuple[str, ...] = tuple(c.name for c in COLUMNS)
COLUMN_BY_NAME: dict[str, Column] = {c.name: c for c in COLUMNS}

# Format used whenever data_referencia is rendered as a string (as in the sample).
DATE_FORMAT_DUCKDB = "%d/%m/%Y %H:%M:%S"
DEFAULT_DATA_REFERENCIA = "2026-10-01 00:00:00"

# Columns affected by --decimal (only when the profile types them as DOUBLE).
DECIMAL_CANDIDATES: tuple[str, ...] = ("saldo", "saldo_limite", "vr_renegociado")
DECIMAL_TYPE = "DECIMAL(18,2)"

# ---------------------------------------------------------------------------
# Type profiles
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TypeProfile:
    name: str
    description: str
    # column name -> DuckDB output type
    types: dict[str, str]
    # Value written for empty vr_renegociado when the column is VARCHAR.
    # None keeps NULL; ' ' mimics the customer's sample.
    empty_vr_renegociado: str | None = None


_TYPED: dict[str, str] = {
    "data_referencia": "TIMESTAMP",
    "chave_sequencial": "BIGINT",
    "co_cpf_cnpj": "BIGINT",
    "contrato": "BIGINT",
    "qt_dias_atraso": "INTEGER",
    "saldo": "DOUBLE",
    "saldo_limite": "DOUBLE",
    "ctr_elegivel": "VARCHAR",
    "carteira": "VARCHAR",
    "cod_produto": "INTEGER",
    "flag_prejuizo_2682": "INTEGER",
    "flag_obito": "INTEGER",
    "qt_dias_atraso_acumulado": "INTEGER",
    "modalidade": "INTEGER",
    "vr_renegociado": "DOUBLE",
}

PROFILE_TYPED = TypeProfile(
    name="typed",
    description="timestamps, int32/int64, double and short strings",
    types=dict(_TYPED),
)

PROFILE_STRINGS = TypeProfile(
    name="strings",
    description="every column as VARCHAR (date dd/MM/yyyy HH:mm:ss, '.' decimals, blank vr_renegociado = ' ')",
    types={name: "VARCHAR" for name in COLUMN_ORDER},
    empty_vr_renegociado=" ",
)

PROFILE_MIXED = TypeProfile(
    name="mixed",
    description="numeric columns typed; data_referencia, co_cpf_cnpj and vr_renegociado as strings (like the sample)",
    types={
        **_TYPED,
        "data_referencia": "VARCHAR",
        "co_cpf_cnpj": "VARCHAR",
        "vr_renegociado": "VARCHAR",
    },
    empty_vr_renegociado=None,
)

# TODO(real-schema): fill in with the physical types returned by the customer's
# profiling script (sql/perfil_parquet.sql). Until then it mirrors "mixed".
PROFILE_REAL = TypeProfile(
    name="real",
    description="placeholder for the customer's real schema (currently identical to mixed)",
    types=dict(PROFILE_MIXED.types),
    empty_vr_renegociado=PROFILE_MIXED.empty_vr_renegociado,
)

PROFILES: dict[str, TypeProfile] = {
    p.name: p for p in (PROFILE_TYPED, PROFILE_STRINGS, PROFILE_MIXED, PROFILE_REAL)
}
DEFAULT_PROFILE = "mixed"
CALIBRATION_PROFILES: tuple[str, ...] = ("typed", "strings", "mixed")


def resolve_types(profile: TypeProfile, decimal: bool) -> dict[str, str]:
    """Return the effective column -> type map for a profile (+ --decimal)."""
    types = dict(profile.types)
    if decimal:
        for name in DECIMAL_CANDIDATES:
            if types[name] == "DOUBLE":
                types[name] = DECIMAL_TYPE
    return types


def render_column(name: str, expr: str, out_type: str, profile: TypeProfile) -> str:
    """SQL that converts a canonical-typed expression into the profile's output type."""
    canonical = COLUMN_BY_NAME[name].canonical
    if out_type == canonical:
        return expr
    if out_type.startswith("DECIMAL"):
        return f"CAST({expr} AS {out_type})"
    if out_type == "VARCHAR":
        if canonical == "TIMESTAMP":
            return f"strftime({expr}, '{DATE_FORMAT_DUCKDB}')"
        if canonical == "DOUBLE":
            rendered = f"printf('%.2f', {expr})"
            if name == "vr_renegociado" and profile.empty_vr_renegociado is not None:
                return f"coalesce({rendered}, '{profile.empty_vr_renegociado}')"
            return rendered
        return f"CAST({expr} AS VARCHAR)"
    if out_type in ("BIGINT", "INTEGER") and canonical == "VARCHAR":
        return f"CAST({expr} AS {out_type})"
    raise ValueError(f"No rendering rule for {name}: {canonical} -> {out_type}")


# ---------------------------------------------------------------------------
# Size target (customer's real file)
# ---------------------------------------------------------------------------

REFERENCE_ROWS = 30_076_105
TARGET_BYTES = 1_100_000_000  # ~1.10 GB, ~36.6 bytes/row
TARGET_TOLERANCE = 0.05

# ---------------------------------------------------------------------------
# Product catalog: (cod_produto, modalidade, carteira, has_limit, weight)
# Weight is the relative share of contracts. Products with has_limit=True
# carry a positive saldo_limite; their weights sum to ~20% of the total.
# 8211 / 203 / C5 comes from the customer's sample.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Product:
    cod_produto: int
    modalidade: int
    carteira: str
    has_limit: bool
    weight: float


PRODUCTS: tuple[Product, ...] = (
    Product(8211, 203, "C5", False, 14.0),
    Product(8212, 203, "C5", False, 6.0),
    Product(8305, 204, "C5", False, 3.0),
    Product(1101, 101, "C1", False, 6.5),
    Product(1102, 101, "C1", False, 4.0),
    Product(1203, 102, "C1", False, 3.0),
    Product(1305, 103, "C1", False, 2.0),
    Product(1401, 104, "C1", False, 1.0),
    Product(2301, 201, "C2", True, 5.5),
    Product(2302, 201, "C2", True, 4.0),
    Product(2405, 202, "C2", True, 3.0),
    Product(2501, 202, "C2", True, 2.0),
    Product(2601, 205, "C2", True, 1.5),
    Product(3101, 301, "C3", False, 4.0),
    Product(3102, 301, "C3", False, 3.0),
    Product(3205, 302, "C3", False, 2.0),
    Product(3301, 303, "C3", False, 1.5),
    Product(4101, 401, "C4", False, 3.5),
    Product(4202, 402, "C4", False, 2.5),
    Product(4303, 403, "C4", False, 1.5),
    Product(4401, 404, "C4", False, 1.0),
    Product(5101, 501, "C1", False, 2.5),
    Product(5102, 501, "C1", False, 1.5),
    Product(5203, 502, "C1", False, 1.0),
    Product(5301, 503, "C1", False, 0.5),
    Product(6101, 601, "C3", False, 2.0),
    Product(6102, 601, "C3", False, 1.5),
    Product(6201, 602, "C3", False, 1.0),
    Product(7101, 701, "C4", True, 1.5),
    Product(7202, 702, "C4", True, 1.0),
    Product(7303, 703, "C4", True, 0.5),
    Product(7401, 704, "C4", True, 0.5),
    Product(8101, 801, "C5", False, 2.0),
    Product(8102, 801, "C5", False, 1.5),
    Product(8405, 802, "C5", False, 1.0),
    Product(8501, 803, "C5", False, 0.5),
    Product(9101, 901, "C2", True, 0.3),
    Product(9102, 901, "C2", False, 0.4),
    Product(9203, 902, "C2", False, 0.4),
    Product(9301, 903, "C2", False, 0.4),
)

# ---------------------------------------------------------------------------
# Default generation distributions (all overridable through the CLI)
# ---------------------------------------------------------------------------

# Contracts per customer: list of [lo, hi, probability]. Probabilities are
# rescaled by the generator so that the mean equals --avg-contracts-per-customer.
DEFAULT_CONTRACTS_DIST: list[list[float]] = [
    [1, 1, 0.50],
    [2, 2, 0.22],
    [3, 3, 0.11],
    [4, 4, 0.06],
    [5, 5, 0.04],
    [6, 10, 0.05],
    [11, 30, 0.02],
]
DEFAULT_AVG_CONTRACTS = 2.5

# Customer risk profile shares. "mixed" is only assigned to multi-contract
# customers; the generator guarantees at least MIN_MIXED_SHARE_OF_MULTI of them.
DEFAULT_RISK_MIX: dict[str, float] = {"clean": 0.70, "mixed": 0.15, "default": 0.15}
MIN_MIXED_SHARE_OF_MULTI = 0.15

# Documents
CPF_SHARE = 0.92  # remaining customers get a CNPJ
FLAG_OBITO_RATE = 0.003

# Per-contract value distributions
DAYS_LATE_MAX = 3650
DAYS_LATE_LOGNORMAL = (300.0, 1.2)  # (median, sigma) for delinquent contracts
DAYS_LATE_BAD_LOGNORMAL = (400.0, 1.0)  # forced > 90 days for the "mixed" anchor contract
DAYS_LATE_BAD_MIN = 91
MIXED_EXTRA_CONTRACT_LATE_PROB = 0.50  # non-anchor contracts of a mixed customer
ACUMULADO_GREATER_PROB = 0.10
ACUMULADO_EXTRA_DAYS_MAX = 365
SALDO_LOGNORMAL = (2000.0, 1.3)  # (median, sigma)
SALDO_MAX = 1_000_000.0
SALDO_LIMITE_LOGNORMAL = (5000.0, 0.8)
SALDO_LIMITE_MIN = 100.0
CTR_ELEGIVEL_S_PROB = 0.60
PREJUIZO_PROB_OVER_360 = 0.85
PREJUIZO_PROB_OTHERWISE = 0.001
PREJUIZO_DAYS_THRESHOLD = 360
VR_RENEGOCIADO_PRESENT_PROB = 0.05
VR_RENEGOCIADO_LOGNORMAL = (1500.0, 1.0)

# contrato id segments: (share, lo_value, hi_value). Each segment is a disjoint
# numeric range, so ids are unique across segments; within a segment ids come
# from an affine bijection, so they are unique as long as the segment capacity
# (hi - lo) is not smaller than the number of rows assigned to it.
CONTRACT_ID_SEGMENTS: list[tuple[float, int, int]] = [
    (0.30, 1_000_000, 100_000_000),  # 7-8 digits
    (0.40, 100_000_000, 10_000_000_000),  # 9-10 digits
    (0.30, 10_000_000_000, 1_000_000_000_000),  # 11-12 digits
]
