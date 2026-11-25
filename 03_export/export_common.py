"""Shared pieces of the 03_export step.

- output column layout (15 input columns + rating_contrato + rating_final)
- $replaceWith projections for the flat and the grouped collections
- $out-to-S3 stage builder
- run_background_out(): launches the aggregate with background=True on the
  ADF and measures it with $queryHistory (exact server-side start/end), with
  an S3 polling fallback.

rating_contrato / rating_final are a SIMULATION (see 02_group_arrasto/rules.py).
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from pymongo import MongoClient

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "02_group_arrasto"))

from generator.schema import COLUMN_ORDER  # noqa: E402
from rules import CONTRACT_FIELDS  # noqa: E402

RATING_COLUMNS: tuple[str, ...] = ("rating_contrato", "rating_final")
OUTPUT_COLUMNS: tuple[str, ...] = COLUMN_ORDER + RATING_COLUMNS

# BSON type expected for every column, from results/type_mapping.md. Used to
# coerce explicitly so no column can come out polymorphic in the Parquet.
BSON_CAST = {
    "data_referencia": "$toString",
    "chave_sequencial": "$toLong",
    "co_cpf_cnpj": "$toString",
    "contrato": "$toLong",
    "qt_dias_atraso": "$toInt",
    "saldo": "$toDouble",
    "saldo_limite": "$toDouble",
    "ctr_elegivel": "$toString",
    "carteira": "$toString",
    "cod_produto": "$toInt",
    "flag_prejuizo_2682": "$toInt",
    "flag_obito": "$toInt",
    "qt_dias_atraso_acumulado": "$toInt",
    "modalidade": "$toInt",
    "vr_renegociado": "$toString",
    "rating_contrato": "$toString",
    "rating_final": "$toString",
}
NULLABLE = {"vr_renegociado"}


def _col(name: str, path: str) -> dict:
    """Typed expression for one output column; nullable columns keep an explicit null."""
    if name in NULLABLE:
        return {"$ifNull": [{BSON_CAST[name]: path}, None]}
    return {BSON_CAST[name]: path}


def replace_with_flat(include_rating: bool = False) -> dict:
    """contratos_flat document -> 15 (or 17) columns in Parquet order, no _id."""
    doc = {name: _col(name, f"${name}") for name in COLUMN_ORDER}
    if include_rating:
        doc["rating_contrato"] = _col("rating_contrato", "$rating_contrato")
        doc["rating_final"] = _col("rating_final", "$rating_final")
    return {"$replaceWith": doc}


def replace_with_grouped() -> dict:
    """Unwound clientes_agrupados document -> 17 columns.

    data_referencia, co_cpf_cnpj (= _id) and rating_final come from the
    customer document; everything else from the unwound contract.
    """
    doc: dict = {}
    for name in COLUMN_ORDER:
        if name == "co_cpf_cnpj":
            doc[name] = _col(name, "$_id")
        elif name == "data_referencia":
            doc[name] = _col(name, "$data_referencia")
        else:
            assert name in CONTRACT_FIELDS
            doc[name] = _col(name, f"$contratos.{name}")
    doc["rating_contrato"] = _col("rating_contrato", "$contratos.rating_contrato")
    doc["rating_final"] = _col("rating_final", "$rating_final")
    return {"$replaceWith": doc}


def pipeline_flat() -> list[dict]:
    return [replace_with_flat(include_rating=False)]


def pipeline_grouped() -> list[dict]:
    return [{"$unwind": "$contratos"}, replace_with_grouped()]


def out_s3(bucket: str, region: str, filename: str, max_file_size: str | None, compression: str = "snappy",
           max_row_group_size: str | None = None) -> dict:
    fmt: dict = {"name": "parquet", "columnCompression": compression}
    if max_file_size:
        fmt["maxFileSize"] = max_file_size
    if max_row_group_size:
        fmt["maxRowGroupSize"] = max_row_group_size
    return {"$out": {"s3": {"bucket": bucket, "region": region, "filename": filename, "format": fmt, "errorMode": "stop"}}}


# ---------------------------------------------------------------------------
# Execution + measurement
# ---------------------------------------------------------------------------


@dataclass
class OutResult:
    ok: bool
    seconds: float | None
    source: str  # "queryHistory" | "s3_polling" | "client"
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str = ""
    history: dict | None = None


def s3_client(region: str):
    return boto3.Session(region_name=region).client("s3")


def list_prefix(s3, bucket: str, prefix: str) -> list[dict]:
    objs: list[dict] = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        objs += resp.get("Contents", [])
        if not resp.get("IsTruncated"):
            return objs
        token = resp["NextContinuationToken"]


def find_history(adf: MongoClient, db: str, coll: str, not_before: datetime, filename: str) -> dict | None:
    """Most recent $queryHistory entry for our background $out (matched by db/coll/filename/time)."""
    rows = list(
        adf["admin"].aggregate(
            [
                {"$queryHistory": {}},
                {"$match": {"appName": "ratings-poc", "db": db, "collection": coll, "background": True,
                            "startTime": {"$gte": not_before - timedelta(seconds=5)}}},
                {"$sort": {"startTime": -1}},
                {"$limit": 5},
            ]
        )
    )
    for q in rows:
        for st in q.get("query", []):
            s3 = st.get("$out", {}).get("s3") if isinstance(st, dict) else None
            if s3 and s3.get("filename") == filename:
                return q
    return None


def run_background_out(adf: MongoClient, db: str, coll: str, pipeline: list[dict], filename: str,
                       s3, bucket: str, poll_seconds: float = 2.0, max_wait_seconds: int = 4 * 3600,
                       log=print) -> OutResult:
    """Launch `aggregate` with background=True and wait for completion.

    Primary source of truth: $queryHistory (endTime present -> finished; ok/error).
    Fallback: S3 polling (object under `filename` appears and size is stable),
    marked as estimated.
    """
    not_before = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    resp = adf[db].command("aggregate", coll, pipeline=pipeline, cursor={}, background=True)
    if resp.get("ok") != 1:
        return OutResult(False, None, "client", error=str(resp))
    log(f"  background aggregate accepted in {time.perf_counter() - t0:.2f}s; polling $queryHistory every {poll_seconds}s")

    last_log = time.perf_counter()
    stable: tuple[int, int] | None = None  # (total_bytes, n_objects)
    stable_since: float | None = None
    while time.perf_counter() - t0 < max_wait_seconds:
        time.sleep(poll_seconds)
        try:
            h = find_history(adf, db, coll, not_before, filename)
        except Exception as exc:  # ADF hiccup; keep polling
            log(f"  queryHistory error: {exc}")
            h = None
        if h and h.get("endTime"):
            dur = (h["endTime"] - h["startTime"]).total_seconds()
            ok = h.get("ok") == 1 and not h.get("error")
            h2 = {k: v for k, v in h.items() if k != "query"}
            return OutResult(ok, round(dur, 3), "queryHistory", h["startTime"], h["endTime"], h.get("error", ""), h2)

        objs = list_prefix(s3, bucket, filename)
        total = sum(o["Size"] for o in objs)
        if time.perf_counter() - last_log >= 60:
            log(f"  {time.perf_counter() - t0:7.0f}s  history={'running' if h else 'not yet visible'}  s3: {len(objs)} obj, {total / 1e6:,.1f} MB")
            last_log = time.perf_counter()
        # S3 fallback only kicks in if history never shows an endTime for a long while.
        if objs:
            key = (total, len(objs))
            if key == stable:
                if stable_since and time.perf_counter() - stable_since > 120 and not h:
                    dur = stable_since - t0
                    return OutResult(True, round(dur, 1), "s3_polling", not_before, None,
                                     "queryHistory entry not found; time from S3 size stabilization")
            else:
                stable, stable_since = key, time.perf_counter()
    return OutResult(False, None, "client", error=f"timeout after {max_wait_seconds}s")
