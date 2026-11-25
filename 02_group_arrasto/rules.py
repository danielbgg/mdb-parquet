"""Simulated rating rules for the 02_group_arrasto step.

IMPORTANT: this is a SIMULATION inspired by the Res. CMN 2682 day-past-due
bands. It is NOT the customer's real rating engine; it only exists so the
"arrasto" (worst rating of a customer propagated to all its contracts) has a
measurable effect on the data.

Everything about the rating bands lives here and is consumed by
group_cluster.py (02a), group_adf.py (02b) and validate_grouped.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from generator.schema import COLUMN_ORDER  # noqa: E402

# (upper bound of qt_dias_atraso inclusive, rank, letter); last band is open-ended.
RATING_BANDS: tuple[tuple[int | None, int, str], ...] = (
    (14, 1, "A"),
    (30, 2, "B"),
    (60, 3, "C"),
    (90, 4, "D"),
    (120, 5, "E"),
    (150, 6, "F"),
    (180, 7, "G"),
    (None, 8, "H"),
)
LETTERS: list[str] = [letter for _, _, letter in RATING_BANDS]
RANK_TO_LETTER: dict[int, str] = {rank: letter for _, rank, letter in RATING_BANDS}

# Contract-level fields copied into the embedded array, in Parquet column order.
# co_cpf_cnpj and data_referencia are lifted to the customer document.
CONTRACT_FIELDS: tuple[str, ...] = tuple(c for c in COLUMN_ORDER if c not in ("co_cpf_cnpj", "data_referencia"))

GROUPED_COLLECTION_CLUSTER = "clientes_agrupados"
GROUPED_COLLECTION_ADF = "clientes_agrupados_adf"


def rank_expr(days_path: str) -> dict:
    """Aggregation expression: rank 1..8 from a qt_dias_atraso field path ('$qt_dias_atraso', '$$this.qt_dias_atraso')."""
    branches = [
        {"case": {"$lte": [days_path, upper]}, "then": rank}
        for upper, rank, _ in RATING_BANDS
        if upper is not None
    ]
    return {"$switch": {"branches": branches, "default": RATING_BANDS[-1][1]}}


def letter_expr(rank_path: str) -> dict:
    """Aggregation expression: rating letter from a rank field path."""
    return {"$arrayElemAt": [LETTERS, {"$subtract": [rank_path, 1]}]}


def rank_of(days: int) -> int:
    for upper, rank, _ in RATING_BANDS:
        if upper is None or days <= upper:
            return rank
    return RATING_BANDS[-1][1]


def grouping_pipeline(out_stage: dict | None = None) -> list[dict]:
    """contratos_flat-shaped input -> one document per customer (clientes_agrupados model).

    Field names and types inside `contratos` are the same as in the flat
    collection, so the export step can rebuild the original 15 columns with
    $unwind. rating_final is stored only at customer level.
    """
    contract_doc = {f: f"${f}" for f in CONTRACT_FIELDS}
    # Keep the key even when the flat document lacks it (Parquet null -> missing).
    contract_doc["vr_renegociado"] = {"$ifNull": ["$vr_renegociado", None]}
    contract_doc["rating_contrato"] = "$rating_contrato"
    pipeline: list[dict] = [
        {"$set": {"rank": rank_expr("$qt_dias_atraso")}},
        {"$set": {"rating_contrato": letter_expr("$rank")}},
        {
            "$group": {
                "_id": "$co_cpf_cnpj",
                "data_referencia": {"$first": "$data_referencia"},
                "qtd_contratos": {"$sum": 1},
                "pior_rank": {"$max": "$rank"},
                "contratos": {"$push": contract_doc},
            }
        },
        {
            # Fix top-level field order to match the target model (rating_final before contratos).
            "$replaceWith": {
                "_id": "$_id",
                "data_referencia": "$data_referencia",
                "qtd_contratos": "$qtd_contratos",
                "pior_rank": "$pior_rank",
                "rating_final": letter_expr("$pior_rank"),
                "contratos": "$contratos",
            }
        },
    ]
    if out_stage is not None:
        pipeline.append(out_stage)
    return pipeline
