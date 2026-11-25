#!/usr/bin/env python3
"""03c — PyMongoArrow + pyarrow.ParquetWriter: single Parquet file written by a client.

Reads clientes_agrupados on the Atlas cluster with the same pipeline as 03b
($unwind + $replaceWith -> 17 typed columns), converts each batch straight to
Arrow with PyMongoArrow and appends row groups to ONE local Parquet file,
then (optionally) uploads it to S3 with multipart.

Run it from an EC2 in the same region as the cluster (us-east-1): the whole
~10 GB of BSON crosses the network, so running from a laptop measures your
link, not the database.

    # on the EC2 (see 03_export/README.md for setup)
    python 03_export/export_pymongoarrow.py --out /data/ratings_export.parquet \
        --s3-uri s3://$S3_BUCKET/output/03c/$(date -u +%Y%m%dT%H%M%SZ)/ratings_export.1.parquet

Time measured client-side (perf_counter) around the read+write loop; the S3
upload is timed separately.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pymongoarrow.monkey import patch_all
from pymongoarrow.schema import Schema

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(HERE))

from common.bench import RESULTS_DIR, Bench, mongo_client  # noqa: E402
from common.config import load_config, load_manifest  # noqa: E402
from export_common import OUTPUT_COLUMNS, pipeline_grouped  # noqa: E402

patch_all()

# Arrow schema mirroring results/type_mapping.md (string for data_referencia /
# vr_renegociado as in the "mixed" input profile).
ARROW_TYPES = {
    "data_referencia": pa.string(),
    "chave_sequencial": pa.int64(),
    "co_cpf_cnpj": pa.string(),
    "contrato": pa.int64(),
    "qt_dias_atraso": pa.int32(),
    "saldo": pa.float64(),
    "saldo_limite": pa.float64(),
    "ctr_elegivel": pa.string(),
    "carteira": pa.string(),
    "cod_produto": pa.int32(),
    "flag_prejuizo_2682": pa.int32(),
    "flag_obito": pa.int32(),
    "qt_dias_atraso_acumulado": pa.int32(),
    "modalidade": pa.int32(),
    "vr_renegociado": pa.string(),
    "rating_contrato": pa.string(),
    "rating_final": pa.string(),
}
ARROW_SCHEMA = pa.schema([pa.field(c, ARROW_TYPES[c], nullable=(c == "vr_renegociado")) for c in OUTPUT_COLUMNS])
PMA_SCHEMA = Schema({c: ARROW_TYPES[c] for c in OUTPUT_COLUMNS})


def iter_batches(coll, pipeline: list[dict], batch_size: int):
    """Yield Arrow RecordBatches of `batch_size` rows from an aggregate cursor.

    PyMongoArrow's aggregate_arrow_all materializes everything; to stream we
    use the pymongo cursor with a bounded batch and convert each batch.
    """
    from pymongoarrow.context import PyMongoArrowContext

    cursor = coll.aggregate_raw_batches(pipeline, allowDiskUse=True, batchSize=batch_size)
    for raw in cursor:
        ctx = PyMongoArrowContext(PMA_SCHEMA, codec_options=coll.codec_options)
        ctx.process_bson_stream(raw)
        tbl = ctx.finish()
        yield tbl.cast(ARROW_SCHEMA)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="03c: PyMongoArrow -> single Parquet file.")
    p.add_argument("--source-coll", default="clientes_agrupados")
    p.add_argument("--out", default="data/output/03c/ratings_export.1.parquet")
    p.add_argument("--s3-uri", default=None, help="upload the file here after writing (s3://bucket/key)")
    p.add_argument("--batch-size", type=int, default=50_000, help="documents per cursor batch (rows per Arrow batch)")
    p.add_argument("--row-group-rows", type=int, default=1_048_576)
    p.add_argument("--compression", default="snappy")
    p.add_argument("--manifest", default="data/manifest.json")
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--notes", default="")
    p.add_argument("--config", default=None)
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    cfg.require("MONGODB_URI")
    # zstd needs backports.zstd (in requirements for the EC2 run); falls back silently otherwise
    cluster = mongo_client(cfg.mongodb_uri, compressors="zstd,zlib")
    manifest = load_manifest(args.manifest)
    pipeline = pipeline_grouped()
    ok_all = True

    for run in range(1, args.runs + 1):
        bench = Bench(scenario="03c_pymongoarrow", cfg=cfg, manifest=manifest, manifest_path=args.manifest, run=run,
                      runs=args.runs, source=f"{cfg.target_db}.{args.source_coll}", target=args.s3_uri or args.out,
                      notes=args.notes)
        bench.capture_environment(cluster)
        bench.details["pipeline"] = pipeline
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        rows = 0
        groups = 0
        t0 = time.perf_counter()
        print(f"[03c run {run}/{args.runs}] {cfg.target_db}.{args.source_coll} -> {out}")
        coll = cluster[cfg.target_db][args.source_coll]
        buffer: list[pa.Table] = []
        buffered = 0
        with pq.ParquetWriter(out, ARROW_SCHEMA, compression=args.compression, write_statistics=True) as writer:
            for tbl in iter_batches(coll, pipeline, args.batch_size):
                buffer.append(tbl)
                buffered += tbl.num_rows
                rows += tbl.num_rows
                if buffered >= args.row_group_rows:
                    writer.write_table(pa.concat_tables(buffer), row_group_size=args.row_group_rows)
                    groups += 1
                    buffer, buffered = [], 0
                    if groups % 5 == 0:
                        el = time.perf_counter() - t0
                        print(f"  {el:7.0f}s  {rows:,} rows  {rows / el:,.0f} rows/s  {groups} row groups", flush=True)
            if buffer:
                writer.write_table(pa.concat_tables(buffer), row_group_size=args.row_group_rows)
                groups += 1
        dt = time.perf_counter() - t0
        bench.timings["main"] = round(dt, 3)
        bench.timings["main_source"] = "client"
        size = out.stat().st_size
        bench.details.update({"rows": rows, "row_groups": groups, "size_bytes": size, "file": str(out)})
        print(f"[03c run {run}/{args.runs}] main: {dt:,.1f}s  rows={rows:,}  row_groups={groups}  size={size / 1e9:.3f} GB")
        ok = rows == manifest["rows"]

        if args.s3_uri:
            import boto3
            from boto3.s3.transfer import TransferConfig

            bucket, _, key = args.s3_uri[5:].partition("/")
            t1 = time.perf_counter()
            boto3.client("s3").upload_file(str(out), bucket, key,
                                           Config=TransferConfig(multipart_chunksize=64 * 1024 * 1024, max_concurrency=8))
            bench.timings["s3_upload"] = round(time.perf_counter() - t1, 3)
            print(f"  uploaded to {args.s3_uri} in {bench.timings['s3_upload']:,.1f}s")
        bench.save(ok, rows)
        ok_all &= ok
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
