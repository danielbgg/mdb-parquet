#!/usr/bin/env python3
"""Multipart upload to S3 with boto3 (fallback used by upload_s3.sh).

Credentials are resolved by boto3's default chain (env vars, AWS profile,
instance role). Nothing is hardcoded.
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig


class Progress:
    def __init__(self, total: int):
        self.total = total
        self.seen = 0
        self.lock = threading.Lock()
        self.start = time.perf_counter()

    def __call__(self, n: int) -> None:
        with self.lock:
            self.seen += n
            pct = 100.0 * self.seen / self.total if self.total else 100.0
            elapsed = time.perf_counter() - self.start
            rate = self.seen / elapsed / 1e6 if elapsed > 0 else 0.0
            sys.stdout.write(f"\r  {pct:6.2f}%  {self.seen / 1e6:,.1f} MB  {rate:,.1f} MB/s")
            sys.stdout.flush()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Upload a file to S3 using multipart upload.")
    p.add_argument("--file", default="data/ratings_input_synthetic.parquet")
    p.add_argument("--s3-uri", required=True, help="s3://bucket/key")
    p.add_argument("--profile", default=None)
    p.add_argument("--region", default=None)
    p.add_argument("--chunk-mb", type=int, default=64)
    p.add_argument("--concurrency", type=int, default=8)
    args = p.parse_args(argv)

    if not args.s3_uri.startswith("s3://"):
        print("--s3-uri must start with s3://", file=sys.stderr)
        return 2
    bucket, _, key = args.s3_uri[5:].partition("/")
    path = Path(args.file)
    if not key:
        key = path.name

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    size = path.stat().st_size
    config = TransferConfig(
        multipart_threshold=args.chunk_mb * 1024 * 1024,
        multipart_chunksize=args.chunk_mb * 1024 * 1024,
        max_concurrency=args.concurrency,
        use_threads=True,
    )
    print(f"Uploading {path} ({size:,} bytes) -> s3://{bucket}/{key}")
    t0 = time.perf_counter()
    s3.upload_file(str(path), bucket, key, Config=config, Callback=Progress(size))
    print(f"\nDone in {time.perf_counter() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
