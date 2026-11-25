"""Benchmark helpers shared by the 01/02/03 steps.

A `Bench` wraps one scenario execution: it measures wall time with
time.perf_counter, captures the environment (tier, region, MongoDB version,
manifest seed/profile), writes results/<scenario>_run<N>_<ts>.json and appends
a row to results/summary.csv.
"""

from __future__ import annotations

import csv
import json
import platform
import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ExecutionTimeout, NetworkTimeout, PyMongoError

from common.config import REPO_ROOT, Config, redact_uri, rel

RESULTS_DIR = REPO_ROOT / "results"
SUMMARY_CSV = RESULTS_DIR / "summary.csv"
SUMMARY_COLUMNS = [
    "timestamp",
    "scenario",
    "run",
    "runs",
    "seconds",
    "ok",
    "docs",
    "source",
    "target",
    "tier",
    "region",
    "mongo_version",
    "seed",
    "type_profile",
    "manifest_rows",
    "manifest",
    "result_file",
    "notes",
]


# A $out of 30M rows can take tens of minutes; still, never wait forever —
# a suspended laptop/NAT drop would otherwise hang the client indefinitely.
DEFAULT_SOCKET_TIMEOUT_MS = 60 * 60 * 1000


def mongo_client(uri: str, **kwargs: Any) -> MongoClient:
    """Client with timeouts suited to long-running $out aggregations."""
    defaults = dict(
        serverSelectionTimeoutMS=30_000,
        connectTimeoutMS=30_000,
        socketTimeoutMS=DEFAULT_SOCKET_TIMEOUT_MS,
        heartbeatFrequencyMS=10_000,
        appname="ratings-poc",
    )
    defaults.update(kwargs)
    return MongoClient(uri, **defaults)


def server_version(client: MongoClient) -> str:
    try:
        return str(client.server_info().get("version", "?"))
    except Exception as exc:  # ADF may not expose the same fields
        return f"? ({type(exc).__name__})"


def coll_stats(client: MongoClient, db: str, coll: str) -> dict:
    """storageSize / indexes / avgObjSize via $collStats (works on Atlas clusters)."""
    try:
        doc = next(client[db][coll].aggregate([{"$collStats": {"storageStats": {}}}]))
    except StopIteration:
        return {}
    except Exception as exc:
        return {"error": str(exc)}
    s = doc.get("storageStats", {})
    return {
        "count": s.get("count"),
        "size_bytes": s.get("size"),
        "storage_size_bytes": s.get("storageSize"),
        "total_index_size_bytes": s.get("totalIndexSize"),
        "index_sizes": s.get("indexSizes"),
        "avg_obj_size_bytes": s.get("avgObjSize"),
        "nindexes": s.get("nindexes"),
    }


def human_bytes(n: float | int | None) -> str:
    if n is None:
        return "-"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


@dataclass
class Bench:
    scenario: str
    cfg: Config
    manifest: dict
    manifest_path: str
    run: int = 1
    runs: int = 1
    source: str = ""
    target: str = ""
    notes: str = ""
    details: dict = field(default_factory=dict)
    timings: dict = field(default_factory=dict)
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))
    environment: dict = field(default_factory=dict)

    def capture_environment(self, cluster: MongoClient | None, adf: MongoClient | None = None) -> None:
        self.environment = {
            "tier": self.cfg.atlas_tier,
            "cloud_region": self.cfg.aws_region,
            "cluster_name": self.cfg.atlas_cluster,
            "mongo_version": server_version(cluster) if cluster else None,
            "adf_version": server_version(adf) if adf else None,
            "mongodb_uri": redact_uri(self.cfg.mongodb_uri),
            "adf_uri": redact_uri(self.cfg.adf_uri),
            "client_host": socket.gethostname(),
            "client_platform": platform.platform(),
            "manifest": {
                "path": self.manifest_path,
                "seed": self.manifest.get("seed"),
                "type_profile": self.manifest.get("type_profile"),
                "rows": self.manifest.get("rows"),
                "customers": self.manifest.get("customers"),
            },
        }

    @contextmanager
    def timed(self, label: str):
        t0 = time.perf_counter()
        print(f"[{self.scenario} run {self.run}/{self.runs}] {label} ...", flush=True)
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self.timings[label] = round(dt, 3)
            print(f"[{self.scenario} run {self.run}/{self.runs}] {label}: {dt:,.1f}s", flush=True)

    @property
    def main_seconds(self) -> float | None:
        return self.timings.get("main")

    def run_out(self, aggregate, cluster: MongoClient, db: str, coll: str, expected_docs: int,
                poll_seconds: int = 10, grace_seconds: int = 60, max_wait_seconds: int = 4 * 3600) -> bool:
        """Execute `aggregate()` (a $out pipeline) under the 'main' timer.

        The aggregate runs in a background thread while the main thread polls
        the target collection. Completion is whichever comes first:
          - the server answers the aggregate (exact wall time), or
          - the target reaches `expected_docs`, stays stable and the answer
            still has not arrived after `grace_seconds` (observed with ADF:
            the idle client connection is dropped by an intermediary while the
            $out keeps running server-side). The time is then the moment the
            count was first observed complete (precision = poll interval) and
            is flagged as an estimate.
        """
        result: dict = {}

        def worker():
            try:
                aggregate()
                result["ok"] = True
            except Exception as exc:  # surfaced in the main thread
                result["exc"] = exc
            result["t"] = time.perf_counter()

        t0 = time.perf_counter()
        print(f"[{self.scenario} run {self.run}/{self.runs}] main ...", flush=True)
        th = threading.Thread(target=worker, daemon=True)
        th.start()

        complete_at: float | None = None
        last = -1
        next_log = t0
        progress: list[tuple[float, int]] = []
        while True:
            th.join(timeout=poll_seconds)
            now = time.perf_counter()
            if "t" in result:
                break
            try:
                n = cluster[db][coll].estimated_document_count()
            except PyMongoError as exc:
                print(f"  poll error: {exc}", flush=True)
                continue
            progress.append((round(now - t0, 1), n))
            if now >= next_log:
                print(f"  {now - t0:7.0f}s  target docs: {n:,}/{expected_docs:,}", flush=True)
                next_log = now + 60
            if n >= expected_docs and n == last:
                complete_at = complete_at or now
                if now - complete_at >= grace_seconds:
                    break
            else:
                complete_at = None
            last = n
            if now - t0 > max_wait_seconds:
                self.details["error"] = f"timeout after {max_wait_seconds}s; target at {n:,}/{expected_docs:,}"
                return False

        self.details["progress"] = progress
        if "ok" in result:
            dt = result["t"] - t0
            self.timings["main"] = round(dt, 3)
            print(f"[{self.scenario} run {self.run}/{self.runs}] main: {dt:,.1f}s", flush=True)
            return True
        if "exc" in result and not isinstance(result["exc"], (AutoReconnect, NetworkTimeout, ExecutionTimeout)):
            self.details["error"] = f"{type(result['exc']).__name__}: {result['exc']}"
            print(f"  ERROR: {self.details['error']}", flush=True)
            return False

        # Count-based completion (first poll that observed the full count).
        first_full = next((t for t, n in progress if n >= expected_docs), None)
        dt = first_full if first_full is not None else time.perf_counter() - t0
        self.timings["main"] = round(dt, 3)
        self.timings["main_is_estimate"] = True
        self.timings["main_poll_interval_s"] = poll_seconds
        reason = f"{type(result['exc']).__name__}" if "exc" in result else "sem resposta do servidor"
        self.notes = (self.notes + "; " if self.notes else "") + f"tempo por polling da coleção destino (±{poll_seconds}s; {reason})"
        if "exc" in result:
            self.details["connection_error"] = str(result["exc"])
        print(f"[{self.scenario} run {self.run}/{self.runs}] main: ~{dt:,.1f}s (count reached {expected_docs:,}; {reason})", flush=True)
        return True

    def save(self, ok: bool, docs: int | None = None) -> Path:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = RESULTS_DIR / f"{self.scenario}_run{self.run}_{ts}.json"
        record = {
            "scenario": self.scenario,
            "run": self.run,
            "runs": self.runs,
            "started_at": self.started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": self.main_seconds,
            "timings": self.timings,
            "ok": ok,
            "docs": docs,
            "source": self.source,
            "target": self.target,
            "notes": self.notes,
            "environment": self.environment,
            "details": self.details,
        }
        out.write_text(json.dumps(record, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")

        new_file = not SUMMARY_CSV.exists()
        with SUMMARY_CSV.open("a", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=SUMMARY_COLUMNS)
            if new_file:
                w.writeheader()
            w.writerow(
                {
                    "timestamp": self.started_at,
                    "scenario": self.scenario,
                    "run": self.run,
                    "runs": self.runs,
                    "seconds": self.main_seconds,
                    "ok": ok,
                    "docs": docs,
                    "source": self.source,
                    "target": self.target,
                    "tier": self.environment.get("tier"),
                    "region": self.environment.get("cloud_region"),
                    "mongo_version": self.environment.get("mongo_version"),
                    "seed": self.manifest.get("seed"),
                    "type_profile": self.manifest.get("type_profile"),
                    "manifest_rows": self.manifest.get("rows"),
                    "manifest": self.manifest_path,
                    "result_file": out.name,
                    "notes": self.notes,
                }
            )
        print(f"[{self.scenario} run {self.run}/{self.runs}] saved {rel(out)} (+ summary.csv)")
        return out


def ensure_target(client: MongoClient, db: str, coll: str, drop: bool) -> None:
    """Drop the target when requested; refuse to append to a non-empty target otherwise."""
    if drop:
        client[db].drop_collection(coll)
        print(f"  dropped {db}.{coll}")
        return
    if coll in client[db].list_collection_names() and client[db][coll].estimated_document_count() > 0:
        raise SystemExit(
            f"{db}.{coll} already has documents; re-run with --drop-target (checksums would be corrupted by appending)."
        )
