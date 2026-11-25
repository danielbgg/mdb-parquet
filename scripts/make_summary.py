#!/usr/bin/env python3
"""Build results/summary.md from results/summary.csv and the per-run JSON files.

For each scenario: median wall time (over successful runs), run count,
document count, validation outcome and storage size of the target collection
(taken from the most recent run).
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.bench import RESULTS_DIR, SUMMARY_CSV, human_bytes  # noqa: E402

SCENARIO_LABELS = {
    "01_load_single": "01 carga plana — 1 arquivo (ADF $out)",
    "01_load_split": "01 carga plana — N arquivos (ADF $out)",
    "01_load_1m": "01 carga plana — fumaça 1M",
    "01_load_1m_1m": "01 carga plana — fumaça 1M",
    "02b_group_adf_1m_1m": "02b carga agrupada — fumaça 1M",
    "02a_group_cluster": "02a agrupamento + arrasto no cluster",
    "02a_group_cluster_1m": "02a agrupamento — fumaça 1M",
    "02b_group_adf_single": "02b carga agrupada direto do S3 (ADF) — 1 arquivo",
    "02b_group_adf_split": "02b carga agrupada direto do S3 (ADF) — N arquivos",
    "02b_group_adf_1m": "02b carga agrupada — fumaça 1M",
}


def fmt_secs(s: float | None) -> str:
    if s is None:
        return "-"
    m, sec = divmod(s, 60)
    return f"{s:,.1f}s ({int(m)}m{sec:02.0f}s)" if s >= 60 else f"{s:,.1f}s"


def main() -> int:
    if not SUMMARY_CSV.exists():
        print("results/summary.csv not found; run a scenario first")
        return 1
    rows = list(csv.DictReader(SUMMARY_CSV.open(encoding="utf-8")))
    # Smoke tests (1M manifest) share scenario names with the full runs; keep them apart.
    by_scenario: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        key = r["scenario"]
        if int(float(r["manifest_rows"] or 0)) < 10_000_000 and not key.endswith("_1m"):
            key += "_1m"
        by_scenario[key].append(r)

    out = [
        "# Resumo dos benchmarks",
        "",
        f"Gerado em {datetime.now(timezone.utc).isoformat(timespec='seconds')} a partir de `results/summary.csv`.",
        "Tempo = mediana das execuções bem-sucedidas (wall time do `aggregate` com `$out`, medido pelo cliente).",
        "",
    ]
    env_line = None
    detail_sections = []
    out += [
        "| Cenário | Execuções | Mediana | Mín / Máx | Docs | Validação | Storage size | Index size | Avg obj |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    order = sorted(by_scenario, key=lambda s: list(SCENARIO_LABELS).index(s) if s in SCENARIO_LABELS else 99)
    for scenario in order:
        runs = by_scenario[scenario]
        ok_runs = [r for r in runs if r["ok"] == "True" and r["seconds"]]
        secs = [float(r["seconds"]) for r in ok_runs]
        latest = runs[-1]
        detail = {}
        f = RESULTS_DIR / latest["result_file"]
        if f.exists():
            detail = json.loads(f.read_text(encoding="utf-8"))
        if env_line is None and detail.get("environment"):
            e = detail["environment"]
            env_line = f"Ambiente: Atlas {e.get('tier')} ({e.get('cloud_region')}), MongoDB {e.get('mongo_version')}, ADF {e.get('adf_version')}; manifest seed {e['manifest'].get('seed')}, perfil {e['manifest'].get('type_profile')}."
        v = detail.get("details", {}).get("validation", {})
        stats = v.get("coll_stats") or detail.get("details", {}).get("coll_stats") or {}
        validation = "-"
        if v:
            n_ok = sum(1 for c in v.get("checks", []) if c["ok"])
            validation = f"{'OK' if v.get('ok') else 'FALHOU'} ({n_ok}/{len(v.get('checks', []))})"
        out.append(
            f"| {SCENARIO_LABELS.get(scenario, scenario)} | {len(ok_runs)}/{len(runs)} | "
            f"**{fmt_secs(statistics.median(secs)) if secs else '-'}** | "
            f"{fmt_secs(min(secs)) if secs else '-'} / {fmt_secs(max(secs)) if secs else '-'} | "
            f"{int(float(latest['docs'])):,} | {validation} | {human_bytes(stats.get('storage_size_bytes'))} | "
            f"{human_bytes(stats.get('total_index_size_bytes'))} | {human_bytes(stats.get('avg_obj_size_bytes'))} |"
            if latest.get("docs") else
            f"| {SCENARIO_LABELS.get(scenario, scenario)} | {len(ok_runs)}/{len(runs)} | - | - | - | ERRO | - | - | - |"
        )
        section = [f"### {SCENARIO_LABELS.get(scenario, scenario)}", "", f"- Origem: `{latest['source']}` → destino: `{latest['target']}`"]
        section.append("- Tempos: " + ", ".join(f"run {r['run']}: {fmt_secs(float(r['seconds']))}" for r in ok_runs))
        if detail.get("details", {}).get("error"):
            section.append(f"- ERRO: `{detail['details']['error']}`")
        for c in v.get("checks", []):
            section.append(f"- [{'x' if c['ok'] else ' '}] {c['check']} — {c['detail']}")
        ex = detail.get("details", {}).get("explain_sample")
        if ex:
            g = ex.get("group_stage", {})
            section.append(
                f"- explain executionStats ({ex.get('sample_rows'):,} linhas): $group usedDisk={g.get('usedDisk')}, "
                f"spills={g.get('spills')}, spilledBytes={g.get('spilledBytes')}, maxUsedMemBytes={g.get('maxUsedMemBytes')} (`{ex.get('file')}`)"
            )
        if v.get("storage_ratio_grouped_vs_flat"):
            section.append(f"- storage agrupado / plano: {v['storage_ratio_grouped_vs_flat']}x")
        if v.get("pct_mixed_rank_differs") is not None:
            section.append(f"- clientes em que o arrasto altera algum contrato (min rank != max rank): {v.get('mixed_rank_differs'):,} ({v.get('pct_mixed_rank_differs')}%)")
        if v.get("max_doc_bytes"):
            section.append(f"- maior documento: {human_bytes(v['max_doc_bytes'])}; médio: {human_bytes(v.get('avg_doc_bytes'))}")
        detail_sections.append("\n".join(section))

    if env_line:
        out.insert(4, env_line)
    notes_file = RESULTS_DIR / "summary_notes.md"
    if notes_file.exists():
        out += ["", notes_file.read_text(encoding="utf-8").rstrip()]
    out += ["", "## Detalhes por cenário (última execução)", ""] + ["\n".join([s, ""]) for s in detail_sections]
    path = RESULTS_DIR / "summary.md"
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"-> {path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
