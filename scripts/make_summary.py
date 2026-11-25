#!/usr/bin/env python3
"""Build results/summary.md from results/summary.csv and the per-run JSON files.

Sections: end-to-end table (01 + 02a + 03b vs the customer's reference),
per-scenario benchmark table, export table (files/size/row groups/time
source), caveats, free-form notes (results/summary_notes.md) and per-scenario
details of the latest run.
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
    "01_load_split": "01 carga plana — 8 arquivos (ADF $out)",
    "01_load_1m": "01 carga plana — fumaça 1M",
    "02a_group_cluster": "02a agrupamento + arrasto no cluster",
    "02a_group_cluster_1m": "02a agrupamento — fumaça 1M",
    "02b_group_adf_single": "02b carga agrupada direto do S3 (ADF) — 1 arquivo",
    "02b_group_adf_split": "02b carga agrupada direto do S3 (ADF) — 8 arquivos",
    "02b_group_adf_1m": "02b carga agrupada — fumaça 1M",
    "03_export_ensaio": "03 export — ensaio 1M (agrupado)",
    "03_export_03a": "03a export plano via ADF (15 colunas, 1 arquivo)",
    "03_export_03b": "03b export agrupado via ADF (17 colunas, 1 arquivo)",
    "03_export_03b_multi": "03b-multi export agrupado (maxFileSize padrão)",
    "03c_pymongoarrow": "03c PyMongoArrow + ParquetWriter (EC2)",
}
EXPORT_SCENARIOS = tuple(k for k in SCENARIO_LABELS if k.startswith("03"))

CUSTOMER_REFERENCE = {  # minutes, from the customer's current PostgreSQL pipeline
    "load": (50, 60),
    "arrasto": (50, 50),
    "export": (40, 40),
    "files": 200,
}


def fmt_secs(s: float | None) -> str:
    if s is None:
        return "-"
    m, sec = divmod(s, 60)
    return f"{s:,.1f}s ({int(m)}m{sec:02.0f}s)" if s >= 60 else f"{s:,.1f}s"


def fmt_min(s: float | None) -> str:
    return "-" if s is None else f"{s / 60:.1f} min"


def load_detail(row: dict) -> dict:
    f = RESULTS_DIR / row["result_file"]
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}


def time_source(detail: dict) -> str:
    t = detail.get("timings", {})
    src = t.get("main_source")
    if src == "queryHistory":
        return "exato ($queryHistory)"
    if src == "client":
        return "exato (cliente)"
    if t.get("main_is_estimate"):
        return "estimado (polling)"
    return "exato (cliente)"


def main() -> int:
    if not SUMMARY_CSV.exists():
        print("results/summary.csv not found; run a scenario first")
        return 1
    rows = list(csv.DictReader(SUMMARY_CSV.open(encoding="utf-8")))
    by_scenario: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        key = r["scenario"]
        # Smoke tests (1M manifest) share scenario names with the full runs; keep them apart.
        if int(float(r["manifest_rows"] or 0)) < 10_000_000 and not key.endswith(("_1m", "ensaio")):
            key += "_1m"
        key = key.replace("_1m_1m", "_1m")
        by_scenario[key].append(r)

    def median_of(key: str) -> float | None:
        ok = [float(r["seconds"]) for r in by_scenario.get(key, []) if r["ok"] == "True" and r["seconds"]]
        return statistics.median(ok) if ok else None

    def runs_of(key: str) -> int:
        return len([r for r in by_scenario.get(key, []) if r["ok"] == "True" and r["seconds"]])

    env = {}
    for key in ("01_load_single", "02a_group_cluster", "03_export_03b", *by_scenario):
        if key not in by_scenario:
            continue
        d = load_detail(by_scenario[key][-1])
        if d.get("environment"):
            env = d["environment"]
            break

    out = [
        "# Resumo dos benchmarks — POC Rating (MongoDB Atlas)",
        "",
        f"Gerado em {datetime.now(timezone.utc).isoformat(timespec='seconds')} a partir de `results/summary.csv` e `results/*.json`.",
        f"Ambiente: Atlas **{env.get('tier')}** ({env.get('cloud_region')}), MongoDB {env.get('mongo_version')}, "
        f"Data Federation {env.get('adf_version')}; dados sintéticos seed {env.get('manifest', {}).get('seed')}, perfil "
        f"`{env.get('manifest', {}).get('type_profile')}`, {env.get('manifest', {}).get('rows', 0):,} linhas / "
        f"{env.get('manifest', {}).get('customers', 0):,} clientes.",
        "",
    ]

    # --- end to end ---------------------------------------------------------
    t01 = median_of("01_load_single")
    idx = None
    d01 = load_detail(by_scenario["01_load_single"][-1]) if by_scenario.get("01_load_single") else {}
    idx = d01.get("timings", {}).get("index_build")
    t02 = median_of("02a_group_cluster")
    t03 = median_of("03_export_03b")
    total = sum(x for x in (t01, t02, t03) if x is not None) if all(x is not None for x in (t01, t02, t03)) else None
    d03 = load_detail(by_scenario["03_export_03b"][-1]) if by_scenario.get("03_export_03b") else {}
    files03 = len(d03.get("details", {}).get("files", [])) or "-"
    ref = CUSTOMER_REFERENCE
    ref_total_lo = ref["load"][0] + ref["arrasto"][0] + ref["export"][0]
    ref_total_hi = ref["load"][1] + ref["arrasto"][1] + ref["export"][1]
    out += [
        "## Ponta a ponta (para a apresentação)",
        "",
        "| Etapa | MongoDB Atlas (M40) | Referência do cliente (PostgreSQL) |",
        "|---|---|---|",
        f"| 1. Carga do Parquet (30 M linhas) | **{fmt_min(t01)}** ({fmt_secs(t01)}; mediana de {runs_of('01_load_single')} exec.) "
        f"+ índice `co_cpf_cnpj` {fmt_secs(idx)} à parte | ~{ref['load'][0]}–{ref['load'][1]} min (inclui regras por linha) |",
        f"| 2. Agrupamento por cliente + arrasto | **{fmt_min(t02)}** ({fmt_secs(t02)}; {runs_of('02a_group_cluster')} exec.) | ~{ref['arrasto'][0]} min |",
        f"| 3. Exportação para Parquet | **{fmt_min(t03)}** ({fmt_secs(t03)}; mediana de {runs_of('03_export_03b')} exec.) → **{files03} arquivo** | ~{ref['export'][0]} min → ~{ref['files']} arquivos |",
        f"| **Total** | **{fmt_min(total)}**{' (+ ' + fmt_secs(idx) + ' de índice)' if idx else ''} | **~2h20–3h** (cliente informa ~2h40 a 3h) |",
        "",
        "Alternativa 1+2 em uma única operação (02b, carga já agrupada direto do S3 via Data Federation): "
        f"**{fmt_min(median_of('02b_group_adf_single'))}** — mais simples operacionalmente, porém mais lenta que 01 + 02a.",
        "",
    ]

    # --- per-scenario table -------------------------------------------------------
    out += [
        "## Todos os cenários",
        "",
        "Tempo = mediana das execuções bem-sucedidas. Fonte do tempo: `$queryHistory` do ADF (início/fim medidos no servidor) "
        "ou cliente (`perf_counter`), conforme a coluna.",
        "",
        "| Cenário | Exec. | Mediana | Mín / Máx | Fonte do tempo | Docs/linhas | Validação | Storage / arquivo | Índices / row groups |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    detail_sections = []
    order = sorted(by_scenario, key=lambda s: list(SCENARIO_LABELS).index(s) if s in SCENARIO_LABELS else 99)
    for scenario in order:
        runs = by_scenario[scenario]
        ok_runs = [r for r in runs if r["ok"] == "True" and r["seconds"]]
        secs = [float(r["seconds"]) for r in ok_runs]
        latest = runs[-1]
        detail = load_detail(latest)
        v = detail.get("details", {}).get("validation", {})
        is_export = scenario.startswith("03")
        if is_export:
            n_ok = sum(1 for c in v.get("checks", []) if c["ok"] or c.get("warn_only"))
            validation = f"{'OK' if v.get('ok') else 'FALHOU'} ({n_ok}/{len(v.get('checks', []))})" if v else "-"
            size = human_bytes(v.get("size_bytes") or detail.get("details", {}).get("size_bytes"))
            nfiles = len(detail.get("details", {}).get("files", [])) or (1 if detail.get("details", {}).get("file") else "-")
            storage = f"{size} em {nfiles} arq."
            indexes = f"{v.get('row_groups') or detail.get('details', {}).get('row_groups', '-')} row groups"
        else:
            stats = v.get("coll_stats") or detail.get("details", {}).get("coll_stats") or {}
            n_ok = sum(1 for c in v.get("checks", []) if c["ok"])
            validation = f"{'OK' if v.get('ok') else 'FALHOU'} ({n_ok}/{len(v.get('checks', []))})" if v else "-"
            storage = human_bytes(stats.get("storage_size_bytes"))
            indexes = human_bytes(stats.get("total_index_size_bytes"))
        sources = {time_source(load_detail(r)) for r in ok_runs}
        out.append(
            f"| {SCENARIO_LABELS.get(scenario, scenario)} | {len(ok_runs)}/{len(runs)} | "
            f"**{fmt_secs(statistics.median(secs)) if secs else '-'}** | "
            f"{fmt_secs(min(secs)) if secs else '-'} / {fmt_secs(max(secs)) if secs else '-'} | {', '.join(sorted(sources)) or '-'} | "
            f"{int(float(latest['docs'])):,} | {validation} | {storage} | {indexes} |"
            if latest.get("docs") else
            f"| {SCENARIO_LABELS.get(scenario, scenario)} | {len(ok_runs)}/{len(runs)} | - | - | - | - | ERRO | - | - |"
        )

        section = [f"### {SCENARIO_LABELS.get(scenario, scenario)}", "", f"- Origem: `{latest['source']}` → destino: `{latest['target']}`"]
        section.append("- Tempos: " + ", ".join(
            f"run {r['run']}: {fmt_secs(float(r['seconds']))} [{time_source(load_detail(r))}]" for r in ok_runs))
        if detail.get("timings", {}).get("index_build"):
            section.append(f"- índice `co_cpf_cnpj`: {fmt_secs(detail['timings']['index_build'])}, "
                           f"{human_bytes(detail.get('details', {}).get('index', {}).get('size_bytes'))}")
        if detail.get("details", {}).get("error"):
            section.append(f"- ERRO: `{detail['details']['error']}`")
        if latest.get("notes"):
            section.append(f"- nota: {latest['notes']}")
        for c in v.get("checks", []):
            mark = "x" if c["ok"] else ("~" if c.get("warn_only") else " ")
            section.append(f"- [{mark}] {c['check']} — {c['detail'][:400]}")
        ex = detail.get("details", {}).get("explain_sample")
        if ex:
            g = ex.get("group_stage", {})
            section.append(f"- explain executionStats ({ex.get('sample_rows'):,} linhas): $group usedDisk={g.get('usedDisk')}, "
                           f"spills={g.get('spills')}, spilledBytes={g.get('spilledBytes')} (`{ex.get('file')}`)")
        if v.get("storage_ratio_grouped_vs_flat"):
            section.append(f"- storage agrupado / plano: {v['storage_ratio_grouped_vs_flat']}x")
        if v.get("pct_mixed_rank_differs") is not None:
            section.append(f"- clientes em que o arrasto altera algum contrato: {v.get('mixed_rank_differs'):,} ({v.get('pct_mixed_rank_differs')}%)")
        if v.get("max_doc_bytes"):
            section.append(f"- maior documento: {human_bytes(v['max_doc_bytes'])}; médio: {human_bytes(v.get('avg_doc_bytes'))}")
        if is_export and v.get("schema_table_md"):
            section += ["", "Schema entrada × saída:", "", v["schema_table_md"]]
            if v.get("schema_annotation_diff"):
                section.append("")
                section.append("Diferenças de anotação/nullability (não alteram os dados): " + "; ".join(v["schema_annotation_diff"]))
        detail_sections.append("\n".join(section))

    # --- export table ---------------------------------------------------------------
    export_rows = []
    for scenario in order:
        if not scenario.startswith("03"):
            continue
        for r in by_scenario[scenario]:
            d = load_detail(r)
            v = d.get("details", {}).get("validation", {})
            files = d.get("details", {}).get("files", [])
            export_rows.append(
                f"| {SCENARIO_LABELS.get(scenario, scenario)} | {r['run']} | {fmt_secs(float(r['seconds'])) if r['seconds'] else '-'} | "
                f"{time_source(d)} | {int(float(r['docs'])):,} | {len(files) or '-'} | "
                f"{human_bytes(sum(f['size_bytes'] for f in files)) if files else human_bytes(d.get('details', {}).get('size_bytes'))} | "
                f"{v.get('row_groups', '-')} | {v.get('compression', '-')} | "
                f"{'OK' if v.get('ok') else ('FALHOU' if v else '-')} |" if r.get("docs") else
                f"| {SCENARIO_LABELS.get(scenario, scenario)} | {r['run']} | - | - | - | - | - | - | - | ERRO |"
            )
    if export_rows:
        out += ["", "## Exportações (etapa 03) — cada execução", "",
                "| Cenário | Run | Duração | Fonte | Linhas | Arquivos | Tamanho | Row groups | Compressão | Validação |",
                "|---|---|---|---|---|---|---|---|---|---|"] + export_rows

    # --- caveats ---------------------------------------------------------------------
    out += [
        "",
        "## Ressalvas",
        "",
        "- **Dados sintéticos**: gerados por `generator/generate_input.py` (DuckDB, seed 42) a partir do header, de 3 linhas de amostra e do "
        "volume do arquivo real; distribuições de contratos por cliente, atraso e saldo são premissas, não dados do cliente.",
        "- **Arrasto simulado**: `rating_contrato`/`rating_final` vêm de faixas de dias de atraso inspiradas na Res. 2682 "
        "(`02_group_arrasto/rules.py`), não da regra real do cliente.",
        "- **Etapa 1 do cliente inclui regras de negócio por linha** que não foram reproduzidas aqui; o tempo de carga mede apenas "
        "Parquet (S3) → coleção, mais o índice informado à parte.",
        f"- **Infra**: cluster Atlas {env.get('tier')} em {env.get('cloud_region')}, Data Federation na mesma região, bucket S3 na mesma região. "
        "Outros tiers/regiões mudam os tempos.",
        "- **Origem dos tempos**: etapas via Data Federation (01, 02b, 03) usam `$queryHistory` (início/fim medidos no servidor; exato). "
        "02a (cluster) é o wall time do cliente em torno do `aggregate` (exato). As cargas 01/02b foram inicialmente medidas por polling "
        "(±10 s) e depois reconciliadas com o `$queryHistory` (`scripts/reconcile_query_history.py`); a diferença ficou entre 2 e 8 s.",
        "- **Schema de saída**: tipos físicos/lógicos idênticos aos da entrada; diferem apenas anotações (ConvertedType legado vs "
        "LogicalType) e nullability (o ADF marca todas as colunas como OPTIONAL). Ver diff em cada cenário 03.",
        "- Validações do export usam o arquivo baixado para a máquina local; a comparação linha a linha compara "
        "`(chave_sequencial, hash das 15 colunas)` nos dois sentidos contra o Parquet de entrada.",
    ]

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
