# 01_load — carga plana S3 → Atlas via Data Federation

`load_flat.py` roda no Federated DB (`ADF_URI`) um `aggregate` com um único estágio `$out` para o cluster:

```js
db.s3import.aggregate(
  [{ $out: { atlas: { projectId: ATLAS_PROJECT_ID, clusterName: ATLAS_CLUSTER, db: TARGET_DB, coll: "contratos_flat" } } }],
  { allowDiskUse: true })
```

e em seguida valida `contratos_flat` no cluster (`MONGODB_URI`) contra `data/manifest.json`.

## Comandos

```bash
python 01_load/load_flat.py --check                                  # conectividade + coleções virtuais, não escreve nada
python 01_load/load_flat.py --source 1m --manifest data/sample_1m.manifest.json --drop-target   # fumaça (1M linhas)
python 01_load/load_flat.py --source single --drop-target --create-index   # 1 arquivo  -> contratos_flat (+ índice co_cpf_cnpj)
python 01_load/load_flat.py --source split  --drop-target                  # N arquivos -> contratos_flat
```

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--source single\|split\|1m` | `single` | coleção virtual `dbimport.s3import`, `s3import_split` ou `s3import_1m` |
| `--target` | `contratos_flat` (`contratos_flat_1m` para `1m`) | coleção de destino em `TARGET_DB` |
| `--runs N` | 1 | repete N vezes (cada execução gera um JSON em `results/` e uma linha em `results/summary.csv`) |
| `--drop-target` | off | apaga a coleção de destino antes de cada execução. Sem a flag, o script **recusa** escrever em coleção não vazia |
| `--type-sample` | 100000 | tamanho da amostra para o relatório de tipos |
| `--create-index` | off | após o `$out`, cria o índice `{co_cpf_cnpj: 1}` (chave do agrupamento da etapa 02). O tempo é medido **separadamente** (`timings.index_build`) por ser parte legítima do custo de "importação", sem contaminar o tempo do `$out` |
| `--check-distinct` | off | valida nº de `co_cpf_cnpj` distintos. Sem índice é um `$group` de 30M linhas com spill (>20 min numa M40); com `--create-index` vira `DISTINCT_SCAN` (segundos). Por padrão o check é pulado no 01 — a etapa 02 valida os 12M clientes via `countDocuments` da coleção agrupada |
| `--manifest` | `data/manifest.json` | manifest do gerador usado como referência |

## Medição do tempo

O `aggregate` com `$out` roda numa thread enquanto o script faz polling da coleção de destino a cada 10 s. O ADF escreve
numa coleção temporária e renomeia no final, então a contagem salta de 0 para o total num único passo — o instante desse
salto é o fim real da carga. Se a resposta do ADF chegar, o tempo é exato; se a conexão ociosa for descartada por algum
intermediário (observado em cargas > 5 min), o tempo é o do polling (±10 s) e fica marcado como estimado no JSON.

## Validação (no cluster)

- `countDocuments == rows` do manifest;
- `sum(saldo)` e `sum(saldo_limite)` via `$group` com `$toDecimal` (soma exata em Decimal128), tolerância 0,01;
- nº de `co_cpf_cnpj` distintos via `$group` + `$count` (não `distinct`, por causa do limite de 16 MB) `== customers`.

## Relatório de tipos — `results/type_mapping.md`

Amostra de 100k documentos (`$sample`) → `$type` de cada uma das 15 colunas → `$facet` com `$sortByCount` por coluna.
Informa também se `vr_renegociado` vazio chega como `null` ou campo ausente, o tipo do `_id` gerado e se a ordem dos
campos do documento é a mesma do header do Parquet. Esse relatório define o `$replaceWith` da etapa 03.

## Métricas gravadas

Wall time do `aggregate` (cliente), `$collStats` (storageSize, dataSize, totalIndexSize, avgObjSize), tier/região/versão,
seed e perfil do manifest — em `results/01_load_<source>_run<N>_<ts>.json` e `results/summary.csv`.
