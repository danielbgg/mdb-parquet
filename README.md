# ratings-poc — POC de performance MongoDB Atlas (motor de Rating)

POC para demonstrar o MongoDB Atlas exportando ~30 milhões de contratos para **um único arquivo Parquet** em poucos
minutos (Atlas Data Federation `$out` para S3 e, como alternativa, PyMongoArrow + `pyarrow.ParquetWriter`).

Etapas:

| Etapa | Conteúdo | Status |
|---|---|---|
| `generator/` | gerador do Parquet sintético de entrada (fiel ao arquivo do cliente), validação, upload S3 | pronto |
| `00_setup/` | documentação e templates da infra (Federated DB, IAM role, storage config) | pronto |
| `01_load/` | carga plana S3 → Atlas via Data Federation `$out` + relatório de tipos | pronto |
| `02_group_arrasto/` | modelo agrupado por cliente + arrasto simulado (02a no cluster, 02b direto do S3) | pronto |
| `03_export/` | exportação de 30M linhas para 1 arquivo Parquet | próximo prompt |

## Estrutura

```
README.md
requirements.txt
config.env.example        # MONGODB_URI, ADF_URI, ATLAS_PROJECT_ID, ATLAS_CLUSTER, S3_BUCKET, AWS_REGION, TARGET_DB
common/
  config.py               # leitura de config.env
  bench.py                # benchmark: wall time, ambiente, results/*.json + results/summary.csv, $collStats
generator/
  schema.py               # definição única de colunas, tipos, perfis, catálogo de produtos e distribuições
  generate_input.py       # gerador (DuckDB)
scripts/
  validate_input.py       # validação (DuckDB) + execução de sql/perfil_parquet.sql
  upload_s3.sh            # upload multipart (AWS CLI; fallback boto3 em upload_s3.py)
  upload_s3.py
  make_summary.py         # results/summary.csv + results/*.json -> results/summary.md
sql/
  perfil_parquet.sql      # script de perfil (o mesmo que o cliente roda sobre o arquivo real)
00_setup/
  README.md               # passo a passo: IAM role, Federated DB, storage config
  adf_storage_config.json # template de storage config (stores s3/atlas, dbimport.s3import[_split|_1m])
  render_storage_config.py
01_load/
  load_flat.py            # ADF: dbimport.s3import --$out--> cluster.contratos_flat + validação + type_mapping.md
02_group_arrasto/
  rules.py                # faixas de rating (simulação) e pipeline de agrupamento — fonte única
  group_cluster.py        # 02a: contratos_flat -> clientes_agrupados (no cluster)
  group_adf.py            # 02b: S3 -> clientes_agrupados_adf (no ADF, etapas 1+2 juntas)
  validate_grouped.py     # validação do modelo agrupado + documento de exemplo
03_export/                # placeholder
results/                  # perfil_sintetico.txt, calibracao.txt, type_mapping.md, summary.csv, summary.md, *.json
data/                     # arquivos gerados (ignorado pelo git)
```

## Setup

Requer Python 3.11+. Exemplo com [uv](https://docs.astral.sh/uv/):

```bash
uv python install 3.12
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt
source .venv/bin/activate
cp config.env.example config.env   # preencher (ver 00_setup/README.md); nunca commitar
```

## Etapas 00–02 (Atlas)

Pré-requisito: infra do [00_setup/README.md](00_setup/README.md) e `config.env` preenchido. Todos os scripts aceitam
`--runs N` (repetir; use 3 e a mediana), `--drop-target` (recriar a coleção de destino a cada execução) e `--manifest`.

> As cargas completas levam dezenas de minutos e o cliente fica bloqueado aguardando o `$out`. Em notebooks, rode com
> `caffeinate -i python ...` (macOS) para impedir o sleep — se a conexão cair, o `$out` continua no servidor e o script
> passa a medir por polling da coleção de destino, marcando o tempo como *estimado* em `results/`.

```bash
python 00_setup/render_storage_config.py                       # storage config com os valores do config.env (não aplica)
python 01_load/load_flat.py --check                            # conectividade e coleções virtuais

# teste de fumaça com 1M linhas (input_1m/ no bucket)
python 01_load/load_flat.py --source 1m --manifest data/sample_1m.manifest.json --drop-target
python 02_group_arrasto/group_cluster.py --source-coll contratos_flat_1m --target clientes_agrupados_1m --manifest data/sample_1m.manifest.json --drop-target
python 02_group_arrasto/group_adf.py --source 1m --target clientes_agrupados_adf_1m --flat-coll contratos_flat_1m --manifest data/sample_1m.manifest.json --drop-target

# completo (30.076.105 linhas)
python 01_load/load_flat.py --source single --drop-target --create-index   # 01 single (+ índice co_cpf_cnpj)
python 01_load/load_flat.py --source split  --drop-target                  # 01 split (input_split/)
python 02_group_arrasto/group_cluster.py --drop-target                     # 02a (--runs 3 para mediana)
python 02_group_arrasto/group_adf.py --drop-target                         # 02b
python scripts/make_summary.py                                              # results/summary.md
```

Detalhes em [01_load/README.md](01_load/README.md) e [02_group_arrasto/README.md](02_group_arrasto/README.md).
Resultados: `results/summary.md` (tabela final + observações), `results/type_mapping.md` (tipos BSON por coluna),
`results/exemplo_cliente_misto_clientes_agrupados.json`, `results/explain_02a_*.json`.

### Resultados (Atlas M40 us-east-1, MongoDB 9.0.7, 1 execução cada)

| Cenário | Tempo | Validação |
|---|---|---|
| 01 carga plana, 1 arquivo (ADF `$out`) | **~375 s** (+ 68 s índice `co_cpf_cnpj`) | 4/4 |
| 01 carga plana, N arquivos | **~375 s** (igual: gargalo é a escrita no cluster) | 3/3 |
| 02a agrupamento + arrasto no cluster | **330 s** | 12/12 |
| 02b carga agrupada direto do S3 (ADF) | **~902 s** | 12/12 |
| 01 + 02a | **~706 s ≈ 11,8 min** (vs ~100 min no Postgres atual) | |

Coleções: `contratos_flat` 30.076.105 docs, 2,2 GB storage; `clientes_agrupados` 12.034.569 docs, 2,1 GB, maior doc
8,7 KB. Ver `results/summary.md` para as ressalvas de medição (tempos do ADF medidos por polling, ±10 s).

## Gerador — comandos

```bash
# 1) Calibrar: gera ~2M linhas por perfil de tipo, mede bytes/linha e recomenda o perfil mais próximo de 1,10 GB
python generator/generate_input.py --calibrate

# 2) Gerar amostra de 1M linhas e validar
python generator/generate_input.py --rows 1000000 --out data/sample_1m.parquet --manifest data/sample_1m.manifest.json
python scripts/validate_input.py --file data/sample_1m.parquet --manifest data/sample_1m.manifest.json \
       --profile-out results/perfil_amostra_1m.txt

# 3) Gerar o arquivo completo (30.076.105 linhas) e validar
python generator/generate_input.py
python scripts/validate_input.py            # grava results/perfil_sintetico.txt

# 4) Upload para S3 (credenciais do ambiente ou do profile AWS; nada hardcoded)
scripts/upload_s3.sh --s3-uri s3://$S3_BUCKET/ratings-poc/ratings_input_synthetic.parquet [--profile <aws-profile>]
```

### Parâmetros do gerador

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--rows` | 30076105 | Total de linhas (contratos) |
| `--avg-contracts-per-customer` | 2.5 | Média de contratos por cliente; a distribuição é reescalada para esta média |
| `--contracts-dist` | ver `schema.py` | JSON `[[lo, hi, prob], ...]`; ex.: `'[[1,1,0.5],[2,2,0.22],[3,3,0.11],[4,4,0.06],[5,5,0.04],[6,10,0.05],[11,30,0.02]]'` |
| `--risk-mix` | `{"clean":0.7,"mixed":0.15,"default":0.15}` | Perfil de risco por cliente |
| `--type-profile` | `mixed` | `typed`, `strings`, `mixed` ou `real` |
| `--decimal` | off | `saldo`, `saldo_limite`, `vr_renegociado` como `DECIMAL(18,2)` em vez de `DOUBLE` |
| `--compression` | `snappy` | `snappy`, `zstd`, `gzip`, `lz4`, `uncompressed` |
| `--row-group-size` | 1048576 | Linhas por row group (padrão do pyarrow; DuckDB usa 122880) |
| `--order` | `random` | `random` (embaralhado) ou `by_customer` |
| `--data-referencia` | `2026-10-01 00:00:00` | Valor constante da coluna |
| `--seed` | 42 | Semente; a saída é bit a bit determinística para a mesma semente |
| `--out` / `--manifest` | `data/ratings_input_synthetic.parquet` / `data/manifest.json` | Saídas |
| `--split N` | — | Grava também N arquivos contíguos em `data/split/` (teste de carga paralela) |
| `--calibrate` / `--calibrate-rows` | — / 2000000 | Modo de calibração |
| `--memory-limit` / `--threads` / `--temp-dir` | `8GB` / todos / `data/.duckdb_tmp` | Recursos do DuckDB (8 GB mantém uma máquina de 16 GB segura) |

## Como o arquivo é gerado

**Por que DuckDB** (e não numpy + `pyarrow.ParquetWriter` em chunks): todo o pipeline — dimensionar clientes, soma
acumulada de offsets, expansão em contratos, embaralhamento e escrita do Parquet — é um único plano SQL vetorizado,
multi-thread e *out-of-core* (DuckDB derrama para disco se faltar memória). Não há loop de chunks nem gestão de memória
em Python, e o mesmo motor é reutilizado na validação e no perfil. A geração completa levou ~12 s com pico de ~4,5 GB.

**Determinismo**: nenhuma função `random()` é usada. Todo valor aleatório é derivado de `hash(chave_inteira)` do
DuckDB (`row_idx` ou `customer_id` + salt + seed), convertido para uniforme [0,1) e, quando preciso, para normal via
Box-Muller. Isso é independente do número de threads e da ordem de execução.

**Cliente primeiro, depois contratos** (`generator/generate_input.py`):

1. `customers`: para cada cliente sorteia-se `n_contracts` (distribuição configurável, reescalada para a média alvo
   ajustando a massa dos buckets ≥ 2 contratos), o perfil de risco, o documento e `flag_obito`. A soma acumulada de
   `n_contracts` define o offset de cada cliente; gera-se ~2% de clientes a mais e corta-se exatamente em `--rows`.
2. Expansão com `unnest(range(n_contracts))` → uma linha por contrato (`row_idx` = offset + k + 1).
3. Colunas por linha derivadas de `row_idx`; `COPY ... TO` Parquet em uma passada.

**Perfil de risco** (por cliente): `clean` — todos os contratos com 0 dias; `default` — todos com atraso (cauda longa
lognormal, mediana 300 d, limitada a 3650); `mixed` — só para clientes com 2+ contratos: o 1º contrato tem > 90 dias,
o 2º tem 0, os demais 50/50. A fatia `mixed` é aplicada apenas aos clientes multi-contrato (≈30% deles, garantindo o
mínimo de 15%) de modo que o total global fique em 15%.

**Regras por coluna**: ver docstrings em `schema.py`. Destaques:
- `chave_sequencial`: `row_number()` sobre um hash de `row_idx` → permutação de 1..rows independente da ordem do arquivo.
- `contrato`: permutação afim de `row_idx` dividida em 3 segmentos de magnitude (7–8, 9–10 e 11–12 dígitos, 30/40/30%);
  único por construção.
- `co_cpf_cnpj`: um por cliente, 92% CPF / 8% CNPJ; o número-base vem de uma bijeção de `customer_id` (sem colisões) e o
  **1º dígito verificador é deslocado em 1..9 posições**, garantindo que nenhum documento seja válido.
- `cod_produto` → `modalidade`, `carteira` e "tem limite" são fixos por produto (catálogo de 40 produtos em `schema.py`,
  com 8211/203/C5 da amostra). Produtos com limite somam ~20% das linhas → `saldo_limite > 0` em ~20%.
- `flag_obito` é atributo do cliente; `flag_prejuizo_2682` ≈ 85% quando `qt_dias_atraso > 360`, ≈ 0,1% caso contrário.

## Perfis de tipo (`generator/schema.py`)

| Coluna | `typed` | `strings` | `mixed` (padrão) |
|---|---|---|---|
| data_referencia | TIMESTAMP | VARCHAR `dd/MM/yyyy HH:mm:ss` | VARCHAR `dd/MM/yyyy HH:mm:ss` |
| chave_sequencial, contrato | BIGINT | VARCHAR | BIGINT |
| co_cpf_cnpj | BIGINT | VARCHAR (zeros à esquerda) | VARCHAR (zeros à esquerda) |
| qt_dias_atraso, *_acumulado, cod_produto, modalidade, flags | INTEGER | VARCHAR | INTEGER |
| saldo, saldo_limite | DOUBLE (`--decimal`: DECIMAL(18,2)) | VARCHAR `1234.56` | DOUBLE (`--decimal`: DECIMAL(18,2)) |
| ctr_elegivel, carteira | VARCHAR | VARCHAR | VARCHAR |
| vr_renegociado | DOUBLE, NULL em ~95% | VARCHAR, vazio = `' '` | VARCHAR, NULL em ~95% |

`real` é um placeholder (hoje igual a `mixed`): quando o cliente devolver `perfil_parquet.sql` do arquivo verdadeiro,
preencha `PROFILE_REAL` em `schema.py` (tipos por coluna e `empty_vr_renegociado`) e, se necessário, ajuste
`DEFAULT_CONTRACTS_DIST`, `PRODUCTS` e as constantes de distribuição. O gerador e a validação não precisam mudar.

## Calibração e tamanho

Referência do cliente: 30.076.105 linhas, ~1,10 GB → ~36,6 bytes/linha. Resultado de `--calibrate`
(`results/calibracao.txt`):

| perfil | bytes/linha | projeção 30M | vs alvo |
|---|---|---|---|
| typed | 31,74 | 0,955 GB | −13,2% |
| strings | 40,86 | 1,229 GB | +11,7% |
| **mixed** | **35,19** | **1,058 GB** | **−3,8%** (dentro de ±5%) |

Na calibração a `chave_sequencial` é escalada para a magnitude de `--rows`, porque é a única coluna cujo tamanho
cresce com N (bit-packing de 1..N); sem isso a projeção a partir de 2M linhas subestimava ~0,6 byte/linha.
O arquivo completo real ficou em **35,22 bytes/linha (1,059 GB, −3,7%)**, confirmando a projeção.

## Saídas

- `data/ratings_input_synthetic.parquet`
- `data/manifest.json`: seed, perfil, linhas, clientes, bytes, row groups, tempo, schema Parquet, histograma de
  contratos por cliente, % de clientes mistos, contagens por carteira e checksums `sum(saldo)` / `sum(saldo_limite)`
  (somados como `DECIMAL(18,2)`, exatos) — base de comparação para as etapas 01–03.
- `results/perfil_sintetico.txt`: saída de `sql/perfil_parquet.sql` sobre o arquivo sintético, para comparar com o perfil
  que o cliente devolver.

## Validação (`scripts/validate_input.py`)

Verifica: contagem; `chave_sequencial` permutação de 1..rows; `contrato` único e com 7–12 dígitos; nomes/ordem das
colunas; nulos só em `vr_renegociado`; histograma, clientes mistos, carteiras e checksums iguais ao manifest;
`flag_obito` constante por cliente; `qt_dias_atraso_acumulado >= qt_dias_atraso`; documentos com 11/14 dígitos e
nenhum válido pelo DV; tamanho dentro de ±5% de 1,10 GB (alerta apenas). Sai com código ≠ 0 se houver falha.

## Resultados desta execução (Apple Silicon, 18 threads, `--memory-limit 8GB`)

| Etapa | Resultado |
|---|---|
| `--calibrate` | recomenda `mixed` (−3,8%) — `results/calibracao.txt` |
| Amostra 1M | 0,37 s, 34,5 MB, 398.499 clientes, 0 falhas — `results/amostra_1m_validacao.txt` |
| Completo 30.076.105 | **11,4 s**, 1.059.319.819 bytes (1,059 GB, 35,22 B/linha), 29 row groups, 12.034.569 clientes, pico RSS 4,5 GB — `results/geracao_completa.txt` |
| Validação completa | 4 s, 20 checks PASS, 0 alertas — `results/validacao_completa.txt`, `results/perfil_sintetico.txt` |

## Próxima etapa (não implementada)

- `03_export`: `$out` S3 Parquet (plano e agrupado com `$unwind`/`$replaceWith`, `maxFileSize: "16GiB"`,
  `errorMode: "stop"`) e PyMongoArrow + `ParquetWriter`; validar com DuckDB (contagem, schema, checksums, 1 arquivo).
  O `$replaceWith` deve seguir `results/type_mapping.md` para reproduzir o schema do Parquet de entrada.
