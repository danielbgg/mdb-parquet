# Resumo dos benchmarks

Gerado em 2026-11-25T15:24:39+00:00 a partir de `results/summary.csv`.
Tempo = mediana das execuções bem-sucedidas (wall time do `aggregate` com `$out`, medido pelo cliente).
Ambiente: Atlas M40 (us-east-1), MongoDB 9.0.7, ADF 8.0.32; manifest seed 42, perfil mixed.

| Cenário | Execuções | Mediana | Mín / Máx | Docs | Validação | Storage size | Index size | Avg obj |
|---|---|---|---|---|---|---|---|---|
| 01 carga plana — 1 arquivo (ADF $out) | 1/1 | **374.9s (6m15s)** | 374.9s (6m15s) / 374.9s (6m15s) | 30,076,105 | OK (4/4) | 2.2 GB | 1.2 GB | 334 B |
| 01 carga plana — N arquivos (ADF $out) | 1/1 | **374.9s (6m15s)** | 374.9s (6m15s) / 374.9s (6m15s) | 30,076,105 | OK (3/3) | 2.2 GB | 820.7 MB | 334 B |
| 01 carga plana — fumaça 1M | 4/4 | **16.0s** | 14.9s / 16.5s | 1,000,000 | OK (4/4) | 71.2 MB | 39.3 MB | 334 B |
| 02a agrupamento + arrasto no cluster | 1/1 | **330.5s (5m31s)** | 330.5s (5m31s) / 330.5s (5m31s) | 12,034,569 | OK (12/12) | 2.1 GB | 412.1 MB | 858 B |
| 02a agrupamento — fumaça 1M | 1/1 | **10.0s** | 10.0s / 10.0s | 398,499 | OK (12/12) | 52.5 MB | 9.1 MB | 861 B |
| 02b carga agrupada direto do S3 (ADF) — 1 arquivo | 1/1 | **901.7s (15m02s)** | 901.7s (15m02s) / 901.7s (15m02s) | 12,034,569 | OK (12/12) | 2.1 GB | 412.9 MB | 858 B |
| 02b carga agrupada — fumaça 1M | 1/1 | **58.4s** | 58.4s / 58.4s | 398,499 | OK (12/12) | 60.0 MB | 12.4 MB | 861 B |

## Observações

- **Tempos marcados como estimados (±10 s)**: nas cargas via ADF (01 single, 01 split, 02b) a resposta do `aggregate`
  nunca chegou ao cliente — a conexão ociosa com o ADF é descartada em execuções longas (>5 min). O tempo registrado é o
  instante em que a coleção de destino atingiu a contagem esperada (o ADF escreve em coleção temporária e renomeia ao
  final, então a contagem salta de 0 para o total num único passo). O 02a (cluster) teve resposta normal: tempo exato.
- **01 single = 01 split (375 s)**: ler 1 arquivo de 1,06 GB ou N arquivos não muda nada → o gargalo é a escrita do
  `$out` no cluster (≈80k docs/s, ~25 MB/s de BSON), não a leitura do S3.
- **01 + 02a (375 + 331 = 706 s ≈ 11,8 min) vs 02b (902 s ≈ 15 min)**: fazer o `$group` no ADF e escrever já agrupado
  foi ~28% mais lento do que carregar plano e agrupar no cluster. O 02b continua sendo uma única operação (mais simples
  operacionalmente), mas a hipótese "1+2 juntos é mais rápido" não se confirmou nesta M40. Todos os operadores do
  pipeline (`$set`, `$switch`, `$arrayElemAt`, `$group` com `$push`, `$ifNull`, `$out` para Atlas) foram aceitos pelo ADF.
- **Comparação com o Postgres atual** (~50 min importação + ~50 min arrasto): 01+02a ≈ 11,8 min, ou 02b ≈ 15 min.
- **Índice `co_cpf_cnpj`** em `contratos_flat`: 68 s para construir, 426 MB. Com ele, a validação de clientes distintos
  cai de >20 min (hash `$group` com spill) para segundos (`DISTINCT_SCAN`). O tempo está em `timings.index_build`,
  separado do `$out`.
- **`$group` do 02a faz spill** (explain em 200k linhas: `usedDisk=true`, 4 spills, 368 MB). Em 30M linhas o spill é
  inevitável numa M40 (16 GB RAM, limite de 100 MB por estágio sem `allowDiskUse`); ainda assim fechou em 5,5 min.
- **Storage**: plano 2,2 GB (dados 9,4 GB não comprimidos, 334 B/doc) vs agrupado 2,1 GB (9,6 GB, 858 B/doc) — ~0,93x;
  o agrupado elimina a repetição de `co_cpf_cnpj`/`data_referencia` mas adiciona `rating_contrato`, `pior_rank` etc.
  Índices: plano 1,2 GB (`_id` ObjectId 800 MB + `co_cpf_cnpj` 426 MB); agrupado 412 MB (só `_id` = CPF/CNPJ).
- **Maior documento**: 8,7 KB (30 contratos) — 0,05% do limite de 16 MB; média 858 B.
- **Clientes mistos**: 1.804.265 (14,99%) na definição do gerador (um contrato em dia e um >90 d); 2.310.901 (19,2%)
  têm ranks diferentes entre contratos, ou seja, o arrasto altera o rating de pelo menos um contrato nesses clientes.
- **Tipos** (`results/type_mapping.md`): BSON 1:1 com o Parquet (`long`, `int`, `double`, `string`), ordem de campos
  preservada e **Parquet NULL → campo ausente** (`vr_renegociado` missing em 95%). O modelo agrupado grava
  `vr_renegociado: null` explicitamente via `$ifNull`.

## Detalhes por cenário (última execução)

### 01 carga plana — 1 arquivo (ADF $out)

- Origem: `dbimport.s3import` → destino: `ratings_poc.contratos_flat`
- Tempos: run 1: 374.9s (6m15s)
- [x] countDocuments == manifest.rows — 30,076,105 vs 30,076,105
- [x] sum_saldo == manifest (±0.01) — 140,014,373,887.45 vs 140,014,373,887.45
- [x] sum_saldo_limite == manifest (±0.01) — 43,255,948,972.24 vs 43,255,948,972.24
- [x] distinct co_cpf_cnpj == manifest.customers — 12,034,569 vs 12,034,569

### 01 carga plana — N arquivos (ADF $out)

- Origem: `dbimport.s3import_split` → destino: `ratings_poc.contratos_flat_split`
- Tempos: run 1: 374.9s (6m15s)
- [x] countDocuments == manifest.rows — 30,076,105 vs 30,076,105
- [x] sum_saldo == manifest (±0.01) — 140,014,373,887.45 vs 140,014,373,887.45
- [x] sum_saldo_limite == manifest (±0.01) — 43,255,948,972.24 vs 43,255,948,972.24

### 01 carga plana — fumaça 1M

- Origem: `dbimport.s3import_1m` → destino: `ratings_poc.contratos_flat_1m`
- Tempos: run 1: 16.3s, run 1: 15.7s, run 1: 16.5s, run 1: 14.9s
- [x] countDocuments == manifest.rows — 1,000,000 vs 1,000,000
- [x] sum_saldo == manifest (±0.01) — 4,657,063,653.98 vs 4,657,063,653.98
- [x] sum_saldo_limite == manifest (±0.01) — 1,436,852,496.92 vs 1,436,852,496.92
- [x] distinct co_cpf_cnpj == manifest.customers — 398,499 vs 398,499

### 02a agrupamento + arrasto no cluster

- Origem: `ratings_poc.contratos_flat` → destino: `ratings_poc.clientes_agrupados`
- Tempos: run 1: 330.5s (5m31s)
- [x] countDocuments == manifest.customers — 12,034,569 vs 12,034,569
- [x] sum(qtd_contratos) == manifest.rows — 30,076,105 vs 30,076,105
- [x] qtd_contratos == size(contratos) for every document — violations=0
- [x] sum(contratos.saldo) == manifest (±0.01) — 140,014,373,887.45 vs 140,014,373,887.45
- [x] sum(contratos.saldo_limite) == manifest (±0.01) — 43,255,948,972.24 vs 43,255,948,972.24
- [x] pior_rank == max(rank of contratos) for every document — violations=0
- [x] rating_final == letter(pior_rank) for every document — violations=0
- [x] rating_contrato == letter(rank(qt_dias_atraso)) for every contract — violations=0
- [x] mixed customers (min days 0 & max > 90) == manifest.customers_mixed — 1,804,265 vs 1,804,265 (14.99% of customers)
- [x] customers with min rank != max rank (arrasto changes a contract) >= manifest mixed — 2,310,901 (19.20% of customers)
- [x] qtd_contratos histogram == manifest — 30 buckets, max 30 contracts
- [x] largest document << 16 MB — max=8.7 KB (0.053% of 16 MB), avg=858 B
- explain executionStats (200,000 linhas): $group usedDisk=True, spills=4, spilledBytes=367924587, maxUsedMemBytes=None (`explain_02a_contratos_flat_200000.json`)
- storage agrupado / plano: 0.934x
- clientes em que o arrasto altera algum contrato (min rank != max rank): 2,310,901 (19.202%)
- maior documento: 8.7 KB; médio: 858 B

### 02a agrupamento — fumaça 1M

- Origem: `ratings_poc.contratos_flat_1m` → destino: `ratings_poc.clientes_agrupados_1m`
- Tempos: run 1: 10.0s
- [x] countDocuments == manifest.customers — 398,499 vs 398,499
- [x] sum(qtd_contratos) == manifest.rows — 1,000,000 vs 1,000,000
- [x] qtd_contratos == size(contratos) for every document — violations=0
- [x] sum(contratos.saldo) == manifest (±0.01) — 4,657,063,653.98 vs 4,657,063,653.98
- [x] sum(contratos.saldo_limite) == manifest (±0.01) — 1,436,852,496.92 vs 1,436,852,496.92
- [x] pior_rank == max(rank of contratos) for every document — violations=0
- [x] rating_final == letter(pior_rank) for every document — violations=0
- [x] rating_contrato == letter(rank(qt_dias_atraso)) for every contract — violations=0
- [x] mixed customers (min days 0 & max > 90) == manifest.customers_mixed — 60,047 vs 60,047 (15.07% of customers)
- [x] customers with min rank != max rank (arrasto changes a contract) >= manifest mixed — 76,874 (19.29% of customers)
- [x] qtd_contratos histogram == manifest — 30 buckets, max 30 contracts
- [x] largest document << 16 MB — max=8.7 KB (0.053% of 16 MB), avg=861 B
- explain executionStats (200,000 linhas): $group usedDisk=True, spills=4, spilledBytes=351773430, maxUsedMemBytes=None (`explain_02a_contratos_flat_1m_200000.json`)
- storage agrupado / plano: 0.738x
- clientes em que o arrasto altera algum contrato (min rank != max rank): 76,874 (19.291%)
- maior documento: 8.7 KB; médio: 861 B

### 02b carga agrupada direto do S3 (ADF) — 1 arquivo

- Origem: `dbimport.s3import` → destino: `ratings_poc.clientes_agrupados_adf`
- Tempos: run 1: 901.7s (15m02s)
- [x] countDocuments == manifest.customers — 12,034,569 vs 12,034,569
- [x] sum(qtd_contratos) == manifest.rows — 30,076,105 vs 30,076,105
- [x] qtd_contratos == size(contratos) for every document — violations=0
- [x] sum(contratos.saldo) == manifest (±0.01) — 140,014,373,887.45 vs 140,014,373,887.45
- [x] sum(contratos.saldo_limite) == manifest (±0.01) — 43,255,948,972.24 vs 43,255,948,972.24
- [x] pior_rank == max(rank of contratos) for every document — violations=0
- [x] rating_final == letter(pior_rank) for every document — violations=0
- [x] rating_contrato == letter(rank(qt_dias_atraso)) for every contract — violations=0
- [x] mixed customers (min days 0 & max > 90) == manifest.customers_mixed — 1,804,265 vs 1,804,265 (14.99% of customers)
- [x] customers with min rank != max rank (arrasto changes a contract) >= manifest mixed — 2,310,901 (19.20% of customers)
- [x] qtd_contratos histogram == manifest — 30 buckets, max 30 contracts
- [x] largest document << 16 MB — max=8.7 KB (0.053% of 16 MB), avg=858 B
- storage agrupado / plano: 0.935x
- clientes em que o arrasto altera algum contrato (min rank != max rank): 2,310,901 (19.202%)
- maior documento: 8.7 KB; médio: 858 B

### 02b carga agrupada — fumaça 1M

- Origem: `dbimport.s3import_1m` → destino: `ratings_poc.clientes_agrupados_adf_1m`
- Tempos: run 1: 58.4s
- [x] countDocuments == manifest.customers — 398,499 vs 398,499
- [x] sum(qtd_contratos) == manifest.rows — 1,000,000 vs 1,000,000
- [x] qtd_contratos == size(contratos) for every document — violations=0
- [x] sum(contratos.saldo) == manifest (±0.01) — 4,657,063,653.98 vs 4,657,063,653.98
- [x] sum(contratos.saldo_limite) == manifest (±0.01) — 1,436,852,496.92 vs 1,436,852,496.92
- [x] pior_rank == max(rank of contratos) for every document — violations=0
- [x] rating_final == letter(pior_rank) for every document — violations=0
- [x] rating_contrato == letter(rank(qt_dias_atraso)) for every contract — violations=0
- [x] mixed customers (min days 0 & max > 90) == manifest.customers_mixed — 60,047 vs 60,047 (15.07% of customers)
- [x] customers with min rank != max rank (arrasto changes a contract) >= manifest mixed — 76,874 (19.29% of customers)
- [x] qtd_contratos histogram == manifest — 30 buckets, max 30 contracts
- [x] largest document << 16 MB — max=8.7 KB (0.053% of 16 MB), avg=861 B
- storage agrupado / plano: 0.844x
- clientes em que o arrasto altera algum contrato (min rank != max rank): 76,874 (19.291%)
- maior documento: 8.7 KB; médio: 861 B

