# Resumo dos benchmarks — POC Rating (MongoDB Atlas)

Gerado em 2026-11-25T17:06:42+00:00 a partir de `results/summary.csv` e `results/*.json`.
Ambiente: Atlas **M40** (us-east-1), MongoDB 9.0.7, Data Federation 8.0.32; dados sintéticos seed 42, perfil `mixed`, 30,076,105 linhas / 12,034,569 clientes.

## Ponta a ponta (para a apresentação)

| Etapa | MongoDB Atlas (M40) | Referência do cliente (PostgreSQL) |
|---|---|---|
| 1. Carga do Parquet (30 M linhas) | **6.2 min** (369.2s (6m09s); mediana de 1 exec.) + índice `co_cpf_cnpj` 68.2s (1m08s) à parte | ~50–60 min (inclui regras por linha) |
| 2. Agrupamento por cliente + arrasto | **5.5 min** (330.5s (5m31s); 1 exec.) | ~50 min |
| 3. Exportação para Parquet | **7.6 min** (453.0s (7m33s); mediana de 3 exec.) → **1 arquivo** | ~40 min → ~200 arquivos |
| **Total** | **19.2 min** (+ 68.2s (1m08s) de índice) | **~2h20–3h** (cliente informa ~2h40 a 3h) |

Alternativa 1+2 em uma única operação (02b, carga já agrupada direto do S3 via Data Federation): **15.0 min** — mais simples operacionalmente, porém mais lenta que 01 + 02a.

## Todos os cenários

Tempo = mediana das execuções bem-sucedidas. Fonte do tempo: `$queryHistory` do ADF (início/fim medidos no servidor) ou cliente (`perf_counter`), conforme a coluna.

| Cenário | Exec. | Mediana | Mín / Máx | Fonte do tempo | Docs/linhas | Validação | Storage / arquivo | Índices / row groups |
|---|---|---|---|---|---|---|---|---|
| 01 carga plana — 1 arquivo (ADF $out) | 1/1 | **369.2s (6m09s)** | 369.2s (6m09s) / 369.2s (6m09s) | exato ($queryHistory) | 30,076,105 | OK (4/4) | 2.2 GB | 1.2 GB |
| 01 carga plana — 8 arquivos (ADF $out) | 1/1 | **366.6s (6m07s)** | 366.6s (6m07s) / 366.6s (6m07s) | exato ($queryHistory) | 30,076,105 | OK (3/3) | 2.2 GB | 820.7 MB |
| 01 carga plana — fumaça 1M | 4/4 | **16.0s** | 14.9s / 16.5s | exato (cliente) | 1,000,000 | OK (4/4) | 71.2 MB | 39.3 MB |
| 02a agrupamento + arrasto no cluster | 1/1 | **330.5s (5m31s)** | 330.5s (5m31s) / 330.5s (5m31s) | exato (cliente) | 12,034,569 | OK (12/12) | 2.1 GB | 412.1 MB |
| 02a agrupamento — fumaça 1M | 1/1 | **10.0s** | 10.0s / 10.0s | exato (cliente) | 398,499 | OK (12/12) | 52.5 MB | 9.1 MB |
| 02b carga agrupada direto do S3 (ADF) — 1 arquivo | 1/1 | **899.3s (14m59s)** | 899.3s (14m59s) / 899.3s (14m59s) | exato ($queryHistory) | 12,034,569 | OK (12/12) | 2.1 GB | 412.9 MB |
| 02b carga agrupada — fumaça 1M | 1/1 | **58.4s** | 58.4s / 58.4s | exato (cliente) | 398,499 | OK (12/12) | 60.0 MB | 12.4 MB |
| 03 export — ensaio 1M (agrupado) | 1/1 | **16.7s** | 16.7s / 16.7s | exato ($queryHistory) | 1,000,000 | OK (19/19) | 29.0 MB em 1 arq. | 2 row groups |
| 03a export plano via ADF (15 colunas, 1 arquivo) | 3/3 | **386.5s (6m26s)** | 363.1s (6m03s) / 406.8s (6m47s) | exato ($queryHistory) | 30,076,105 | OK (16/16) | 1.0 GB em 1 arq. | 39 row groups |
| 03b export agrupado via ADF (17 colunas, 1 arquivo) | 3/3 | **453.0s (7m33s)** | 411.6s (6m52s) / 453.6s (7m34s) | exato ($queryHistory) | 30,076,105 | OK (19/19) | 868.4 MB em 1 arq. | 43 row groups |
| 03b-multi export agrupado (maxFileSize padrão) | 1/1 | **373.6s (6m14s)** | 373.6s (6m14s) / 373.6s (6m14s) | exato ($queryHistory) | 30,076,105 | OK (19/19) | 872.9 MB em 54 arq. | 54 row groups |

## Exportações (etapa 03) — cada execução

| Cenário | Run | Duração | Fonte | Linhas | Arquivos | Tamanho | Row groups | Compressão | Validação |
|---|---|---|---|---|---|---|---|---|---|
| 03 export — ensaio 1M (agrupado) | 1 | 16.7s | exato ($queryHistory) | 1,000,000 | 1 | 29.0 MB | 2 | SNAPPY | OK |
| 03a export plano via ADF (15 colunas, 1 arquivo) | 1 | 406.8s (6m47s) | exato ($queryHistory) | 30,076,105 | 1 | 1.0 GB | 39 | SNAPPY | OK |
| 03a export plano via ADF (15 colunas, 1 arquivo) | 2 | 363.1s (6m03s) | exato ($queryHistory) | 30,076,105 | 1 | 1.0 GB | 39 | SNAPPY | OK |
| 03a export plano via ADF (15 colunas, 1 arquivo) | 3 | 386.5s (6m26s) | exato ($queryHistory) | 30,076,105 | 1 | 1.0 GB | 39 | SNAPPY | OK |
| 03b export agrupado via ADF (17 colunas, 1 arquivo) | 1 | 453.6s (7m34s) | exato ($queryHistory) | 30,076,105 | 1 | 868.5 MB | 43 | SNAPPY | OK |
| 03b export agrupado via ADF (17 colunas, 1 arquivo) | 2 | 453.0s (7m33s) | exato ($queryHistory) | 30,076,105 | 1 | 868.4 MB | 43 | SNAPPY | OK |
| 03b export agrupado via ADF (17 colunas, 1 arquivo) | 3 | 411.6s (6m52s) | exato ($queryHistory) | 30,076,105 | 1 | 868.4 MB | 43 | SNAPPY | OK |
| 03b-multi export agrupado (maxFileSize padrão) | 1 | 373.6s (6m14s) | exato ($queryHistory) | 30,076,105 | 54 | 872.9 MB | 54 | SNAPPY | OK |

## Ressalvas

- **Dados sintéticos**: gerados por `generator/generate_input.py` (DuckDB, seed 42) a partir do header, de 3 linhas de amostra e do volume do arquivo real; distribuições de contratos por cliente, atraso e saldo são premissas, não dados do cliente.
- **Arrasto simulado**: `rating_contrato`/`rating_final` vêm de faixas de dias de atraso inspiradas na Res. 2682 (`02_group_arrasto/rules.py`), não da regra real do cliente.
- **Etapa 1 do cliente inclui regras de negócio por linha** que não foram reproduzidas aqui; o tempo de carga mede apenas Parquet (S3) → coleção, mais o índice informado à parte.
- **Infra**: cluster Atlas M40 em us-east-1, Data Federation na mesma região, bucket S3 na mesma região. Outros tiers/regiões mudam os tempos.
- **Origem dos tempos**: etapas via Data Federation (01, 02b, 03) usam `$queryHistory` (início/fim medidos no servidor; exato). 02a (cluster) é o wall time do cliente em torno do `aggregate` (exato). As cargas 01/02b foram inicialmente medidas por polling (±10 s) e depois reconciliadas com o `$queryHistory` (`scripts/reconcile_query_history.py`); a diferença ficou entre 2 e 8 s.
- **Schema de saída**: tipos físicos/lógicos idênticos aos da entrada; diferem apenas anotações (ConvertedType legado vs LogicalType) e nullability (o ADF marca todas as colunas como OPTIONAL). Ver diff em cada cenário 03.
- Validações do export usam o arquivo baixado para a máquina local; a comparação linha a linha compara `(chave_sequencial, hash das 15 colunas)` nos dois sentidos contra o Parquet de entrada.

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

## Observações — etapa 03 (export)

- **`background: true` + `$queryHistory` resolveu a medição.** O ADF aceita o `aggregate` em ~0,3 s e segue executando;
  o `$queryHistory` (db `admin`) devolve `startTime`/`endTime`/`ok`/`error` de cada query. Todos os tempos da etapa 03
  são exatos, medidos no servidor. O mesmo histórico permitiu reconciliar as cargas 01/02b (polling errava de 2 a 8 s).
- **03b (principal): 30.076.105 linhas → 1 arquivo Parquet de 868 MB em 7,6 min (mediana de 3: 453,6 / 453,0 / 411,6 s)**,
  43 row groups, snappy. Validação completa 3×: contagem, chaves únicas, checksums, nulos, igualdade linha a linha com a
  entrada (0 diferenças), `rating_final` consistente, leitura com pyarrow.
- **03a (plano, 15 colunas): 6,4 min** (406,8 / 363,1 / 386,5 s). O `$unwind` + 2 colunas do 03b custam ~15%.
- **03b-multi (maxFileSize padrão): 54 arquivos de ~16 MB em 6,2 min** — ligeiramente mais rápido que o arquivo único,
  porque o ADF paraleliza a escrita de vários objetos. Reproduz o problema atual do cliente (~200 arquivos): o custo de
  ter 1 arquivo é ~1 min a mais, e `maxFileSize` é o único parâmetro que muda.
- **Tamanho**: 03b (17 colunas) = 868 MB < 03a (15 colunas) = 1,11 GB. A saída agrupada sai ordenada por cliente, então
  `co_cpf_cnpj` (coluna mais pesada da entrada) comprime muito melhor com valores consecutivos repetidos.
- **Arrasto na saída**: 5.311.074 linhas (17,66%) têm `rating_final != rating_contrato`, em 2.310.901 clientes (19,20%) —
  exatamente os clientes com ranks diferentes identificados na etapa 02.
- **Schema**: tipos físicos/lógicos das 15 colunas idênticos aos da entrada (normalizados). Diferenças de anotação
  (ConvertedType legado × LogicalType) e nullability (REQUIRED → OPTIONAL em 4 colunas) listadas em cada relatório;
  não são controláveis pelos parâmetros do `$out`.
- **03c (PyMongoArrow)** não foi executado — o script está pronto para uma EC2 (ver `03_export/README.md`); do notebook
  o tempo mediria a rede, não o banco. Teste funcional com 1M passou na validação completa.

## Detalhes por cenário (última execução)

### 01 carga plana — 1 arquivo (ADF $out)

- Origem: `dbimport.s3import` → destino: `ratings_poc.contratos_flat`
- Tempos: run 1: 369.2s (6m09s) [exato ($queryHistory)]
- índice `co_cpf_cnpj`: 68.2s (1m08s), 426.0 MB
- nota: tempo exato do $queryHistory do ADF (reconciliado; estimativa original por polling: 374.9s)
- [x] countDocuments == manifest.rows — 30,076,105 vs 30,076,105
- [x] sum_saldo == manifest (±0.01) — 140,014,373,887.45 vs 140,014,373,887.45
- [x] sum_saldo_limite == manifest (±0.01) — 43,255,948,972.24 vs 43,255,948,972.24
- [x] distinct co_cpf_cnpj == manifest.customers — 12,034,569 vs 12,034,569

### 01 carga plana — 8 arquivos (ADF $out)

- Origem: `dbimport.s3import_split` → destino: `ratings_poc.contratos_flat_split`
- Tempos: run 1: 366.6s (6m07s) [exato ($queryHistory)]
- nota: tempo exato do $queryHistory do ADF (reconciliado; estimativa original por polling: 374.9s)
- [x] countDocuments == manifest.rows — 30,076,105 vs 30,076,105
- [x] sum_saldo == manifest (±0.01) — 140,014,373,887.45 vs 140,014,373,887.45
- [x] sum_saldo_limite == manifest (±0.01) — 43,255,948,972.24 vs 43,255,948,972.24

### 01 carga plana — fumaça 1M

- Origem: `dbimport.s3import_1m` → destino: `ratings_poc.contratos_flat_1m`
- Tempos: run 1: 16.3s [exato (cliente)], run 1: 15.7s [exato (cliente)], run 1: 16.5s [exato (cliente)], run 1: 14.9s [exato (cliente)]
- índice `co_cpf_cnpj`: 2.0s, 12.4 MB
- [x] countDocuments == manifest.rows — 1,000,000 vs 1,000,000
- [x] sum_saldo == manifest (±0.01) — 4,657,063,653.98 vs 4,657,063,653.98
- [x] sum_saldo_limite == manifest (±0.01) — 1,436,852,496.92 vs 1,436,852,496.92
- [x] distinct co_cpf_cnpj == manifest.customers — 398,499 vs 398,499

### 02a agrupamento + arrasto no cluster

- Origem: `ratings_poc.contratos_flat` → destino: `ratings_poc.clientes_agrupados`
- Tempos: run 1: 330.5s (5m31s) [exato (cliente)]
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
- explain executionStats (200,000 linhas): $group usedDisk=True, spills=4, spilledBytes=367924587 (`explain_02a_contratos_flat_200000.json`)
- storage agrupado / plano: 0.934x
- clientes em que o arrasto altera algum contrato: 2,310,901 (19.202%)
- maior documento: 8.7 KB; médio: 858 B

### 02a agrupamento — fumaça 1M

- Origem: `ratings_poc.contratos_flat_1m` → destino: `ratings_poc.clientes_agrupados_1m`
- Tempos: run 1: 10.0s [exato (cliente)]
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
- explain executionStats (200,000 linhas): $group usedDisk=True, spills=4, spilledBytes=351773430 (`explain_02a_contratos_flat_1m_200000.json`)
- storage agrupado / plano: 0.738x
- clientes em que o arrasto altera algum contrato: 76,874 (19.291%)
- maior documento: 8.7 KB; médio: 861 B

### 02b carga agrupada direto do S3 (ADF) — 1 arquivo

- Origem: `dbimport.s3import` → destino: `ratings_poc.clientes_agrupados_adf`
- Tempos: run 1: 899.3s (14m59s) [exato ($queryHistory)]
- nota: tempo exato do $queryHistory do ADF (reconciliado; estimativa original por polling: 901.7s)
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
- clientes em que o arrasto altera algum contrato: 2,310,901 (19.202%)
- maior documento: 8.7 KB; médio: 858 B

### 02b carga agrupada — fumaça 1M

- Origem: `dbimport.s3import_1m` → destino: `ratings_poc.clientes_agrupados_adf_1m`
- Tempos: run 1: 58.4s [exato (cliente)]
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
- clientes em que o arrasto altera algum contrato: 76,874 (19.291%)
- maior documento: 8.7 KB; médio: 861 B

### 03 export — ensaio 1M (agrupado)

- Origem: `ratings_poc.clientes_agrupados_1m` → destino: `s3://btg-ratings-poc-dgoncalves/output/ensaio/20261125T161019Z/`
- Tempos: run 1: 16.7s [exato ($queryHistory)]
- [x] no error/other files in the prefix — 1 parquet file(s)
- [x] exactly 1 parquet file generated — 1 file(s)
- [x] column names and order — data_referencia, chave_sequencial, co_cpf_cnpj, contrato, qt_dias_atraso, saldo, saldo_limite, ctr_elegivel, carteira, cod_produto, flag_prejuizo_2682, flag_obito, qt_dias_atraso_acumulado, modalidade, vr_renegociado, rating_contrato, rating_final
- [x] no nested/polymorphic (group) columns — all leaves
- [x] first 15 columns: same physical/logical type as input (normalized) — identical
- [~] first 15 columns: identical annotations and nullability (repetition) — 17 difference(s): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: in
- [x] compression is snappy — SNAPPY
- [x] count == manifest.rows — 1,000,000 vs 1,000,000
- [x] chave_sequencial unique and covering 1..N — min=1 max=1000000 distinct=1,000,000
- [x] contrato unique — distinct=1,000,000
- [x] sum(saldo) == manifest — 4657063653.98 vs 4657063653.98
- [x] sum(saldo_limite) == manifest — 1436852496.92 vs 1436852496.92
- [x] vr_renegociado null count == input — 949,903 vs 949,903
- [x] nulls only in vr_renegociado — ok
- [x] row-by-row: 15 columns identical to input (keyed hash EXCEPT both ways) — missing=0 extra=0 (0.2s)
- [x] rating_final constant per co_cpf_cnpj — violations=0
- [x] rating_final == worst rating_contrato of the customer — violations=0
- [x] rows with rating_final != rating_contrato (arrasto effect) — 177,004 rows (17.70%) in 76,874 customers (19.29% — expected ≈19.2%, the share of customers with min rank != max rank in 02)
- [x] pyarrow reads the whole file — 1,000,000 rows in 0.0s; schema: ['data_referencia: string', 'chave_sequencial: int64', 'co_cpf_cnpj: string', 'contrato: int64', 'qt_dias_atraso: int32', 'saldo: double', 'saldo_limite: double', 'ctr_elegivel: string', 'carteira: string', 'cod_produto: int32', 'flag_prejuizo_2682: int32', 'flag_obito: int32', 'qt_dias_atraso_acumulado: int32', 'modalidade: int32', 'vr_renegociado: string', 'rating

Schema entrada × saída:

| # | coluna | entrada: físico / converted / logical / repetition | saída: físico / converted / logical / repetition | tipo | anotação/nullability |
|---|---|---|---|---|---|
| 1 | `data_referencia` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 2 | `chave_sequencial` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 3 | `co_cpf_cnpj` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 4 | `contrato` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 5 | `qt_dias_atraso` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 6 | `saldo` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 7 | `saldo_limite` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 8 | `ctr_elegivel` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 9 | `carteira` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 10 | `cod_produto` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 11 | `flag_prejuizo_2682` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 12 | `flag_obito` | INT32 / INT_32 / None / REQUIRED | INT32 / None / None / OPTIONAL | igual | difere |
| 13 | `qt_dias_atraso_acumulado` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 14 | `modalidade` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 15 | `vr_renegociado` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 16 | `rating_contrato` | (não existe na entrada) | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | — | — |
| 17 | `rating_final` | (não existe na entrada) | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | — | — |

Diferenças de anotação/nullability (não alteram os dados): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: input='INT_64' output=None; col 5 qt_dias_atraso.converted_type: input='INT_32' output=None; col 8 ctr_elegivel.logical_type: input=None output='StringType()'; col 8 ctr_elegivel.repetition_type: input='REQUIRED' output='OPTIONAL'; col 9 carteira.logical_type: input=None output='StringType()'; col 10 cod_produto.converted_type: input='INT_32' output=None; col 11 flag_prejuizo_2682.converted_type: input='INT_32' output=None; col 12 flag_obito.converted_type: input='INT_32' output=None; col 12 flag_obito.repetition_type: input='REQUIRED' output='OPTIONAL'; col 13 qt_dias_atraso_acumulado.converted_type: input='INT_32' output=None; col 14 modalidade.converted_type: input='INT_32' output=None; col 15 vr_renegociado.logical_type: input=None output='StringType()'

### 03a export plano via ADF (15 colunas, 1 arquivo)

- Origem: `ratings_poc.contratos_flat` → destino: `s3://btg-ratings-poc-dgoncalves/output/03a/20261125T165012Z/`
- Tempos: run 1: 406.8s (6m47s) [exato ($queryHistory)], run 2: 363.1s (6m03s) [exato ($queryHistory)], run 3: 386.5s (6m26s) [exato ($queryHistory)]
- [x] no error/other files in the prefix — 1 parquet file(s)
- [x] exactly 1 parquet file generated — 1 file(s)
- [x] column names and order — data_referencia, chave_sequencial, co_cpf_cnpj, contrato, qt_dias_atraso, saldo, saldo_limite, ctr_elegivel, carteira, cod_produto, flag_prejuizo_2682, flag_obito, qt_dias_atraso_acumulado, modalidade, vr_renegociado
- [x] no nested/polymorphic (group) columns — all leaves
- [x] first 15 columns: same physical/logical type as input (normalized) — identical
- [~] first 15 columns: identical annotations and nullability (repetition) — 17 difference(s): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: in
- [x] compression is snappy — SNAPPY
- [x] count == manifest.rows — 30,076,105 vs 30,076,105
- [x] chave_sequencial unique and covering 1..N — min=1 max=30076105 distinct=30,076,105
- [x] contrato unique — distinct=30,076,105
- [x] sum(saldo) == manifest — 140014373887.45 vs 140014373887.45
- [x] sum(saldo_limite) == manifest — 43255948972.24 vs 43255948972.24
- [x] vr_renegociado null count == input — 28,571,205 vs 28,571,205
- [x] nulls only in vr_renegociado — ok
- [x] row-by-row: 15 columns identical to input (keyed hash EXCEPT both ways) — missing=0 extra=0 (3.5s)
- [x] pyarrow reads the whole file — 30,076,105 rows in 0.9s; schema: ['data_referencia: string', 'chave_sequencial: int64', 'co_cpf_cnpj: string', 'contrato: int64', 'qt_dias_atraso: int32', 'saldo: double', 'saldo_limite: double', 'ctr_elegivel: string', 'carteira: string', 'cod_produto: int32', 'flag_prejuizo_2682: int32', 'flag_obito: int32', 'qt_dias_atraso_acumulado: int32', 'modalidade: int32', 'vr_renegociado: string']

Schema entrada × saída:

| # | coluna | entrada: físico / converted / logical / repetition | saída: físico / converted / logical / repetition | tipo | anotação/nullability |
|---|---|---|---|---|---|
| 1 | `data_referencia` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 2 | `chave_sequencial` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 3 | `co_cpf_cnpj` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 4 | `contrato` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 5 | `qt_dias_atraso` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 6 | `saldo` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 7 | `saldo_limite` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 8 | `ctr_elegivel` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 9 | `carteira` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 10 | `cod_produto` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 11 | `flag_prejuizo_2682` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 12 | `flag_obito` | INT32 / INT_32 / None / REQUIRED | INT32 / None / None / OPTIONAL | igual | difere |
| 13 | `qt_dias_atraso_acumulado` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 14 | `modalidade` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 15 | `vr_renegociado` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |

Diferenças de anotação/nullability (não alteram os dados): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: input='INT_64' output=None; col 5 qt_dias_atraso.converted_type: input='INT_32' output=None; col 8 ctr_elegivel.logical_type: input=None output='StringType()'; col 8 ctr_elegivel.repetition_type: input='REQUIRED' output='OPTIONAL'; col 9 carteira.logical_type: input=None output='StringType()'; col 10 cod_produto.converted_type: input='INT_32' output=None; col 11 flag_prejuizo_2682.converted_type: input='INT_32' output=None; col 12 flag_obito.converted_type: input='INT_32' output=None; col 12 flag_obito.repetition_type: input='REQUIRED' output='OPTIONAL'; col 13 qt_dias_atraso_acumulado.converted_type: input='INT_32' output=None; col 14 modalidade.converted_type: input='INT_32' output=None; col 15 vr_renegociado.logical_type: input=None output='StringType()'

### 03b export agrupado via ADF (17 colunas, 1 arquivo)

- Origem: `ratings_poc.clientes_agrupados` → destino: `s3://btg-ratings-poc-dgoncalves/output/03b/20261125T162817Z/`
- Tempos: run 1: 453.6s (7m34s) [exato ($queryHistory)], run 2: 453.0s (7m33s) [exato ($queryHistory)], run 3: 411.6s (6m52s) [exato ($queryHistory)]
- [x] no error/other files in the prefix — 1 parquet file(s)
- [x] exactly 1 parquet file generated — 1 file(s)
- [x] column names and order — data_referencia, chave_sequencial, co_cpf_cnpj, contrato, qt_dias_atraso, saldo, saldo_limite, ctr_elegivel, carteira, cod_produto, flag_prejuizo_2682, flag_obito, qt_dias_atraso_acumulado, modalidade, vr_renegociado, rating_contrato, rating_final
- [x] no nested/polymorphic (group) columns — all leaves
- [x] first 15 columns: same physical/logical type as input (normalized) — identical
- [~] first 15 columns: identical annotations and nullability (repetition) — 17 difference(s): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: in
- [x] compression is snappy — SNAPPY
- [x] count == manifest.rows — 30,076,105 vs 30,076,105
- [x] chave_sequencial unique and covering 1..N — min=1 max=30076105 distinct=30,076,105
- [x] contrato unique — distinct=30,076,105
- [x] sum(saldo) == manifest — 140014373887.45 vs 140014373887.45
- [x] sum(saldo_limite) == manifest — 43255948972.24 vs 43255948972.24
- [x] vr_renegociado null count == input — 28,571,205 vs 28,571,205
- [x] nulls only in vr_renegociado — ok
- [x] row-by-row: 15 columns identical to input (keyed hash EXCEPT both ways) — missing=0 extra=0 (2.2s)
- [x] rating_final constant per co_cpf_cnpj — violations=0
- [x] rating_final == worst rating_contrato of the customer — violations=0
- [x] rows with rating_final != rating_contrato (arrasto effect) — 5,311,074 rows (17.66%) in 2,310,901 customers (19.20% — expected ≈19.2%, the share of customers with min rank != max rank in 02)
- [x] pyarrow reads the whole file — 30,076,105 rows in 0.4s; schema: ['data_referencia: string', 'chave_sequencial: int64', 'co_cpf_cnpj: string', 'contrato: int64', 'qt_dias_atraso: int32', 'saldo: double', 'saldo_limite: double', 'ctr_elegivel: string', 'carteira: string', 'cod_produto: int32', 'flag_prejuizo_2682: int32', 'flag_obito: int32', 'qt_dias_atraso_acumulado: int32', 'modalidade: int32', 'vr_renegociado: string', 'ratin

Schema entrada × saída:

| # | coluna | entrada: físico / converted / logical / repetition | saída: físico / converted / logical / repetition | tipo | anotação/nullability |
|---|---|---|---|---|---|
| 1 | `data_referencia` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 2 | `chave_sequencial` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 3 | `co_cpf_cnpj` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 4 | `contrato` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 5 | `qt_dias_atraso` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 6 | `saldo` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 7 | `saldo_limite` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 8 | `ctr_elegivel` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 9 | `carteira` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 10 | `cod_produto` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 11 | `flag_prejuizo_2682` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 12 | `flag_obito` | INT32 / INT_32 / None / REQUIRED | INT32 / None / None / OPTIONAL | igual | difere |
| 13 | `qt_dias_atraso_acumulado` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 14 | `modalidade` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 15 | `vr_renegociado` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 16 | `rating_contrato` | (não existe na entrada) | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | — | — |
| 17 | `rating_final` | (não existe na entrada) | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | — | — |

Diferenças de anotação/nullability (não alteram os dados): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: input='INT_64' output=None; col 5 qt_dias_atraso.converted_type: input='INT_32' output=None; col 8 ctr_elegivel.logical_type: input=None output='StringType()'; col 8 ctr_elegivel.repetition_type: input='REQUIRED' output='OPTIONAL'; col 9 carteira.logical_type: input=None output='StringType()'; col 10 cod_produto.converted_type: input='INT_32' output=None; col 11 flag_prejuizo_2682.converted_type: input='INT_32' output=None; col 12 flag_obito.converted_type: input='INT_32' output=None; col 12 flag_obito.repetition_type: input='REQUIRED' output='OPTIONAL'; col 13 qt_dias_atraso_acumulado.converted_type: input='INT_32' output=None; col 14 modalidade.converted_type: input='INT_32' output=None; col 15 vr_renegociado.logical_type: input=None output='StringType()'

### 03b-multi export agrupado (maxFileSize padrão)

- Origem: `ratings_poc.clientes_agrupados` → destino: `s3://btg-ratings-poc-dgoncalves/output/03b-multi/20261125T165731Z/`
- Tempos: run 1: 373.6s (6m14s) [exato ($queryHistory)]
- [x] no error/other files in the prefix — 54 parquet file(s)
- [x] parquet files generated — 54
- [x] column names and order — data_referencia, chave_sequencial, co_cpf_cnpj, contrato, qt_dias_atraso, saldo, saldo_limite, ctr_elegivel, carteira, cod_produto, flag_prejuizo_2682, flag_obito, qt_dias_atraso_acumulado, modalidade, vr_renegociado, rating_contrato, rating_final
- [x] no nested/polymorphic (group) columns — all leaves
- [x] first 15 columns: same physical/logical type as input (normalized) — identical
- [~] first 15 columns: identical annotations and nullability (repetition) — 17 difference(s): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: in
- [x] compression is snappy — SNAPPY
- [x] count == manifest.rows — 30,076,105 vs 30,076,105
- [x] chave_sequencial unique and covering 1..N — min=1 max=30076105 distinct=30,076,105
- [x] contrato unique — distinct=30,076,105
- [x] sum(saldo) == manifest — 140014373887.45 vs 140014373887.45
- [x] sum(saldo_limite) == manifest — 43255948972.24 vs 43255948972.24
- [x] vr_renegociado null count == input — 28,571,205 vs 28,571,205
- [x] nulls only in vr_renegociado — ok
- [x] row-by-row: 15 columns identical to input (keyed hash EXCEPT both ways) — missing=0 extra=0 (2.4s)
- [x] rating_final constant per co_cpf_cnpj — violations=0
- [x] rating_final == worst rating_contrato of the customer — violations=0
- [x] rows with rating_final != rating_contrato (arrasto effect) — 5,311,074 rows (17.66%) in 2,310,901 customers (19.20% — expected ≈19.2%, the share of customers with min rank != max rank in 02)
- [x] pyarrow reads the whole file — 30,076,105 rows in 0.5s; schema: ['data_referencia: string', 'chave_sequencial: int64', 'co_cpf_cnpj: string', 'contrato: int64', 'qt_dias_atraso: int32', 'saldo: double', 'saldo_limite: double', 'ctr_elegivel: string', 'carteira: string', 'cod_produto: int32', 'flag_prejuizo_2682: int32', 'flag_obito: int32', 'qt_dias_atraso_acumulado: int32', 'modalidade: int32', 'vr_renegociado: string', 'ratin

Schema entrada × saída:

| # | coluna | entrada: físico / converted / logical / repetition | saída: físico / converted / logical / repetition | tipo | anotação/nullability |
|---|---|---|---|---|---|
| 1 | `data_referencia` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 2 | `chave_sequencial` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 3 | `co_cpf_cnpj` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 4 | `contrato` | INT64 / INT_64 / None / OPTIONAL | INT64 / None / None / OPTIONAL | igual | difere |
| 5 | `qt_dias_atraso` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 6 | `saldo` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 7 | `saldo_limite` | DOUBLE / None / None / OPTIONAL | DOUBLE / None / None / OPTIONAL | igual | igual |
| 8 | `ctr_elegivel` | BYTE_ARRAY / UTF8 / None / REQUIRED | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 9 | `carteira` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 10 | `cod_produto` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 11 | `flag_prejuizo_2682` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 12 | `flag_obito` | INT32 / INT_32 / None / REQUIRED | INT32 / None / None / OPTIONAL | igual | difere |
| 13 | `qt_dias_atraso_acumulado` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 14 | `modalidade` | INT32 / INT_32 / None / OPTIONAL | INT32 / None / None / OPTIONAL | igual | difere |
| 15 | `vr_renegociado` | BYTE_ARRAY / UTF8 / None / OPTIONAL | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | igual | difere |
| 16 | `rating_contrato` | (não existe na entrada) | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | — | — |
| 17 | `rating_final` | (não existe na entrada) | BYTE_ARRAY / UTF8 / StringType() / OPTIONAL | — | — |

Diferenças de anotação/nullability (não alteram os dados): col 1 data_referencia.logical_type: input=None output='StringType()'; col 1 data_referencia.repetition_type: input='REQUIRED' output='OPTIONAL'; col 2 chave_sequencial.converted_type: input='INT_64' output=None; col 3 co_cpf_cnpj.logical_type: input=None output='StringType()'; col 3 co_cpf_cnpj.repetition_type: input='REQUIRED' output='OPTIONAL'; col 4 contrato.converted_type: input='INT_64' output=None; col 5 qt_dias_atraso.converted_type: input='INT_32' output=None; col 8 ctr_elegivel.logical_type: input=None output='StringType()'; col 8 ctr_elegivel.repetition_type: input='REQUIRED' output='OPTIONAL'; col 9 carteira.logical_type: input=None output='StringType()'; col 10 cod_produto.converted_type: input='INT_32' output=None; col 11 flag_prejuizo_2682.converted_type: input='INT_32' output=None; col 12 flag_obito.converted_type: input='INT_32' output=None; col 12 flag_obito.repetition_type: input='REQUIRED' output='OPTIONAL'; col 13 qt_dias_atraso_acumulado.converted_type: input='INT_32' output=None; col 14 modalidade.converted_type: input='INT_32' output=None; col 15 vr_renegociado.logical_type: input=None output='StringType()'

