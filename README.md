---
glean_provenance: {"generation_id":"b97f6b0798ee4e7187f73ecceda82b4c.1","generated_at":"2026-11-25T18:17:07Z","model":"Glean Assistant 571.123"}
---
# ratings-poc — POC de performance MongoDB Atlas para processamento de rating em lote

POC que reproduz, no MongoDB Atlas, um pipeline batch de rating de crédito com ~30 milhões de contratos:

1. **Carga** de um arquivo Parquet do S3 para o Atlas.
2. **Agrupamento por cliente e arrasto** (propagar o pior rating do cliente para todos os seus contratos).
3. **Exportação** de volta para o S3 em **um único arquivo Parquet**.

Tudo roda em serviços gerenciados (cluster Atlas + Atlas Data Federation `$out`), sem aplicação intermediária lendo e
escrevendo linha a linha. Os dados de entrada são **sintéticos**, com o mesmo layout, tipos e volume de um arquivo real
de referência; o repositório também traz o caminho para repetir o teste com o arquivo real, no ambiente do próprio cliente.

## Resumo dos resultados

Atlas M40, AWS us-east-1, MongoDB 9.0.7, 30.076.105 linhas (1,06 GB), 12.034.569 clientes. Tempos medidos no servidor
via `$queryHistory`.

| Etapa | MongoDB Atlas (POC) | Referência: processo atual em PostgreSQL |
|---|---|---|
| 1. Carga Parquet S3 → Atlas (ADF `$out`) | **6,2 min** (369 s) + 68 s de índice `co_cpf_cnpj` | ~50–60 min |
| 2. Agrupamento por cliente + arrasto (02a) | **5,5 min** (331 s) | ~50 min |
| 3. Export 30.076.105 linhas → **1 Parquet** (03b) | **7,6 min** (mediana de 3: 454 / 453 / 412 s), 868 MB | ~40 min, ~200 arquivos |
| **Total** | **≈ 20 min** (≈ 19 min sem o índice) | **≈ 2h40–3h** |

- Ganho total de ~8×; por etapa, entre ~5× (exportação) e ~9× (arrasto).
- Exportação validada linha a linha contra a entrada: **0 linhas faltando, 0 sobrando**, em todas as execuções.
- Os tempos da coluna de referência foram **informados** pela equipe do processo atual, não medidos nesta POC.

Detalhes, outros cenários e ressalvas: [Resultados detalhados](#resultados-detalhados) e
[results/summary.md](results/summary.md).

## Arquitetura

```
   Amazon S3                      MongoDB Atlas (gerenciado)                       Amazon S3
┌──────────────┐   01 $out   ┌───────────────────────────────────┐   03 $out   ┌──────────────┐
│ Parquet de   │ ──────────> │ Data Federation ──> Cluster (M40) │ ──────────> │ 1 arquivo    │
│ entrada      │             │                     contratos_flat│             │ Parquet      │
│ 30M linhas   │             │  02 aggregation:   ──> clientes_  │             │ 15 + 2 cols  │
│ 15 colunas   │             │  agrupa + arrasto      agrupados  │             │              │
└──────────────┘             └───────────────────────────────────┘             └──────────────┘
```

- **01_load**: o Data Federation lê o Parquet do S3 (coleção virtual `dbimport.s3import`) e grava no cluster
  (`contratos_flat`) com um único `$out`.
- **02_group_arrasto**: uma aggregation agrupa os contratos por cliente (`clientes_agrupados`, 1 documento por cliente)
  e calcula `rating_contrato` e `rating_final`.
- **03_export**: o Data Federation desagrupa (`$unwind` + `$replaceWith` tipado) e grava Parquet no S3 com `$out`.
  O parâmetro `maxFileSize` decide se a saída é 1 arquivo ou N arquivos.

Modelo agrupado (simplificado):

```js
{
  co_cpf_cnpj: "…", data_referencia: …,
  rating_final: "C",                       // pior rating entre os contratos do cliente
  contratos: [
    { contrato: …, qt_dias_atraso: 0,  rating_contrato: "A", … },
    { contrato: …, qt_dias_atraso: 75, rating_contrato: "C", … }
  ]
}
```

## O que esta POC prova e o que não prova

Prova:
- A movimentação de dados (S3 → Atlas → S3) deixa de ser o gargalo: carga e exportação de 30M linhas em minutos.
- É possível exportar **um único arquivo Parquet** com 30M linhas, sem append e sem pós-processamento.
- O agrupamento por cliente cabe com folga no modelo de documentos (maior documento: 8,7 KB; limite: 16 MB).
- A saída preserva os dados de entrada, verificado linha a linha.

Ainda não prova (ressalvas):
- **Dados sintéticos**: mesmo layout, tipos e volume, mas a distribuição real (contratos por cliente, nulos, cardinalidade)
  pode ser diferente e afetar o agrupamento e o tamanho dos documentos.
- **Arrasto simulado**: a regra em `02_group_arrasto/rules.py` usa faixas de dias de atraso. A regra real do motor de
  rating precisa ser implementada.
- **Regras de negócio da importação não reproduzidas**: a carga apenas copia os dados; validações e transformações por
  linha do processo atual precisam virar estágios de aggregation e entrar no tempo medido.
- **Ambiente fixo**: Atlas M40 em us-east-1, sem tuning. Outro tier, outra região ou outra rede mudam os tempos.
- **Schema**: 4 colunas `REQUIRED` na entrada saem como `OPTIONAL` (ver [Validação da saída](#validação-da-saída)).

## Estrutura

```
README.md
requirements.txt
config.env.example        # variáveis de ambiente (ver "Configuração"); copiar para config.env
common/
  config.py               # leitura de config.env
  bench.py                # benchmark: wall time, ambiente, results/*.json + results/summary.csv, $collStats
generator/
  schema.py               # definição única de colunas, tipos, perfis, catálogo de produtos e distribuições
  generate_input.py       # gerador (DuckDB)
sql/
  perfil_parquet.sql      # script de perfil (o mesmo que roda sobre o arquivo real, sem expor dados)
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
03_export/
  export_common.py        # layout de saída (15 + 2 colunas), $replaceWith tipado, $out S3, background + $queryHistory
  export_adf.py           # 03a / 03b / 03b-multi / ensaio via Data Federation
  export_pymongoarrow.py  # 03c: PyMongoArrow + pyarrow.ParquetWriter (rodar numa EC2 na região do cluster)
scripts/
  validate_input.py       # validação do Parquet de entrada (DuckDB) + execução de sql/perfil_parquet.sql
  validate_output.py      # validação do Parquet exportado (DuckDB + pyarrow, diff de schema, linha a linha)
  upload_s3.sh            # upload multipart (AWS CLI; fallback boto3 em upload_s3.py)
  upload_s3.py
  reconcile_query_history.py  # substitui tempos estimados (polling) pelos exatos do $queryHistory
  make_summary.py         # results/summary.csv + results/*.json -> results/summary.md
results/                  # perfil_sintetico.txt, calibracao.txt, type_mapping.md, summary.csv, summary.md, *.json
data/                     # arquivos gerados (ignorado pelo git)
```

## Pré-requisitos

- **Python 3.11+** e, para o upload, **AWS CLI** (há fallback em boto3).
- **Bucket S3** na mesma região do cluster, para entrada e saída.
- **Cluster MongoDB Atlas** na AWS. A POC usou **M40**; use o mesmo tier para comparar resultados.
- **Atlas Federated Database Instance** com acesso ao bucket e ao cluster.
- **IAM role** autorizada no Atlas (Cloud Provider Access), com leitura e escrita no bucket.
- Usuário de banco com permissão de leitura e escrita no `TARGET_DB` e acesso ao Federated DB.
- Para o gerador: máquina com ~16 GB de RAM (a geração completa teve pico de ~4,5 GB com `--memory-limit 8GB`).

Passo a passo da infraestrutura em [00_setup/README.md](00_setup/README.md).

## Configuração

```bash
uv python install 3.12
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt
source .venv/bin/activate
cp config.env.example config.env   # preencher; nunca commitar
```

| Variável | Descrição |
|---|---|
| `MONGODB_URI` | connection string do cluster Atlas |
| `ADF_URI` | connection string do Federated Database Instance |
| `ATLAS_PROJECT_ID` | ID do projeto Atlas |
| `ATLAS_CLUSTER` | nome do cluster (usado no storage config do ADF) |
| `S3_BUCKET` | bucket de entrada e saída |
| `AWS_REGION` | região do bucket e do cluster |
| `TARGET_DB` | database de destino no cluster (padrão `ratings_poc`) |

Boas práticas:
- `config.env` e `data/` estão fora do controle de versão. Não commite credenciais, nomes de buckets ou arquivos de dados.
- As credenciais AWS vêm do ambiente ou de um profile (`AWS_PROFILE`); nada fica hardcoded.

## Execução

Todos os scripts aceitam `--runs N` (repetir; use 3 e a mediana), `--drop-target` (recriar a coleção de destino a cada
execução) e `--manifest`.

> As cargas completas levam vários minutos e o cliente fica bloqueado aguardando o `$out`. Em notebooks, rode com
> `caffeinate -i python ...` (macOS) para impedir o sleep. Se a conexão cair, o `$out` continua no servidor e o script
> passa a medir por polling da coleção de destino, marcando o tempo como *estimado* em `results/`. Depois, rode
> `scripts/reconcile_query_history.py` para substituir pelo tempo exato.

Ordem recomendada: gerar e enviar a entrada ([Gerador](#gerador--comandos)) → teste de fumaça com 1M linhas → volume
completo → validação → resumo.

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

# etapa 03 — export (AWS_PROFILE só para o validador baixar o arquivo)
export AWS_PROFILE=<profile>
python 03_export/export_adf.py --scenario ensaio                             # 1M
python 03_export/export_adf.py --scenario 03a --runs 3                       # plano, 15 colunas
python 03_export/export_adf.py --scenario 03b --runs 3                       # agrupado + arrasto, 17 colunas (principal)
python 03_export/export_adf.py --scenario 03b-multi                          # maxFileSize padrão -> N arquivos
python scripts/reconcile_query_history.py                                    # tempos exatos para execuções antigas
python scripts/make_summary.py
```

Detalhes em [01_load/README.md](01_load/README.md), [02_group_arrasto/README.md](02_group_arrasto/README.md) e
[03_export/README.md](03_export/README.md).

Saídas: `results/summary.md` (tabela final + observações), `results/type_mapping.md` (tipos BSON por coluna),
`results/exemplo_cliente_misto_clientes_agrupados.json`, `results/explain_02a_*.json`.

## Como os tempos são medidos

- As operações longas (`$out` no Data Federation) rodam em background. O tempo oficial vem do `$queryHistory`
  (`startTime`, `endTime`, `status`), ou seja, é medido no servidor e não depende da rede de quem executa o script.
- Se o `$queryHistory` não estiver disponível para uma execução, o tempo é medido por polling e marcado como *estimado*.
- Os cenários principais foram executados 3 vezes; o resultado reportado é a mediana.
- `common/bench.py` registra também o ambiente (tier, versão, região) e estatísticas das coleções (`$collStats`)
  em `results/*.json`.

## Validação da saída

`scripts/validate_output.py` roda 19 verificações em cada exportação, entre elas:

- contagem exata de linhas e quantidade de arquivos gerados (e ausência de arquivos de erro);
- unicidade de `chave_sequencial` e `contrato`;
- somas de `saldo` e `saldo_limite` e contagem de nulos de `vr_renegociado` iguais às da entrada;
- **comparação linha a linha**: hash das 15 colunas originais por `chave_sequencial`, nos dois sentidos;
- `rating_final` igual ao pior `rating_contrato` do cliente;
- tipos físicos e lógicos iguais aos da entrada e ausência de colunas polimórficas ou aninhadas;
- leitura do arquivo com DuckDB e pyarrow.

Teste negativo: um arquivo adulterado de propósito (1 linha removida e 1 centavo alterado) foi detectado pelo validador.

Diferenças de schema conhecidas:
- Diferenças só de anotação (por exemplo, `INT64` sem a anotação `INT_64`; strings com `LogicalType String`), que não
  mudam a leitura.
- `data_referencia`, `co_cpf_cnpj`, `ctr_elegivel` e `flag_obito` saem como `OPTIONAL` (na entrada são `REQUIRED`).
  O `$out` do Data Federation não controla a nulabilidade. Se algum consumidor validar isso, as alternativas são o
  cenário 03c (PyMongoArrow com schema explícito) ou reescrever apenas os metadados do arquivo.

## Resultados detalhados

| Cenário | Tempo | Observação |
|---|---|---|
| 01 single (arquivo único) | 369 s | ~81 mil documentos/s |
| Índice `co_cpf_cnpj` | 68 s | 426 MB; necessário só se o uso exigir |
| 01 split (8 arquivos) | 367 s | igual ao arquivo único: o limite é a escrita no cluster |
| 02a agrupamento + arrasto no cluster | 331 s | 12.034.569 documentos; 19,2% dos clientes com rating alterado |
| 02b agrupado direto do S3 (etapas 1+2 no ADF) | ~15 min (899 s) | mais lento que 01 + 02a (~11,7 min) |
| 03a plano → 1 arquivo (15 colunas) | 6,4 min | 1,11 GB |
| 03b agrupado → 1 arquivo (17 colunas) | 7,6 min (mediana de 3) | 868 MB, 43 row groups; cenário principal |
| 03b-multi (`maxFileSize` padrão) | 6,2 min | 54 arquivos |

- Armazenamento no cluster: modelo agrupado ~2,1 GB contra ~2,2 GB no modelo plano.
- Maior documento em `clientes_agrupados`: 8,7 KB.
- A saída agrupada sai ordenada por cliente, o que melhora a compressão: 868 MB contra 1,06 GB da entrada, mesmo com
  2 colunas a mais.
- O arquivo único custa ~1 min a mais que a saída em 54 arquivos, porque com N arquivos a escrita é paralela.
- 03c (PyMongoArrow) está implementado, mas não foi medido com o volume completo.

## Rodar com o arquivo real

O objetivo é levar o código até os dados: o arquivo real não sai da conta AWS do cliente, e só os relatórios de tempo e
validação são compartilhados.

1. **Perfil do arquivo real**: rodar `sql/perfil_parquet.sql` (DuckDB) sobre o arquivo real. O script devolve apenas
   estatísticas agregadas (tipos, nulos, distribuições), sem dados de clientes.
2. **Ajustar o gerador (opcional)**: com o perfil, preencher `PROFILE_REAL` em `generator/schema.py` e, se necessário,
   `DEFAULT_CONTRACTS_DIST` e `PRODUCTS`. Isso permite gerar dados sintéticos mais fiéis fora do ambiente do cliente.
3. **Infraestrutura**: cluster Atlas (M40 para comparar) na região do bucket, Federated DB e IAM role, conforme
   [00_setup/README.md](00_setup/README.md).
4. **Apontar para o arquivo real**: colocar o arquivo no prefixo de entrada esperado pela coleção virtual
   `dbimport.s3import` (ou ajustar o caminho em `00_setup/adf_storage_config.json`) e preencher o `config.env`.
5. **Regra real de arrasto**: substituir a simulação em `02_group_arrasto/rules.py` pela regra real. Regras de negócio da
   importação também entram como estágios de pipeline.
6. **Executar** 01 → 02a → 03b, com `--runs 3`.
7. **Validar** com `scripts/validate_output.py`.
   > A validação de entrada (`validate_input.py`) e os checksums usam o `manifest.json` gerado pelo gerador. Para o
   > arquivo real não há manifest. [A definir: gerar um manifest equivalente a partir do arquivo real ou rodar a
   > validação comparando diretamente entrada e saída.]
8. **Compartilhar** `results/summary.md`, que contém apenas tempos e métricas.

Informações úteis para levantar antes do teste: tempo atual medido de cada etapa, colunas adicionais do arquivo de
saída e requisitos de schema dos consumidores (por exemplo, colunas `REQUIRED`).

## Limpeza e custos

Os recursos da POC geram custo enquanto existirem. Ao terminar:
- pausar ou remover o cluster Atlas;
- remover o Federated Database Instance, se não for reutilizado;
- apagar os arquivos de entrada e saída do bucket (os Parquet de saída têm ~0,9–1,1 GB cada execução);
- revogar a IAM role, se ela foi criada só para a POC.

O Data Federation cobra pelo volume de dados processado; consulte a página de preços do Atlas para estimar o custo de
cada execução.

## Problemas comuns

- **Conexão caiu durante um `$out`**: a operação continua no servidor. Aguarde o fim e rode
  `scripts/reconcile_query_history.py` para registrar o tempo exato.
- **Coleção virtual vazia ou ausente** no `load_flat.py --check`: conferir o storage config (bucket, prefixo, região) e as
  permissões da IAM role.
- **Saída em vários arquivos em vez de um**: conferir o `maxFileSize` no `$out` (ver `03_export/export_common.py`). O
  limite é medido sobre o BSON descomprimido, que é bem maior que o Parquet final.
- **Tempos muito diferentes dos da POC**: conferir tier, região do cluster e do bucket, e se outra carga estava rodando no
  cluster ao mesmo tempo.

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
  com 8211/203/C5 da amostra de referência). Produtos com limite somam ~20% das linhas → `saldo_limite > 0` em ~20%.
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

`real` é um placeholder (hoje igual a `mixed`): quando houver o resultado de `perfil_parquet.sql` sobre o arquivo real,
preencha `PROFILE_REAL` em `schema.py` (tipos por coluna e `empty_vr_renegociado`) e, se necessário, ajuste
`DEFAULT_CONTRACTS_DIST`, `PRODUCTS` e as constantes de distribuição. O gerador e a validação não precisam mudar.

## Calibração e tamanho

Referência: 30.076.105 linhas, ~1,10 GB → ~36,6 bytes/linha. Resultado de `--calibrate` (`results/calibracao.txt`):

| perfil | bytes/linha | projeção 30M | vs alvo |
|---|---|---|---|
| typed | 31,74 | 0,955 GB | −13,2% |
| strings | 40,86 | 1,229 GB | +11,7% |
| **mixed** | **35,19** | **1,058 GB** | **−3,8%** (dentro de ±5%) |

Na calibração a `chave_sequencial` é escalada para a magnitude de `--rows`, porque é a única coluna cujo tamanho
cresce com N (bit-packing de 1..N); sem isso a projeção a partir de 2M linhas subestimava ~0,6 byte/linha.
O arquivo completo real ficou em **35,22 bytes/linha (1,059 GB, −3,7%)**, confirmando a projeção.

## Saídas do gerador

- `data/ratings_input_synthetic.parquet`
- `data/manifest.json`: seed, perfil, linhas, clientes, bytes, row groups, tempo, schema Parquet, histograma de
  contratos por cliente, % de clientes mistos, contagens por carteira e checksums `sum(saldo)` / `sum(saldo_limite)`
  (somados como `DECIMAL(18,2)`, exatos) — base de comparação para as etapas 01–03.
- `results/perfil_sintetico.txt`: saída de `sql/perfil_parquet.sql` sobre o arquivo sintético, para comparar com o perfil
  do arquivo real.

## Validação da entrada (`scripts/validate_input.py`)

Verifica: contagem; `chave_sequencial` permutação de 1..rows; `contrato` único e com 7–12 dígitos; nomes/ordem das
colunas; nulos só em `vr_renegociado`; histograma, clientes mistos, carteiras e checksums iguais ao manifest;
`flag_obito` constante por cliente; `qt_dias_atraso_acumulado >= qt_dias_atraso`; documentos com 11/14 dígitos e
nenhum válido pelo DV; tamanho dentro de ±5% de 1,10 GB (alerta apenas). Sai com código ≠ 0 se houver falha.

### Resultados do gerador (Apple Silicon, 18 threads, `--memory-limit 8GB`)

| Etapa | Resultado |
|---|---|
| `--calibrate` | recomenda `mixed` (−3,8%) — `results/calibracao.txt` |
| Amostra 1M | 0,37 s, 34,5 MB, 398.499 clientes, 0 falhas — `results/amostra_1m_validacao.txt` |
| Completo 30.076.105 | **11,4 s**, 1.059.319.819 bytes (1,059 GB, 35,22 B/linha), 29 row groups, 12.034.569 clientes, pico RSS 4,5 GB — `results/geracao_completa.txt` |
| Validação completa | 4 s, 20 checks PASS, 0 alertas — `results/validacao_completa.txt`, `results/perfil_sintetico.txt` |

## Próximos passos

1. **Arquivo real**: repetir a POC no ambiente do cliente, sem mudar o código, e comparar com os tempos atuais medidos.
2. **Regras reais**: implementar as regras da importação e o arrasto real em aggregation pipeline, validando o resultado
   contra o processo atual.
3. **Otimização e sizing**: agrupar e exportar num pipeline só (sem gravar a coleção intermediária), testar tiers acima
   do M40 (a carga é limitada pela escrita), medir o 03c com volume completo e estimar custo.
4. **Integração**: encaixar o fluxo na orquestração existente e definir o plano de adoção.

## Aviso

Este repositório é código de POC, criado para medir desempenho e demonstrar viabilidade. Não é um produto oficial do
MongoDB nem está pronto para produção: revise segurança, tratamento de erros, observabilidade e custos antes de qualquer
uso em produção.
