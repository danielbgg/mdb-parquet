# 03_export — 30 M linhas → um único arquivo Parquet

Foco principal da POC. Hoje o cliente leva ~40 min e gera ~200 arquivos porque a ferramenta atual não faz append.
Aqui o Atlas Data Federation escreve diretamente no S3 com `$out` em Parquet, e o `maxFileSize` alto força **um único
arquivo**.

> **Simulação.** `rating_contrato` e `rating_final` vêm das faixas de [`02_group_arrasto/rules.py`](../02_group_arrasto/rules.py),
> uma simulação inspirada na Res. 2682 — não é a regra real do cliente.

## Layout do arquivo de saída

As 15 colunas do Parquet de entrada (mesmos nomes, ordem e tipos) seguidas das colunas do arrasto:

```
data_referencia, chave_sequencial, co_cpf_cnpj, contrato, qt_dias_atraso, saldo, saldo_limite, ctr_elegivel,
carteira, cod_produto, flag_prejuizo_2682, flag_obito, qt_dias_atraso_acumulado, modalidade, vr_renegociado,
rating_contrato, rating_final
```

- Sem `_id`. O `$replaceWith` com objeto literal ([`export_common.py`](export_common.py)) fixa a ordem.
- Cada coluna é convertida explicitamente para o tipo BSON de `results/type_mapping.md` (`$toLong`, `$toInt`,
  `$toDouble`, `$toString`) — nenhuma coluna pode sair polimórfica (o ADF geraria um *group* com subcolunas).
- `vr_renegociado`: `$ifNull(..., null)` → o campo existe em todos os documentos (no plano o Parquet NULL chega como
  campo ausente; no agrupado já é `null` explícito).

## Medição do tempo (resolvido)

Na etapa 01 a conexão com o ADF caía em operações > 5 min. Aqui:

1. O `aggregate` é disparado com **`background: true`** via `db.command("aggregate", coll, pipeline=..., cursor={},
   background=True)` (pymongo; não usar mongosh — MONGOSH-1149 descarta a opção). O ADF responde em ~0,3 s e segue
   executando.
2. O script faz polling (2 s) do **`$queryHistory`** (`admin` db), que registra `startTime`, `endTime`, `ok`, `error`
   e `background` de cada query do ADF. A duração oficial é `endTime − startTime` (medida no servidor, exata).
3. Fallback (nunca acionado nos testes): se o histórico não mostrar o fim, o tempo vem da estabilização do tamanho dos
   objetos no S3 (`list_objects_v2` a cada 2 s) e é marcado como estimado.

Com o `$queryHistory` também foram recuperados os tempos exatos das cargas 01 e 02b (ver `results/summary.md`).

## Cenários

| | Origem (ADF → coleção do cluster) | Pipeline | `$out` S3 |
|---|---|---|---|
| **03a** | `contratos_flat` | `$replaceWith` (15 colunas) | parquet, `maxFileSize: "20GiB"`, snappy, `errorMode: "stop"` |
| **03b** (principal) | `clientes_agrupados` | `$unwind contratos` → `$replaceWith` (17 colunas) | idem |
| 03b-multi | `clientes_agrupados` | idem | `maxFileSize` padrão (200 MiB) → vários arquivos |
| ensaio | `clientes_agrupados_1m` | idem 03b | idem 03b |
| 03c | `clientes_agrupados` (cluster, pymongo) | idem, no cliente | PyMongoArrow + `pyarrow.ParquetWriter` → 1 arquivo local → upload |

Cada execução grava em `s3://$S3_BUCKET/output/<cenário>/<timestamp UTC>/ratings_export.1.parquet` (o ADF acrescenta
`.N.parquet`), nunca sobrescrevendo outra. `maxRowGroupSize` fica no padrão do ADF (128 MiB → ~43 row groups de
~700 k linhas em 30 M).

```bash
export AWS_PROFILE=<profile>                       # para o validador baixar o arquivo do S3
python 03_export/export_adf.py --scenario ensaio   # 1M, valida sintaxe/tipos
python 03_export/export_adf.py --scenario 03a --runs 3
python 03_export/export_adf.py --scenario 03b --runs 3
python 03_export/export_adf.py --scenario 03b-multi
```

Após cada execução o script chama `scripts/validate_output.py` e grava `results/03_export_<cenário>_run<N>_<ts>.json`
(+ `summary.csv`) com duração, fonte do tempo, arquivos/tamanho e o resultado da validação.

## Validação (`scripts/validate_output.py`, DuckDB + pyarrow)

Baixa o prefixo para `data/output/<cenário>/<ts>/` e verifica: 1 arquivo (e nenhum arquivo de erro); `count ==
30.076.105`; `chave_sequencial` única cobrindo 1..N; `contrato` único; nomes/ordem das colunas; nenhuma coluna *group*;
schema das 15 primeiras colunas igual ao da entrada (diff de `parquet_schema`, com tabela); checksums `sum(saldo)` /
`sum(saldo_limite)`; nulos de `vr_renegociado` iguais à entrada e só nela; **igualdade linha a linha** —
`(chave_sequencial, hash(15 colunas))` da saída `EXCEPT` entrada nos dois sentidos deve dar 0 (comparação exata por
chave, ~16 B/linha; testado com um arquivo adulterado: detecta 1 linha removida e 1 centavo alterado); só no 03b:
`rating_final` constante por cliente e igual ao pior `rating_contrato`, % de linhas alteradas pelo arrasto; tamanho,
row groups e compressão; leitura completa com `pyarrow.parquet`.

### Diff de schema entrada × saída

O tipo **normalizado** (físico + lógico) das 15 colunas é idêntico. Há diferenças de *anotação* e *nullability*,
reportadas como WARN e listadas no relatório:

| Diferença | Entrada (DuckDB) | Saída (ADF) | Efeito |
|---|---|---|---|
| inteiros | `INT64` + `ConvertedType INT_64` | `INT64` sem anotação | nenhum — `INT_64` é a anotação legada do tipo padrão |
| strings | `BYTE_ARRAY` + `UTF8` | `BYTE_ARRAY` + `UTF8` + `LogicalType String` | nenhum — `UTF8 ≡ STRING` |
| repetition | `REQUIRED` em 4 colunas sem NULL | `OPTIONAL` em todas | nullability: o ADF marca tudo como opcional; os dados não mudam |

Nenhum desses pontos é controlável pelos parâmetros do `$out`.

## 03c — PyMongoArrow numa EC2

[`export_pymongoarrow.py`](export_pymongoarrow.py) lê `clientes_agrupados` com o mesmo pipeline (`aggregate_raw_batches`
+ `PyMongoArrowContext`, lotes de 50 k docs), converte direto para Arrow e grava row groups de 1.048.576 linhas num
único arquivo com `pyarrow.ParquetWriter`; depois faz upload multipart. Não rodar do notebook: ~10 GB de BSON
atravessam a rede.

Instância recomendada: **c6i.2xlarge** (8 vCPU, 16 GB, até 12,5 Gbps) em **us-east-1**, mesma VPC/peering ou IP
liberado na access list do Atlas; disco gp3 de 20 GB. Para dar mais margem à rede e usar paralelismo, c6in.2xlarge
(até 40 Gbps). Setup:

```bash
sudo dnf install -y git python3.12 && git clone <repo> && cd ratings-poc
python3.12 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
cp config.env.example config.env   # MONGODB_URI etc.
python 03_export/export_pymongoarrow.py --out /data/ratings_export.1.parquet \
    --s3-uri s3://$S3_BUCKET/output/03c/$(date -u +%Y%m%dT%H%M%SZ)/ratings_export.1.parquet
python scripts/validate_output.py --s3-prefix s3://$S3_BUCKET/output/03c/<ts>/ --grouped
```

O gargalo esperado é o cursor único (`$unwind` no servidor + decodificação BSON em 1 thread). Se precisar de mais
vazão, particionar por faixa de `_id` em N cursores paralelos e escrever cada partição em row groups do mesmo
`ParquetWriter` (serializado) é a evolução natural.
