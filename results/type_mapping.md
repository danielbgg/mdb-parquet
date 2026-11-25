# Mapeamento de tipos Parquet → BSON (coleção plana)

- Coleção: `ratings_poc.contratos_flat`
- Gerado em: 2026-11-25T15:24:02+00:00
- Manifest: `data/ratings_input_synthetic.parquet` (perfil `mixed`, seed 42)
- MongoDB: 9.0.7 (M40, us-east-1); ADF: 8.0.32
- Amostra: 100,000 documentos (`$sample` + `$type`)

| # | Coluna | Tipo Parquet (físico / lógico) | Tipo(s) BSON na amostra |
|---|---|---|---|
| 1 | `data_referencia` | BYTE_ARRAY / UTF8 | `string` 100.0% |
| 2 | `chave_sequencial` | INT64 / INT_64 | `long` 100.0% |
| 3 | `co_cpf_cnpj` | BYTE_ARRAY / UTF8 | `string` 100.0% |
| 4 | `contrato` | INT64 / INT_64 | `long` 100.0% |
| 5 | `qt_dias_atraso` | INT32 / INT_32 | `int` 100.0% |
| 6 | `saldo` | DOUBLE | `double` 100.0% |
| 7 | `saldo_limite` | DOUBLE | `double` 100.0% |
| 8 | `ctr_elegivel` | BYTE_ARRAY / UTF8 | `string` 100.0% |
| 9 | `carteira` | BYTE_ARRAY / UTF8 | `string` 100.0% |
| 10 | `cod_produto` | INT32 / INT_32 | `int` 100.0% |
| 11 | `flag_prejuizo_2682` | INT32 / INT_32 | `int` 100.0% |
| 12 | `flag_obito` | INT32 / INT_32 | `int` 100.0% |
| 13 | `qt_dias_atraso_acumulado` | INT32 / INT_32 | `int` 100.0% |
| 14 | `modalidade` | INT32 / INT_32 | `int` 100.0% |
| 15 | `vr_renegociado` | BYTE_ARRAY / UTF8 | `missing` 95.0%, `string` 5.0% |

## Observações

- `_id`: gerado pelo ADF/cluster como `ObjectId` (não existe no Parquet).
- `vr_renegociado` vazio chega como **missing (campo ausente)**.
- Ordem dos campos preservada (igual ao header do Parquet): **sim**.
  Ordem observada (1º documento): `_id, data_referencia, chave_sequencial, co_cpf_cnpj, contrato, qt_dias_atraso, saldo, saldo_limite, ctr_elegivel, carteira, cod_produto, flag_prejuizo_2682, flag_obito, qt_dias_atraso_acumulado, modalidade`
- Documento com `vr_renegociado` presente — 15 campos na ordem do header: **sim**.

Implicações para o export (03): o `$replaceWith`/projeção final deve emitir as 15 colunas nesta ordem e com estes
tipos BSON para que o Parquet de saída reproduza o schema de entrada; `vr_renegociado` precisa ser escrito
explicitamente (`$ifNull`) se o valor vazio chegar como campo ausente.
