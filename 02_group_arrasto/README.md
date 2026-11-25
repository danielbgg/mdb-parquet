# 02_group_arrasto — modelo agrupado por cliente + arrasto simulado

> **Simulação.** As faixas de rating em [`rules.py`](rules.py) são inspiradas nos dias de atraso da Res. CMN 2682
> (0–14 A, 15–30 B, 31–60 C, 61–90 D, 91–120 E, 121–150 F, 151–180 G, >180 H) e existem apenas para que o arrasto tenha
> efeito mensurável. **Não são a regra real do cliente.**

## Modelo (`clientes_agrupados`, 1 documento por cliente)

```js
{
  _id: "<co_cpf_cnpj>",
  data_referencia: "01/10/2026 00:00:00",
  qtd_contratos: 3,
  pior_rank: 6,              // 1..8, interno
  rating_final: "F",         // letra do pior rank; só no nível do cliente (o export propaga no $unwind)
  contratos: [               // array BSON nativo, mesmos nomes/tipos da coleção plana
    { chave_sequencial, contrato, qt_dias_atraso, saldo, saldo_limite, ctr_elegivel, carteira, cod_produto,
      flag_prejuizo_2682, flag_obito, qt_dias_atraso_acumulado, modalidade, vr_renegociado, rating_contrato: "A" }
  ]
}
```

- `co_cpf_cnpj` vira `_id` e `data_referencia` sobe para o cliente; as outras 13 colunas ficam em cada item de `contratos`.
- `vr_renegociado` é gravado com `$ifNull` → aparece sempre (`null` quando vazio), mesmo que no plano chegue como campo
  ausente — conforme o modelo acima. O `rank` por contrato não é persistido (só a letra `rating_contrato`).

## Pipeline (único, em `rules.grouping_pipeline()`)

```js
[
  { $set: { rank: { $switch: { branches: [{ case: { $lte: ["$qt_dias_atraso", 14] }, then: 1 }, ...], default: 8 } } } },
  { $set: { rating_contrato: { $arrayElemAt: [["A","B","C","D","E","F","G","H"], { $subtract: ["$rank", 1] }] } } },
  { $group: { _id: "$co_cpf_cnpj", data_referencia: { $first: "$data_referencia" }, qtd_contratos: { $sum: 1 },
              pior_rank: { $max: "$rank" }, contratos: { $push: { chave_sequencial: "$chave_sequencial", ..., rating_contrato: "$rating_contrato" } } } },
  { $replaceWith: { _id: "$_id", data_referencia: "$data_referencia", qtd_contratos: "$qtd_contratos", pior_rank: "$pior_rank",
                    rating_final: { $arrayElemAt: [[...], { $subtract: ["$pior_rank", 1] }] }, contratos: "$contratos" } },
  { $out: ... }
]
```

## Variantes

| | Origem | Onde roda | `$out` | Script |
|---|---|---|---|---|
| **02a** | `contratos_flat` (cluster) | cluster Atlas | `clientes_agrupados` | `group_cluster.py` |
| **02b** | `dbimport.s3import` (Parquet no S3) | Data Federation | `{atlas: {... coll: "clientes_agrupados_adf"}}` | `group_adf.py` |

02b junta as etapas 1 e 2 numa única operação (hipótese da POC: hoje são ~50 min de importação + ~50 min de arrasto no
Postgres).

```bash
# fumaça (1M) — exige 01_load --source 1m antes para o 02a
python 02_group_arrasto/group_cluster.py --source-coll contratos_flat_1m --target clientes_agrupados_1m --manifest data/sample_1m.manifest.json --drop-target
python 02_group_arrasto/group_adf.py     --source 1m --target clientes_agrupados_adf_1m --flat-coll contratos_flat_1m --manifest data/sample_1m.manifest.json --drop-target

# completo
python 02_group_arrasto/group_cluster.py --runs 3 --drop-target
python 02_group_arrasto/group_adf.py     --runs 3 --drop-target          # --source split para N arquivos

# validação avulsa + documento de exemplo
python 02_group_arrasto/validate_grouped.py --coll clientes_agrupados
```

`group_cluster.py` também roda `explain("executionStats")` do pipeline (sem `$out`, com `$limit` de 200k linhas) e
salva em `results/explain_02a_<coll>_<n>.json`; o campo `usedDisk`/`spills` do estágio `$group` diz se houve spill.

## Validação (as duas variantes)

- `countDocuments == customers`; `sum(qtd_contratos) == rows`; `qtd_contratos == size(contratos)`;
- `sum(contratos.saldo)` e `sum(contratos.saldo_limite)` iguais aos checksums (Decimal128, ±0,01);
- para todo documento: `pior_rank == max(rank(contratos.qt_dias_atraso))`, `rating_final == letra(pior_rank)` e
  `rating_contrato == letra(rank(qt_dias_atraso))` em todos os contratos;
- clientes mistos na definição do gerador (min dias = 0 e max > 90) `== customers_mixed` do manifest; e o número de
  clientes em que o arrasto efetivamente altera algum contrato (`min rank != max rank`), que é ≥ o anterior porque
  inclui inadimplentes com faixas diferentes;
- histograma de `qtd_contratos` igual ao do manifest;
- maior documento (`$bsonSize`) bem abaixo de 16 MB (limite do check: 1,6 MB);
- `$collStats` de `clientes_agrupados` comparado com `contratos_flat`.

Um documento de exemplo de cliente misto é salvo em `results/exemplo_cliente_misto_<coleção>.json`.

## Limitações do ADF (02b)

O pipeline usa apenas `$set`, `$switch`, `$arrayElemAt`, `$group` (`$first`, `$sum`, `$max`, `$push`), `$ifNull` e
`$out` para o Atlas — todos listados como suportados pelo Data Federation. Se alguma execução falhar por operador não
suportado ou limite de memória do `$group` no ADF, as alternativas são: (1) `$out` plano via ADF (01) + 02a no cluster;
(2) particionar o `$group` por faixa de `co_cpf_cnpj` em vários `aggregate` ADF com `$match` + `$merge`.
