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
