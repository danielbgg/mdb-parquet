# 03_export (placeholder)

(a) coleção plana → `$out` S3 parquet; (b) coleção agrupada → `$unwind` + `$replaceWith` (ordem das colunas) + `$out` S3
parquet com `maxFileSize: "16GiB"` e `errorMode: "stop"`; (c) PyMongoArrow + `pyarrow.ParquetWriter` em um único arquivo.

Validar com DuckDB: contagem, schema, checksums (`data/manifest.json`) e quantidade de arquivos = 1.
