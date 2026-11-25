-- Uso: duckdb -c ".read perfil_parquet.sql"   (ajuste o caminho abaixo)
-- Substitua ARQUIVO.parquet pelo caminho do arquivo real

-- 1) Schema físico/lógico (tipos reais de cada coluna)
SELECT name, type, converted_type, logical_type, scale, precision
FROM parquet_schema('ARQUIVO.parquet');

-- 2) Layout do arquivo: nº de row groups, linhas por row group, compressão
SELECT count(DISTINCT row_group_id) AS row_groups,
       max(row_group_num_rows)      AS max_rows_por_rg,
       any_value(compression)       AS compressao,
       sum(total_compressed_size)/1e6 AS mb_comprimido
FROM parquet_metadata('ARQUIVO.parquet');

-- 3) Tamanho por coluna (quais colunas pesam mais)
SELECT path_in_schema AS coluna, round(sum(total_compressed_size)/1e6,1) AS mb
FROM parquet_metadata('ARQUIVO.parquet') GROUP BY 1 ORDER BY 2 DESC;

-- 4) Perfil estatístico agregado (sem valores individuais)
SUMMARIZE SELECT * EXCLUDE (co_cpf_cnpj, contrato, chave_sequencial) FROM read_parquet('ARQUIVO.parquet');

-- 5) Cardinalidades das chaves (apenas contagens)
SELECT count(*) AS linhas,
       approx_count_distinct(co_cpf_cnpj) AS clientes,
       approx_count_distinct(contrato)    AS contratos,
       approx_count_distinct(chave_sequencial) AS chaves
FROM read_parquet('ARQUIVO.parquet');

-- 6) Distribuição de contratos por cliente (define o modelo agrupado)
SELECT CASE WHEN n>=20 THEN '20+' ELSE n::VARCHAR END AS contratos_por_cliente,
       count(*) AS clientes
FROM (SELECT co_cpf_cnpj, count(*) n FROM read_parquet('ARQUIVO.parquet') GROUP BY 1)
GROUP BY 1 ORDER BY min(n);
