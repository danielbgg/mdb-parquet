# 00_setup — Federated Database Instance, IAM role e bucket

Esta pasta contém apenas **documentação e templates**. Nenhum script aqui cria, altera ou destrói recursos no Atlas
ou na AWS; os comandos abaixo devem ser executados por quem administra a conta.

## Pré-requisitos

- Cluster Atlas (M40, AWS) no projeto `ATLAS_PROJECT_ID`, nome `ATLAS_CLUSTER`.
- Bucket S3 `S3_BUCKET` na região `AWS_REGION`, com a estrutura:

```
s3://$S3_BUCKET/
  input/ratings_input_synthetic.parquet        # arquivo completo (30M linhas)
  input_split/*.parquet                        # opcional: saída de --split N
  input_1m/sample_1m.parquet                   # amostra de 1M para teste de fumaça
```

Upload a partir do repositório:

```bash
scripts/upload_s3.sh --s3-uri s3://$S3_BUCKET/input/ratings_input_synthetic.parquet
scripts/upload_s3.sh --file data/sample_1m.parquet --s3-uri s3://$S3_BUCKET/input_1m/sample_1m.parquet
# opcional
python generator/generate_input.py --split 8      # gera data/split/*.parquet
aws s3 sync data/split s3://$S3_BUCKET/input_split/
```

## 1. IAM role para o Atlas acessar o bucket (Unified AWS Access)

1. Atlas UI → **Project Settings → Integrations → AWS IAM Role Access → Authorize an AWS IAM Role**
   (ou `atlas cloudProviders accessRoles aws create --projectId $ATLAS_PROJECT_ID`).
   O Atlas devolve `atlasAWSAccountArn` e `atlasAssumedRoleExternalId`.
2. Na AWS, crie a role com a *trust policy* fornecida pelo Atlas:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "<atlasAWSAccountArn>" },
    "Action": "sts:AssumeRole",
    "Condition": { "StringEquals": { "sts:ExternalId": "<atlasAssumedRoleExternalId>" } }
  }]
}
```

3. Anexe à role a política de acesso ao bucket (leitura para a carga, escrita para a exportação da etapa 03):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::<S3_BUCKET>"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::<S3_BUCKET>/*"
    }
  ]
}
```

4. Volte ao Atlas e conclua a autorização informando o ARN da role
   (`atlas cloudProviders accessRoles aws authorize <roleId> --iamAssumedRoleArn <arn>`).

## 2. Federated Database Instance

1. Atlas UI → **Data Federation → Create New Federated Database → Configure manually**
   (ou `atlas datafederation create <NOME> --projectId $ATLAS_PROJECT_ID --region <REGIÃO_ADF>`).
   Escolha a região do ADF mais próxima do bucket e do cluster.
2. Na aba **JSON Editor**, cole a *storage configuration* renderizada a partir do template:

```bash
python 00_setup/render_storage_config.py          # lê config.env → data/adf_storage_config.rendered.json
# ou aplique via CLI:
atlas datafederation update <NOME> --projectId $ATLAS_PROJECT_ID --file data/adf_storage_config.rendered.json
```

   O template [`adf_storage_config.json`](adf_storage_config.json) define:

   | Objeto | Conteúdo |
   |---|---|
   | store `s3store` | provider `s3`, bucket `${S3_BUCKET}`, região `${AWS_REGION}`, delimiter `/` |
   | store `atlasstore` | provider `atlas`, cluster `${ATLAS_CLUSTER}`, projeto `${ATLAS_PROJECT_ID}` |
   | `dbimport.s3import` | `/input/*.parquet` — 1 arquivo (cenário *single*) |
   | `dbimport.s3import_split` | `/input_split/*.parquet` — N arquivos (cenário *split*) |
   | `dbimport.s3import_1m` | `/input_1m/*.parquet` — teste de fumaça |
   | `${TARGET_DB}.*` | espelho das coleções do cluster, necessário para a etapa 03 (`$out` para S3 a partir do ADF) |

   Sintaxe conforme a documentação *Configuration File Formats for Data Stores* e *$out Stage (Atlas Data Federation)*.
   O `path` é relativo ao `prefix` do store; `defaultFormat: ".parquet"` só é usado se algum arquivo vier sem extensão.

3. Em **Cloud Provider → AWS IAM Role**, selecione a role autorizada no passo 1 e valide o acesso ao bucket.
4. Crie um usuário de banco (ou use o existente) com acesso ao Federated DB e ao cluster. A connection string do ADF
   (`Connect → Connect your application`) vai para `ADF_URI`; a do cluster para `MONGODB_URI`.

## 3. config.env

```bash
cp config.env.example config.env
# preencha MONGODB_URI, ADF_URI, ATLAS_PROJECT_ID, ATLAS_CLUSTER, S3_BUCKET, AWS_REGION, TARGET_DB
```

`config.env` está no `.gitignore`. Os scripts leem o arquivo via [`common/config.py`](../common/config.py); variáveis de
ambiente com o mesmo nome têm precedência.

## 4. Verificação rápida (sem escrever nada)

```bash
python 01_load/load_flat.py --check   # lista as coleções virtuais do ADF, conta os arquivos de cada uma e pinga o cluster
```
