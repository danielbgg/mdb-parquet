#!/usr/bin/env bash
# Upload a local file to S3 with multipart upload.
# Credentials come from the environment / AWS profile (never hardcoded).
#
# Usage:
#   scripts/upload_s3.sh --s3-uri s3://bucket/path/file.parquet [--file data/ratings_input_synthetic.parquet]
#                        [--profile aws-profile] [--region us-east-1] [--chunk-mb 64]
#
# Uses the AWS CLI when available (multipart is automatic above 8 MB);
# otherwise falls back to scripts/upload_s3.py (boto3 TransferConfig).
set -euo pipefail

FILE="data/ratings_input_synthetic.parquet"
S3_URI=""
PROFILE="${AWS_PROFILE:-}"
REGION="${AWS_REGION:-}"
CHUNK_MB=64

while [[ $# -gt 0 ]]; do
  case "$1" in
    --s3-uri)   S3_URI="$2"; shift 2 ;;
    --file)     FILE="$2"; shift 2 ;;
    --profile)  PROFILE="$2"; shift 2 ;;
    --region)   REGION="$2"; shift 2 ;;
    --chunk-mb) CHUNK_MB="$2"; shift 2 ;;
    -h|--help)  sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -z "$S3_URI" ]] && { echo "--s3-uri is required (s3://bucket/key)" >&2; exit 2; }
[[ -f "$FILE" ]] || { echo "file not found: $FILE" >&2; exit 2; }

SIZE_BYTES=$(stat -f%z "$FILE" 2>/dev/null || stat -c%s "$FILE")
echo "Uploading $FILE ($SIZE_BYTES bytes) -> $S3_URI"

if command -v aws >/dev/null 2>&1; then
  ARGS=()
  [[ -n "$PROFILE" ]] && ARGS+=(--profile "$PROFILE")
  [[ -n "$REGION" ]]  && ARGS+=(--region "$REGION")
  # Larger chunks = fewer parts for a ~1 GB file.
  aws "${ARGS[@]}" configure set default.s3.multipart_chunksize "${CHUNK_MB}MB" >/dev/null 2>&1 || true
  aws "${ARGS[@]}" configure set default.s3.multipart_threshold "${CHUNK_MB}MB" >/dev/null 2>&1 || true
  aws "${ARGS[@]}" s3 cp "$FILE" "$S3_URI" --expected-size "$SIZE_BYTES"
else
  PY="${PYTHON:-}"
  if [[ -z "$PY" ]]; then
    if [[ -x ".venv/bin/python" ]]; then PY=".venv/bin/python"; else PY="python3"; fi
  fi
  ARGS=(--file "$FILE" --s3-uri "$S3_URI" --chunk-mb "$CHUNK_MB")
  [[ -n "$PROFILE" ]] && ARGS+=(--profile "$PROFILE")
  [[ -n "$REGION" ]]  && ARGS+=(--region "$REGION")
  "$PY" "$(dirname "$0")/upload_s3.py" "${ARGS[@]}"
fi

echo "Done."
