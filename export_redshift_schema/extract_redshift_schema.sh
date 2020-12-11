#!/bin/bash

set -eoux pipefail

REGION="$1"
DB_NAME="$2"
DB_USER="$3"
CLUSTER_IDENTIFIER="$4"
ENDPOINT="$5"

if [ -z "$REGION" ] || [ -z "$DB_NAME" ] || [ -z "$DB_USER" ] || [ -z "$CLUSTER_IDENTIFIER" ] || [ -z "$ENDPOINT" ]; then
  echo "usage: ./extract_redshift_schema [region] [db_name] [db_user] [cluster_identifier] [endpoint]"
  exit
fi

python scripts/redshift/extract_schema/extract_redshift_schema.py "$REGION" "$DB_NAME" "$DB_USER" "$CLUSTER_IDENTIFIER" "$ENDPOINT"
