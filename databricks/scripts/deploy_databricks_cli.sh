#!/usr/bin/env bash
set -euo pipefail

# env variables expected:
# DATABRICKS_HOST, DATABRICKS_TOKEN, ENV=dev, CLUSTER_ID
export ENV=${ENV:-dev}

# import notebook directory to workspace
databricks workspace import-dir --overwrite ../notebooks "/Shared/${ENV}"

# create/ensure secret scope is present (backed by Key Vault)
databricks secrets create-scope \
  --scope kv-dev-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "${AKV_RESOURCE_ID}" \
  --dns-name "${AKV_DNS_NAME}" || echo "scope may already exist"

# deploy job (create or reset)
JOB_JSON="../jobs/dev-job.json"
# provide cluster id substitution
TMP_JOB="/tmp/job-${ENV}.json"
jq --arg cluster "${CLUSTER_ID}" '.tasks[0].existing_cluster_id = $cluster' $JOB_JSON > $TMP_JOB

# try create, if exists then reset
JOB_ID=$(databricks jobs create --json-file $TMP_JOB 2>/dev/null | jq -r '.job_id' || true)
if [ -z "$JOB_ID" ]; then
  # find existing job id by name and reset
  JOB_ID=$(databricks jobs list --output JSON | jq -r '.[] | select(.settings.name=="dev_patient_ingest") | .job_id' )
  if [ -n "$JOB_ID" ]; then
    databricks jobs reset --job-id "$JOB_ID" --json-file $TMP_JOB
  else
    databricks jobs create --json-file $TMP_JOB
  fi
else
  echo "Job created with id $JOB_ID"
fi
