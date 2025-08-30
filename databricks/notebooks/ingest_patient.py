# ingest_patient.py
# Requires: pyspark on Databricks
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os

spark = SparkSession.builder.getOrCreate()

# Read credentials from secret scope
client_id = dbutils.secrets.get("kv-dev-scope", "sp-client-id")
client_secret = dbutils.secrets.get("kv-dev-scope", "sp-client-secret")
tenant_id = dbutils.secrets.get("kv-dev-scope", "sp-tenant-id")
adls_account = dbutils.secrets.get("kv-dev-scope", "adls-account")
container = "datalake"
delta_path = f"abfss://{container}@{adls_account}.dfs.core.windows.net/dev/delta/patients"

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# set config on spark hadoop for direct ABFSS usage
spark.conf.set(f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Synthetic patient dataset (no PHI)
data = [
    (1, "PATIENT_A", "M", 45, "2024-11-01", "hypertension"),
    (2, "PATIENT_B", "F", 60, "2024-11-02", "diabetes"),
    (3, "PATIENT_C", "M", 30, "2024-11-03", "asthma")
]
columns = ["patient_id","patient_code","gender","age","admission_date","diagnosis"]
df = spark.createDataFrame(data, schema=columns)

# Add provenance metadata (env marker)
df = df.withColumn("env", lit("dev"))

# Write as Delta
df.write.format("delta").mode("overwrite").save(delta_path)

# Simple readback to confirm
print("Delta saved to:", delta_path)
display(spark.read.format("delta").load(delta_path).limit(10))
