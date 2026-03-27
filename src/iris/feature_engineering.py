# Databricks notebook source

# COMMAND ----------
from databricks.sdk.runtime import dbutils

# COMMAND ----------
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")
print(f"Run date: {run_date}")

# COMMAND ----------
file_name = (
    f"/Volumes/workspace/lakeflow_db/lakeflow_volume/iris_{run_date}.csv"
)

df = spark.read.csv(file_name, header=True, inferSchema=True)
df.printSchema()


# COMMAND ----------
try:
    file_name = f"/Volumes/workspace/lakeflow_db/lakeflow_volume/features_{run_date}.csv"

    df = df.withColumn(
        "sepal_ratio", df["sepal length (cm)"] / df["sepal width (cm)"]
    )
    df.to_csv(file_name, index=False)
    df.printSchema()
    print(f"Archivo {file_name} creado correctamente.")
except Exception:
    print("Error saving file with features")
