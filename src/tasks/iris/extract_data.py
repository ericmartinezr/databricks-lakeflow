# Databricks notebook source
from databricks.sdk.runtime import dbutils
from datetime import datetime, timedelta

# MAGIC %pip install scikit-learn==1.7

# COMMAND ----------
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")
yesterday = datetime.fromisoformat(run_date) - timedelta(days=1)
print(f"Run date: {run_date}")
print(f"Yesterday: {yesterday}")

# COMMAND ----------
from sklearn.datasets import load_iris

try:
    # TODO: Apuntar a un Volumen de databricks
    # file_name = f"{hook.get_path()}/iris_{ds}.csv"
    file_name = ""

    data = load_iris(as_frame=True)
    df = data.frame
    df.to_csv(file_name, index=False)

    print(f"Archivo {file_name} creado correctamente.")

except Exception as e:
    print("Error extracting training data")
    print(e)
