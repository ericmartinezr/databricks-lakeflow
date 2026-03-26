# Databricks notebook source

# COMMAND ----------
from databricks.sdk.runtime import dbutils
from datetime import datetime, timedelta
from sklearn.datasets import load_iris

# COMMAND ----------
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")
yesterday = datetime.fromisoformat(run_date) - timedelta(days=1)
print(f"Run date: {run_date}")
print(f"Yesterday: {yesterday}")

# COMMAND ----------

# TODO: Validar, deberia capturar el error y retornar un simple mensaje
# o deberia detener la ejecucion del pipeline en caso de excepcion

try:
    # TODO: Apuntar a un Volumen de databricks
    file_name = (
        f"/Volumes/workspace/lakeflow_db/lakeflow_volume/iris_{run_date}.csv"
    )

    data = load_iris(as_frame=True)
    df = data.frame
    df.to_csv(file_name, index=False)

    print(f"Archivo {file_name} creado correctamente.")

except Exception as e:
    print("Error extracting training data")
    print(e)
