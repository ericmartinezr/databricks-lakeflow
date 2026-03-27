# Databricks notebook source

# COMMAND ----------
import os
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
    folder_name = (
        f"/Volumes/workspace/lakeflow_db/lakeflow_volume/features/{run_date}"
    )
    file_name = f"{folder_name}/features_{run_date}.csv"

    # Spark Dataframe, no puedo usar 'to_csv'
    df = df.withColumn(
        "sepal_ratio", df["sepal length (cm)"] / df["sepal width (cm)"]
    )

    # Guardar el DF en carpeta con el nombre de la fecha de ejecucion
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        folder_name
    )

    # Renombrar el archivo con nombre por defecto
    files = os.listdir(folder_name)
    csv_file = [f for f in files if f.endswith(".csv")][0]
    os.rename(
        f"{folder_name}/{csv_file}",
        file_name,
    )

    df.printSchema()
    print(f"Archivo {file_name} creado correctamente.")

except Exception as error:
    raise RuntimeError(f"Error saving features: \n{error}")
