# Databricks notebook source

# COMMAND ----------
import pandas as pd
import mlflow.sklearn
from databricks.sdk.runtime import dbutils

# COMMAND ----------
run_id = dbutils.jobs.taskValues.get(taskKey="evaluate_model", key="run_id")
model = mlflow.sklearn.load_model(f"runs:/{run_id}/iris-model")
df = pd.DataFrame(
    [
        [5.1, 3.5, 1.4, 0.2, 0.8],
        [6.2, 3.4, 5.4, 2.3, 1.0],
        [5.9, 3.0, 4.2, 1.5, 0.2],
    ],
    columns=[
        "sepal length (cm)",
        "sepal width (cm)",
        "petal length (cm)",
        "petal width (cm)",
        "sepal_ratio",
    ],
)
pred = model.predict(df)
print(pred)
