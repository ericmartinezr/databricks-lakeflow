# Databricks notebook source

# COMMAND ----------
import pandas as pd
from databricks.sdk.runtime import dbutils
from mlflow import sklearn

# COMMAND ----------
run_id = dbutils.jobs.taskValues.get(taskKey="train_model", key="run_id")
model = sklearn.load_model(f"runs:/{run_id}/model")
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
