# COMMAND ----------
# MAGIC %pip install mlflow>=3.0 --upgrade

# COMMAND ----------
# MAGIC %restart_python

# COMMAND ----------
import pandas as pd
import mlflow
from databricks.sdk.runtime import dbutils
from sklearn.model_selection import train_test_split
from mlflow import sklearn
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestClassifier

# COMMAND ----------
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")
print(f"Run date: {run_date}")

# COMMAND ----------
RANDOM_STATE = 42
TEST_SIZE = 0.2

# COMMAND ----------
file_name = (
    f"/Volumes/workspace/lakeflow_db/lakeflow_volume/features_{run_date}.csv"
)

df = pd.read_csv(file_name)

X = df.drop("target", axis=1)
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
)

# COMMAND ----------

experiment_name = "iris-experiment"
experiment = mlflow.set_experiment(experiment_name)

with mlflow.start_run(experiment_id=experiment.experiment_id) as active_run:
    run_id = active_run.info.run_id
    print(f"Run activo: {run_id}")

    model = RandomForestClassifier(random_state=RANDOM_STATE)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    
    signature = infer_signature(X_test, y_pred)
    
    model_info = sklearn.log_model(
        sk_model=model,
        name="iris-model",
        signature=signature,
        input_example=X_test.iloc[:5],
    )
    
    print(f"Id del modelo registrado '{model_info.model_id}'")

    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    dbutils.jobs.taskValues.set(key="model_id", value=model_info.model_id)

