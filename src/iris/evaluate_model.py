# Databricks notebook source
"""
Script para evaluar el modelo entrenado con los datos de prueba.
Calcula métricas clave (exactitud, precisión, F1-score) y verifica que el 
modelo supere un umbral mínimo de rendimiento para aprobar su pase a producción.
"""

# COMMAND ----------
import pandas as pd
from databricks.sdk.runtime import dbutils
from mlflow import sklearn
from mlflow.tracking import MlflowClient
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix,
)

# COMMAND ----------
# Define parámetro externo de orquestación
dbutils.widgets.text("run_date", "")
# Extrae valor capturado desde la tarea
run_date = dbutils.widgets.get("run_date")
print(f"Run date: {run_date}")

# COMMAND ----------
RANDOM_STATE = 42
TEST_SIZE = 0.2

# COMMAND ----------
folder_name = (
    f"/Volumes/workspace/lakeflow_db/lakeflow_volume/features/{run_date}"
)
file_name = f"{folder_name}/features_{run_date}.csv"

df = pd.read_csv(file_name)

X = df.drop("target", axis=1)
y = df["target"]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
)

# COMMAND ----------
try:
    # Obtener el run_id del paso de entrenamiento leyendo variable expuesta por la tarea previa ("train_model")
    run_id = dbutils.jobs.taskValues.get(taskKey="train_model", key="run_id")

    # Cargar el modelo asegurando la ruta correcta (artefacto completo registrado en ese Run ID de MLflow)
    model = sklearn.load_model(f"runs:/{run_id}/iris-model")
    y_pred = model.predict(X_test)
    clases = sorted(y_test.unique().tolist())

    # Cálculo de métricas
    acc = accuracy_score(y_test, y_pred)
    precision_macro = precision_score(y_test, y_pred, average="macro")
    recall_macro = recall_score(y_test, y_pred, average="macro")
    f1_macro = f1_score(y_test, y_pred, average="macro")
    matriz = confusion_matrix(y_test, y_pred).tolist()
    reporte = classification_report(
        y_test, y_pred, target_names=[f"clase_{c}" for c in clases]
    )
    print(f"Exactitud: {acc:.4f}")
    print(f"F1 macro: {f1_macro:.4f}")
    print(f"Reporte:\n{reporte}")
    if acc < 0.8:
        raise ValueError(
            f"Rendimiento del modelo por debajo del umbral "
            f"(exactitud={acc:.4f} < 0.8)"
        )

    # Registra métricas y artefactos en el run existente
    # Cliente auxiliar para forzar escritura en corridas no-activas o históricas
    client = MlflowClient()
    client.set_tag(run_id, "etapa", "evaluacion")
    client.set_tag(run_id, "evaluacion.resultado", "aprobado")
    client.set_tag(run_id, "evaluacion.umbral_exactitud", "0.8")
    # Inyecta una métrica específica al registro de MLflow
    client.log_metric(run_id, "exactitud", acc)
    client.log_metric(run_id, "precision_macro", precision_macro)
    client.log_metric(run_id, "recall_macro", recall_macro)
    client.log_metric(run_id, "f1_macro", f1_macro)
    # Métricas por clase
    prec_por_clase = precision_score(
        y_test, y_pred, average=None, labels=clases
    )
    rec_por_clase = recall_score(y_test, y_pred, average=None, labels=clases)
    f1_por_clase = f1_score(y_test, y_pred, average=None, labels=clases)
    for i, clase in enumerate(clases):
        client.log_metric(run_id, f"precision_clase_{clase}", prec_por_clase[i])
        client.log_metric(run_id, f"recall_clase_{clase}", rec_por_clase[i])
        client.log_metric(run_id, f"f1_clase_{clase}", f1_por_clase[i])

    # Propagar valores para register_model.py (Pasa el Run ID hacia el siguiente paso)
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    dbutils.jobs.taskValues.set(key="model_accuracy", value=float(acc))
    dbutils.jobs.taskValues.set(key="is_model_approved", value=True)

except Exception as error:
    raise RuntimeError(f"Error evaluando el modelo: \n{error}")
