# Databricks notebook source
"""
Script para asentar el modelo en el Model Registry (Unity Catalog).
Si el modelo fue aprobado en la etapa de evaluación, se registra formalmente 
y se le asigna el alias 'production'.
"""

# COMMAND ----------
# MAGIC %pip install mlflow>=3.0 --upgrade

# COMMAND ----------
# MAGIC %restart_python

# COMMAND ----------
import mlflow
from databricks.sdk.runtime import dbutils
from mlflow.tracking import MlflowClient

# COMMAND ----------
# Recuperar valores de la tarea de evaluación
# NOTA: Asegúrate de que en la definición del Job la tarea anterior se llame 'evaluate_model'
# Lee el Run ID desde MLflow
run_id = dbutils.jobs.taskValues.get(
    taskKey="evaluate_model", key="run_id", default=""
)
# Determina si se excedió el threshold establecido
is_model_approved = dbutils.jobs.taskValues.get(
    taskKey="evaluate_model", key="is_model_approved", default=False
)
accuracy = dbutils.jobs.taskValues.get(
    taskKey="evaluate_model", key="model_accuracy", default=0.0
)

if not run_id:
    raise ValueError(
        "No se obtuvo el run_id. Verifica que la tarea 'evaluate_model' exporte el valor correctamente."
    )

print(f"Cargando modelo desde el Run ID: {run_id}")
print(f"Precisión obtenida en evaluación (Accuracy): {accuracy:.4f}")

# COMMAND ----------
model_name = "workspace.lakeflow_db.iris_model_registry"
model_uri = f"runs:/{run_id}/iris-model"

if is_model_approved:
    print(
        "\nEl modelo fue aprobado en la evaluación. Registrando el modelo en el Model Registry de Databricks..."
    )

    # Registrar o crear una nueva versión del modelo (Promueve el artefacto al Unity Catalog / Model Registry)
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
    print(
        f"Modelo '{model_details.name}' registrado exitosamente (Versión: {model_details.version})"
    )

    # Asignar un Alias al modelo (método recomendado en MLflow)
    client = MlflowClient()
    # Asigna alias "production", reemplazando la lógica de Stages antiguos
    client.set_registered_model_alias(
        name=model_name, alias="production", version=model_details.version
    )
    print(
        f"Etiqueta 'production' asignada a la versión {model_details.version} del modelo '{model_name}'."
    )
else:
    print("\nEl modelo no fue aprobado. Omitiendo el registro del modelo.")
