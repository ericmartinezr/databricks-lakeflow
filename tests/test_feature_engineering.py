import pytest
import sys
from unittest.mock import MagicMock

# Mockeamos dbutils globalmente antes de importar el código fuente para evitar errores de importación y posibles ciclos
mock_dbutils = MagicMock()
mock_dbutils.widgets.get.return_value = "2026-03-29"
sys.modules["databricks.sdk.runtime"] = MagicMock(dbutils=mock_dbutils)

# Ahora podemos importar la función segura testable desde src.iris.feature_engineering
from src.iris.feature_engineering import calculate_sepal_ratio

def test_feature_engineering_ratio(load_fixture):
    """
    Prueba que el transformer `calculate_sepal_ratio` aplica la lógica correctamente.
    Utilizamos la fixture `load_fixture` proveída por conftest.py para cargar datos CSV.
    Se inyectan datos controlados para que funcione velozmente sin colgarse en el clúster.
    """
    # Cargamos el dataframe a traves de la fixture de data mock
    df = load_fixture("test_iris.csv")
    
    # Cast "sepal length (cm)" and "sepal width (cm)" to float (Databricks .csv loader using load_fixture yields strings by default)
    from pyspark.sql.functions import col
    df = df.withColumn("sepal length (cm)", col("sepal length (cm)").cast("double"))
    df = df.withColumn("sepal width (cm)", col("sepal width (cm)").cast("double"))
    
    # Ejecutamos la función de features extraída del notebook
    df_transformed = calculate_sepal_ratio(df)
    
    # Recolectamos para verificar salida (evaluamos 2 porciones)
    results = df_transformed.collect()
    
    # Validaciones fundamentales (el csv tiene 10 filas):
    assert len(results) == 10, "Debe devolver la misma cantidad de filas probadas"
    assert "sepal_ratio" in df_transformed.columns, "Faltó la columna 'sepal_ratio'"
    
    # Verificamos matemáticamente la creación del feature
    assert round(results[0]["sepal_ratio"], 2) == round(5.1 / 3.5, 2)
    assert round(results[1]["sepal_ratio"], 2) == round(4.9 / 3.0, 2)
