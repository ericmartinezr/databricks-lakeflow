import pytest
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

@pytest.fixture()
def sample_features_df(load_fixture):
    """Genera un DataFrame pequeño que simula ser el dataset Iris ya procesado"""
    # Usamos la capacidad de cargar fixtures del proyecto Databricks
    df_spark = load_fixture("test_iris.csv")
    
    # Parseo a Floats/Double ya que el lector manual del conftest.DictReader puede arrojar Strings
    from pyspark.sql.functions import col
    df_spark = df_spark.withColumn("sepal length (cm)", col("sepal length (cm)").cast("double"))
    df_spark = df_spark.withColumn("sepal width (cm)", col("sepal width (cm)").cast("double"))
    df_spark = df_spark.withColumn("target", col("target").cast("integer"))
    
    # Calculamos la feature que faltó
    from src.iris.feature_engineering import calculate_sepal_ratio
    df_spark = calculate_sepal_ratio(df_spark)
    
    return df_spark.toPandas()

def test_model_training_fast(sample_features_df):
    """
    Prueba funcional de ML.
    Entrena un RF sencillo sin hiperparámetros pesados ni variables globales,
    para garantizar que la prueba sea rápida y evitar esperas o bloqueos.
    """
    df = sample_features_df
    X = df.drop("target", axis=1)
    y = df["target"]
    
    # Simula la separación ejecutada en src.iris.train_model
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # n_estimators y max_depth bajos para prevenir ejecución prologada (sin hang)
    model = RandomForestClassifier(random_state=42, n_estimators=5, max_depth=3) 
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    
    # Comprobaciones de que el entrenamiento fluyó exitosamente:
    assert len(y_pred) == len(X_test), "Las predicciones deben igualar el tamaño del conjunto de prueba"
    assert set(y_pred).issubset({0, 1, 2}), "Las clases predichas deben pertenecer al dominio Iris"
    assert model.classes_ is not None, "El modelo debe tener clases detectadas"
