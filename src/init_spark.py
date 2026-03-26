# Databricks notebook source

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Crear SparkSession\,,
spark = (
    SparkSession.builder.appName("ML Iris")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
