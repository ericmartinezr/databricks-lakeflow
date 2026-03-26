# Databricks notebook source

# COMMAND ----------
# MAGIC %pip install great_expectations==1.15.1

# COMMAND ----------
# MAGIC %restart_python


# COMMAND ----------
import great_expectations as gx
from databricks.sdk.runtime import dbutils

# COMMAND ----------
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")
print(f"Run date: {run_date}")

# COMMAND ----------
context = gx.get_context(mode="ephemeral")
suite = gx.ExpectationSuite(name="iris-gx-suite")
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="sepal length (cm)")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="sepal length (cm)")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="petal length (cm)")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="petal width (cm)")
)

# COMMAND ----------
file_name = (
    f"/Volumes/workspace/lakeflow_db/lakeflow_volume/iris_{run_date}.csv"
)

df = spark.read.csv(file_name, header=True, inferSchema=True)
df.printSchema()

# COMMAND ----------
suite = context.suites.add(suite)
# persist=false es necesario porque Databricks Free no permite "PERSIST TABLE" en serverless
datasource = context.data_sources.add_spark(
    name="spark_in_memory", persist=False
)
data_asset = datasource.add_dataframe_asset(name="iris_df_data_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    "Iris Definition"
)
batch = batch_definition.get_batch({"dataframe": df})
expectation = batch.validate(suite)
print(f"Expectation result: \n{expectation}")
