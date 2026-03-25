# Databricks notebook source

# MAGIC %pip install great_expectations==1.15.1

# MAGIC %run "../../init_spark.py"


# COMMAND ----------
from databricks.sdk.runtime import dbutils
import great_expectations as gx

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

suite = context.suites.add(suite)
datasource = context.data_sources.add_pandas(name="pandas_source")
data_asset = datasource.add_dataframe_asset(name="iris_df_data_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    "Iris Definition"
)

# COMMAND ----------
file_name = (
    f"/Volumes/workspace/lakeflow_db/lakeflow_volume/iris_{run_date}.csv"
)

df = spark.read.csv(file_name, header=True, inferSchema=True)
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
expectation = batch.validate(suite)
print(f"Expectation result: \n{expectation}")
