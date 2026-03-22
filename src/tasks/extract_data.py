# Databricks notebook source

# TODO: Scrapea e inserta datos a alguna tabla en Databricks
from databricks.sdk.runtime import dbutils

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")


print(f"Run date {run_date}")
