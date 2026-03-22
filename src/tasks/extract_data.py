# Databricks notebook source

# TODO: Scrapea e inserta datos a alguna tabla en Databricks
from databricks.sdk.runtime import dbutils

run_date = dbutils.jobs.taskValues.get(
    taskKey="extract_links", key="run_date", debugValue=None
)

print(f"Run date {run_date}")
