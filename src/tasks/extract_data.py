# Databricks notebook source

# TODO: Scrapea e inserta datos a alguna tabla en Databricks
from databricks.sdk.runtime import dbutils

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")


print(f"Run date {run_date}")


news_links = dbutils.jobs.taskValues.get(
    taskKey="extract_links", key="news_links", debugValue=None
)

print(f"Received {len(news_links)} links")
for nl in news_links:
    print(f"Link received: {nl}")
