# Databricks notebook source
from databricks.sdk.runtime import dbutils

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# TODO: Extrae los links usando la API de Emol

# TODO: Inicialmente solo probamos con 1 endpoint
ENDPOINTS = [
    {
        "categoria": "nacional",
        "url": "https://newsapi.ecn.cl/NewsApi/emol/seccionFiltrada/nacional/0",
    },
    # {
    #    "categoria": "internacional",
    #    "url": "https://newsapi.ecn.cl/NewsApi/emol/seccionFiltrada/internacional/0"
    # },
    # {
    #    "categoria": "tecnologia",
    #    "url": "https://newsapi.ecn.cl/NewsApi/emol/seccionFiltrada/tecnología/0"
    # },
    # {
    #    "categoria": "educacion",
    #    "url": "https://newsapi.ecn.cl/NewsApi/emol/temaFiltrado/79,109/0"
    # },
    # {
    #    "categoria": "multimedia",
    #    "url": "https://newsapi.ecn.cl/NewsApi/emol/temaFiltrado/960/0"
    # }
]
