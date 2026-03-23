# Databricks notebook source
# MAGIC %pip install curl_cffi
# MAGIC %pip install scrapling==0.4

# COMMAND ----------
import requests
from databricks.sdk.runtime import dbutils
from datetime import datetime, timedelta
from scrapling.parser import Selector


dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# Le resta 1 día para consultar datos del dia anterior al dia de ejecucion
yesterday = datetime.fromisoformat(run_date) - timedelta(days=1)

print(f"Run date: {run_date}")
print(f"Yesterday: {yesterday}")

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

categoria = ENDPOINTS[0]["categoria"]
url = ENDPOINTS[0]["url"]

params = {"size": 5, "from": 0, "fechaPublicacion": yesterday}


def fetch_data(params):
    news_links = []
    request = requests.get(url, params=params)
    data = request.json()

    hits_news = data["hits"]["hits"]
    if not hits_news:
        return []

    for hit in hits_news:
        id = hit["_source"]["id"]
        permalink = hit["_source"]["permalink"]
        fecha_publicacion = hit["_source"]["fechaPublicacion"]
        fecha_modificacion = hit["_source"]["fechaModificacion"]
        news_links.append(
            {
                "id": id,
                "categoria": categoria,
                "fecha_publicacion": fecha_publicacion,
                "fecha_modificacion": fecha_modificacion,
                "link": permalink.replace("http://", "https://"),
            }
        )

    return news_links


# No se que son los numeros al final, parece que no afectan
news_links = fetch_data(params)
if not news_links:
    raise ValueError(f"No news found for date {params['fechaPublicacion']}")

print(f"Found {len(news_links)} news.")

data_dates = [d["fecha_publicacion"].split("T")[0] for d in news_links]

# Query loop until no date is found in the records
counter = 1
while params["fechaPublicacion"] in data_dates:
    print(f"Iteration {counter}")

    params["from"] += counter * (params["size"] + 1)
    data_news = fetch_data(params)
    if not data_news:
        raise ValueError(
            f"No news found for date {params['fechaPublicacion']}. Iteration {counter}"
        )

    data_dates = [d["fecha_publicacion"].split("T")[0] for d in data_news]

    # In case there's another iteration and the wanted date is not in the returned data
    if params["fechaPublicacion"] not in data_dates:
        break

    news_links.extend(data_news)
    counter += 1

news_links

dbutils.jobs.taskValues.set(key="news_links", value=news_links)
