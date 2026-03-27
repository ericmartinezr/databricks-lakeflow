# Databricks Lakeflow - Proyecto de Aprendizaje

Este proyecto tiene como objetivo principal explorar, aprender y establecer buenas prácticas para la creación y automatización de procesos utilizando **Databricks Lakeflow** y **Databricks Asset Bundles (DABs)**.

A continuación, se listan algunas ideas de Jobs y ETLs que se desarrollarán y analizarán en este repositorio:

| Nombre del Job / ETL    | Tipo             | Descripción                                                                                                               | Factibilidad / Estado |
| ----------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| **Ingesta S3 o BBDD**   | Ingesta          | Job automatizado para recibir y cargar lotes de datos desde sistemas externos (ej. AWS S3 o PostgreSQL) a la capa Bronze. | Por definir           |
| **Curaduría DLT**       | Pipeline         | Explorar un pipeline con Delta Live Tables para automatizar las transformaciones de datos Bronze -> Silver -> Gold.       | Por definir           |
| **Análisis de Modelos** | Machine Learning | Entrenar un modelo de prueba y registrar parámetros, métricas y resultados con MLflow gestionado por un Job.              | Por definir           |

---

## 📁 Estructura del Proyecto

El repositorio fue inicializado mediante plantillas de Databricks Bundles, lo que generó la siguiente estructura recomendada:

- `src/`: Almacena el código fuente en Python (como los notebooks y scripts de extracción).
- `resources/`: Contiene la definición de tus flujos de trabajo e infraestructura como código (ej. configuración del Job `emol_job`).
- `tests/`: Destinado a las pruebas unitarias locales para certificar la estabilidad de tu código fuente.
- `fixtures/`: Archivos y datasets estáticos locales para usarse durante las pruebas (`tests`).

---

## 🚀 Guía de Instalación y Configuración

### 1. Instalar Databricks CLI

Databricks Asset Bundles interactúa con los workspaces de la nube mediante el CLI oficial de Databricks. Debes instalarlo en tu entorno loca:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Verifica que se instaló correctamente con:

```bash
databricks -v
```

### 2. Autenticarse con el Workspace de Databricks

Debes vincular tu configuración local con la nube mediante un proceso de _Single Sign-On (SSO)_.

> **Nota para usuarios de WSL (Windows Subsystem for Linux):**
> Para que el CLI logre lanzar correctamente una pestaña del navegador de Windows desde la terminal de Ubuntu y validar la credencial, necesitas instalar `wslu`:
>
> ```bash
> sudo apt update
> sudo apt install wslu
> export BROWSER=wslview
> ```

Ejecuta el inicio de sesión y sigue los pasos en tu navegador:

```bash
databricks auth login
```

### 3. Entorno Virtual y Dependencias

Este proyecto utiliza `uv` como gestor ultrarrápido de paquetes en Python. Ubícate en este directorio y ejecuta:

```bash
# Generar un ambiente virtual de Python
uv venv

# Activar el ambiente virtual
source .venv/bin/activate

# Instalar sincronizadamente las dependencias del proyecto (incluyendo librerías de desarrollo útiles)
uv sync --dev
```

---

## 💻 Comandos Principales de Despliegue (Bundles)

Gracias a Databricks Asset Bundles, puedes controlar tus flujos de trabajo de forma declarativa desde tu consola favorita.

- **Desplegar proyecto en entorno Desarrollo (dev)**:
  Sincroniza tus cambios en Python (en tu Workspace personal) y actualiza la definición del Job sin afectar a producción.

  ```bash
  databricks bundle deploy --target dev
  ```

  _(El tag `dev` corresponde al objetivo por defecto si lo omites)._

- **Correr o Disparar Manualmente un Job**:
  Arranca una ejecución programada al instante. Te generará un enlace para seguir la evolución en la interfaz UI.

  ```bash
  databricks bundle run
  ```

- **Desplegar proyecto en Producción (prod)**:
  Impacta oficialmente los cambios en los perfiles destinados a producción.

  ```bash
  databricks bundle deploy --target prod
  ```

- **Ejecutar Pruebas Unitarias localmente**:
  Revisa si tu código funciona en el computador mediante la validación de `pytest`.
  ```bash
  uv run pytest
  ```

---

## ⚠️ Consideraciones Importantes Especiales

Hasta el momento me he encontrado con las siguientes observaciones, todas aplican a Databricks Free.

- No se puede configurar Spark, o al menos las siguientes opciones no se pueden utilizar

* spark.sql.adaptive.enabled
* spark.sql.adaptive.coalescePartitions.enabled

- Con Great Expectations, al agregar un contexto como `context.data_sources.add_spark` no se puede usar `persist` (True por defecto), ya que intenta almacenar los pasos intermedios, por lo que es necesario especificar `persist=False`

```python
suite = context.suites.add(suite)
datasource = context.data_sources.add_spark(
    name="spark_in_memory", persist=False
)
```

- **Llamadas Web y Red Restringida**: Si posees una cuenta **Databricks Free Edition. Community Edition**, por defecto la red del cluster se restringe fuertemente contra sitios de internet públicos, lo que **no permite hacer llamadas a APIs externas o Web Scraping**. Para más detalles revisa este hilo en el [foro de la comunidad Databricks](https://community.databricks.com/t5/data-engineering/not-able-to-call-external-api-when-using-databricks-free-edition/td-p/126542).

---

## 📚 Enlaces de Referencia Útiles

- [Databricks CLI - Instalación CURL](https://docs.databricks.com/aws/en/dev-tools/cli/install#curl-install)
- [Databricks SDK para Python - Docs](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/jobs/jobs.html)
- [Databricks Asset Bundles Settings](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
- [Ejemplos Oficiales de Proyectos Bundles](https://github.com/databricks/bundle-examples)
- [Generador / Explicador formato Quartz Cron](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)
- [Artículos: Migración desde Apache Airflow a Lakeflow Jobs](https://www.databricks.com/blog/how-move-apache-airflowr-databricks-lakeflow-jobs)
