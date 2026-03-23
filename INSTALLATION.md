# databricks-lakeflow

Instrucciones para el paso a paso
luego se limpiará y ordenará

## Instrucciones

### Instalar Databricks cli

curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

### validar instalacion

databricks -v

### Iniciar sesión con Databricks

Tuve que instalar `wslview`. De esta forma el comando `databricks auth login` me arrojó un enlace que pude copiar y pegar en la terminal.

```sh
sudo apt update
sudo apt install wslu # instala wslview

export BROWSER=wslview

databricks auth login
```

### Iniciar proyecto

```sh
databricks bundle init

# Configurar a gusto del consumidor
```

Luego que genere el proyecto, para instalar las dependencias, ejecutar

```sh
# Entrar al directorio del proyecto
cd <nombre_proyecto>

# Generar ambiente virtual
uv venv

# Activar ambiente virtual
source .venv/bin/activate

# Sincronizar las dependencias y otros
uv sync
```

### Desplegar el bundle

```sh
databricks bundle deploy --target dev
```

###

## Referencia

- https://docs.databricks.com/aws/en/dev-tools/cli/install#curl-install
- https://databricks-sdk-py.readthedocs.io/en/latest/workspace/jobs/jobs.html
- https://www.databricks.com/blog/how-move-apache-airflowr-databricks-lakeflow-jobs
- https://github.com/databricks/bundle-examples
- https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html
- https://docs.databricks.com/aws/en/dev-tools/bundles/settings
