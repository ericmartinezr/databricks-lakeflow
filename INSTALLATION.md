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

###

## Referencia

https://docs.databricks.com/aws/en/dev-tools/cli/install#curl-install
