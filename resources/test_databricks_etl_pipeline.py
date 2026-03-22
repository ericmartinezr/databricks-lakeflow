from databricks.bundles.pipelines import Pipeline

"""
The main pipeline for test_databricks
"""

test_databricks_etl = Pipeline.from_dict(
    {
        "name": "test_databricks_etl",
        "catalog": "${var.catalog}",
        "schema": "${var.schema}",
        "serverless": True,
        "root_path": "src/test_databricks_etl",
        "libraries": [
            {
                "glob": {
                    "include": "src/test_databricks_etl/transformations/**",
                },
            },
        ],
        "environment": {
            "dependencies": [
                # We include every dependency defined by pyproject.toml by defining an editable environment
                # that points to the folder where pyproject.toml is deployed.
                "--editable ${workspace.file_path}",
            ],
        },
    }
)
