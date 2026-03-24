from databricks.bundles.jobs import (
    Job,
    Task,
    NotebookTask,
    TaskDependency,
    JobParameterDefinition,
    CronSchedule,
    PauseStatus,
)


extract_data = Task(
    task_key="extract_data",
    notebook_task=NotebookTask(
        notebook_path="src/tasks/iris/extract_data.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)

# TODO: Short circuit
validate_data = Task(
    task_key="validate_data",
    depends_on=[TaskDependency(task_key="extract_data")],
    notebook_task=NotebookTask(
        notebook_path="src/tasks/iris/validate_data.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)
#
# feature_engineering = Task(
#    task_key="feature_engineering",
#    depends_on=[TaskDependency(task_key="validate_data")],
# )
#
# train_register_model = Task(
#    task_key="train_register_model",
#    depends_on=[TaskDependency(task_key="feature_engineering")],
# )
#
# evaluate_model = Task(
#    task_key="evaluate_model",
#    depends_on=[TaskDependency(task_key="train_register_model")],
# )
#
# test_model = Task(
#    task_key="test_model",
#    depends_on=[TaskDependency(task_key="evaluate_model")],
# )
#
## TODO: Es necesario copiar el modelo en Databricks a otra ruta?
# copy_model = Task(
#    task_key="copy_model",
#    depends_on=[TaskDependency(task_key="test_model")],
# )


job = Job(
    name="mlflow_job",
    tasks=[
        extract_data,
        validate_data,
        # feature_engineering,
        # train_register_model,
        # evaluate_model,
        # test_model,
        # copy_model,
    ],
    schedule=CronSchedule(
        quartz_cron_expression="0 0 9 * * ?",
        timezone_id="America/Santiago",
        pause_status=PauseStatus.UNPAUSED,
    ),
    parameters=[
        JobParameterDefinition(
            name="run_date", default="{{ job.trigger.time.iso_date }}"
        ),
    ],
)
