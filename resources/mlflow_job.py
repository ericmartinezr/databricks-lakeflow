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
        notebook_path="src/iris/extract_data.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)

validate_data = Task(
    task_key="validate_data",
    depends_on=[TaskDependency(task_key="extract_data")],
    notebook_task=NotebookTask(
        notebook_path="src/iris/validate_data.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)

feature_engineering = Task(
    task_key="feature_engineering",
    depends_on=[TaskDependency(task_key="validate_data")],
    notebook_task=NotebookTask(
        notebook_path="src/iris/feature_engineering.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)

train_model = Task(
    task_key="train_model",
    depends_on=[TaskDependency(task_key="feature_engineering")],
    notebook_task=NotebookTask(
        notebook_path="src/iris/train_model.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)

evaluate_model = Task(
    task_key="evaluate_model",
    depends_on=[TaskDependency(task_key="train_model")],
    notebook_task=NotebookTask(
        notebook_path="src/iris/evaluate_model.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)

test_model = Task(
    task_key="test_model",
    depends_on=[TaskDependency(task_key="evaluate_model")],
    notebook_task=NotebookTask(notebook_path="src/iris/test_model.py"),
)

register_model = Task(
    task_key="register_model",
    depends_on=[TaskDependency(task_key="test_model")],
    notebook_task=NotebookTask(notebook_path="src/iris/register_model.py"),
)

job = Job(
    name="mlflow_job",
    tasks=[
        extract_data,
        validate_data,
        feature_engineering,
        train_model,
        evaluate_model,
        test_model,
        register_model,
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
