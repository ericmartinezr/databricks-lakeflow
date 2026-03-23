from databricks.bundles.jobs import (
    Job,
    Task,
    NotebookTask,
    TaskDependency,
    JobParameterDefinition,
    CronSchedule,
    PauseStatus,
)


extract_links = Task(
    task_key="extract_links",
    notebook_task=NotebookTask(
        notebook_path="src/tasks/extract_links.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)

extract_data = Task(
    task_key="extract_data",
    depends_on=[TaskDependency(task_key="extract_links")],
    notebook_task=NotebookTask(
        notebook_path="src/tasks/extract_data.py",
        base_parameters={"run_date": "{{ job.parameters.run_date }}"},
    ),
)


job = Job(
    name="emol_job",
    tasks=[extract_links, extract_data],
    schedule=CronSchedule(
        quartz_cron_expression="0 0 9 * * *",
        timezone_id="America/Santiago",
        pause_status=PauseStatus.UNPAUSED,
    ),
    parameters=[
        JobParameterDefinition(
            name="run_date", default="{{ job.trigger.time.iso_date }}"
        ),
    ],
)
