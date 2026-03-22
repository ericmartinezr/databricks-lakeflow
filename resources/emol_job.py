from databricks.bundles.jobs import Job, Task, NotebookTask, TaskDependency

extract_links = Task(
    task_key="extract_links",
    notebook_task=NotebookTask(
        notebook_path="src/tasks/extract_links.py",
        base_parameters={"run_date": "{{ job.trigger.time.iso_date }}"},
    ),
)

extract_data = Task(
    task_key="extract_data",
    depends_on=[TaskDependency(task_key="extract_links")],
    notebook_task=NotebookTask(notebook_path="src/tasks/extract_data.py"),
)

job = Job(name="emol_job", tasks=[extract_links, extract_data])
