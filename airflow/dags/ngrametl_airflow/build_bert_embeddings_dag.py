from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)


def build_bert_embeddings_dag(
    dag_id,
    ngram_dataset,
    input_dataset_project_id,
    input_dataset_name,
    input_table,
    output_dataset_project_id,
    output_dataset_name,
    output_table,
    dataflow_template_path,
    dataflow_environment,
    batch_size=1000,
    notification_emails=None,
    load_max_active_runs=1,
    load_start_date=datetime(2021, 1, 1),
):
    default_dag_args = {
        "depends_on_past": False,
        "start_date": load_start_date,
        "end_date": None,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 0,
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args["email"] = [
            email.strip() for email in notification_emails.split(",")
        ]

    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=None,
        max_active_runs=load_max_active_runs,
        default_args=default_dag_args,
        is_paused_upon_creation=True,
    )

    dataflow_operator = DataflowStartFlexTemplateOperator(
        task_id="load_bert_embeddings",
        body={
            "launchParameter": {
                "containerSpecGcsPath": dataflow_template_path,
                "jobName": ngram_dataset.replace("_", "-")
                + '-bert-embeddings-{{ execution_date.strftime("%Y%m%d-%H%M%S") }}',
                "parameters": {
                    "input-table": f"{input_dataset_project_id}.{input_dataset_name}.{input_table}",
                    "output-table": f"{output_dataset_project_id}:{output_dataset_name}.{output_table}",
                    "batch-size": f"{batch_size}",
                },
                "environment": dataflow_environment,
            }
        },
        location="us-central1",
        wait_until_finished=False,
        dag=dag,
    )

    return dag
