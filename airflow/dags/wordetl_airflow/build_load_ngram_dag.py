from datetime import datetime, timedelta
from airflow import models, configuration
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)


def build_load_ngram_dag(
    dag_id,
    input_file,
    output_table,
    dataset_project_id,
    dataset_name,
    dataflow_template_path,
    dataflow_environment,
    notification_emails=None,
    load_start_date=datetime(2021, 1, 1),
    load_end_date=None,
    load_schedule_interval=None,
    load_max_active_runs=1,
):
    default_dag_args = {
        "depends_on_past": False,
        "start_date": load_start_date,
        "end_date": load_end_date,
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
        schedule_interval=load_schedule_interval,
        max_active_runs=load_max_active_runs,
        default_args=default_dag_args,
    )

    dataflow_operator = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_streaming_beam_sql",
        body={
            "launchParameter": {
                "containerSpecGcsPath": f"{dataflow_template_path}/ngrams-beam.json",
                "jobName": 'ngram-beam-{{ execution_date.strftime("%Y%m%d-%H%M%S") }}',
                "parameters": {
                    "input-file": input_file,
                    "output-table": f"{dataset_project_id}:{dataset_name}.{output_table}",
                },
                "environment": dataflow_environment,
            }
        },
        location="us-central1",
        wait_until_finished=False,
        dag=dag,
    )

    return dag
