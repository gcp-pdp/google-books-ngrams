from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
import os

from google.cloud import bigquery

from ngrametl_airflow.bigquery_utils import submit_bigquery_job
from ngrametl_airflow.file_utils import read_file


def build_load_ngram_dag(
    dag_id,
    dataset,
    ngram,
    input_file,
    output_table,
    dataset_project_id,
    dataset_name,
    dataflow_template_path,
    dataflow_environment,
    notification_emails=None,
    load_start_date=datetime(2021, 1, 1),
    load_max_active_runs=1,
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
    )

    dataflow_operator = DataflowStartFlexTemplateOperator(
        task_id="load_ngram",
        body={
            "launchParameter": {
                "containerSpecGcsPath": dataflow_template_path,
                "jobName": "load-{dataset}-{ngram}".format(
                    dataset=dataset.replace("_", "-"), ngram=ngram
                )
                + '-{{ execution_date.strftime("%Y%m%d-%H%M%S") }}',
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

    def enrich_task(**context):
        client = bigquery.Client()
        dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")

        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.INTERACTIVE

        sql_path = os.path.join(dags_folder, "resources/stages/load/sqls/enrich.sql")
        sql_template = read_file(sql_path)

        template_context = {
            "table": output_table,
            "project_id": dataset_project_id,
            "dataset": dataset_name,
        }

        sql = context["task"].render_template(sql_template, template_context)
        job = client.query(sql, job_config=job_config, timeout=7200)
        submit_bigquery_job(job, job_config)
        assert job.state == "DONE"

    return dag
