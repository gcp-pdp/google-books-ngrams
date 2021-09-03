from datetime import datetime, timedelta
from airflow import models, configuration
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
import os

from google.cloud import bigquery

from wordetl_airflow.bigquery_utils import submit_bigquery_job
from wordetl_airflow.file_utils import read_file


def build_load_ngram_dag(
    dag_id,
    n_gram,
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

    table_name = output_table.format(n=n_gram)

    dataflow_operator = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_streaming_beam_sql",
        body={
            "launchParameter": {
                "containerSpecGcsPath": f"{dataflow_template_path}/ngrams-beam.json",
                "jobName": 'ngram-beam-{{ execution_date.strftime("%Y%m%d-%H%M%S") }}',
                "parameters": {
                    "input-file": input_file.format(n=n_gram),
                    "output-table": f"{dataset_project_id}:{dataset_name}.{table_name}",
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
            "table": table_name,
            "project_id": dataset_project_id,
            "dataset": dataset_name,
        }

        sql = context["task"].render_template(sql_template, template_context)
        job = client.query(sql, job_config=job_config, timeout=7200)
        submit_bigquery_job(job, job_config)
        assert job.state == "DONE"

    enrich_operator = PythonOperator(
        task_id="enrich",
        python_callable=enrich_task,
        provide_context=True,
        execution_timeout=timedelta(minutes=130),
        retries=0,
        dag=dag,
    )

    dataflow_operator >> enrich_operator

    return dag
