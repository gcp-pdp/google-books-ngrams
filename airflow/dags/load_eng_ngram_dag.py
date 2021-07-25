from __future__ import print_function

from wordetl_airflow.build_load_ngram_dag import build_load_ngram_dag
from wordetl_airflow.variables import read_load_dag_vars

for i in range(5):
    n = i + 1
    dag_id = f"load_eng_{n}_gram_dag"
    globals()[dag_id] = build_load_ngram_dag(
        dag_id=dag_id,
        input_file=f"gs://books/ngrams/books/20200217/eng/{n}-*.gz",
        output_table=f"eng_{n}",
        **read_load_dag_vars(
            load_schedule_interval=None,
            load_max_active_runs=1,
        ),
    )
