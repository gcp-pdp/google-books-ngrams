from __future__ import print_function

from wordetl_airflow.build_load_ngram_dag import build_load_ngram_dag
from wordetl_airflow.variables import read_load_dag_vars, read_var

datasets = read_var("input_datasets", required=True, input_datasets="eng")
for dataset in datasets.split(","):
    prefix = dataset.replace("-", "_")
    for i in range(5):
        n = i + 1
        dag_id = f"load_{prefix}_{n}_gram_dag"
        globals()[dag_id] = build_load_ngram_dag(
            dag_id=dag_id,
            n_gram=n,
            **read_load_dag_vars(
                var_prefix=f"{prefix}_",
                input_file="gs://books/ngrams/books/20200217/{dataset}/{n}-*.gz".format(
                    dataset=dataset, n=n
                ),
                output_table="{prefix}_{n}".format(prefix=prefix, n=n),
                load_schedule_interval=None,
                load_max_active_runs=1,
            ),
        )
