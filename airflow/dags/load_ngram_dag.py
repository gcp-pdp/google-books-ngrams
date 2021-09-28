from __future__ import print_function

from ngrametl_airflow.build_load_ngram_dag import build_load_ngram_dag
from ngrametl_airflow.variables import read_load_dag_vars, read_var

datasets = read_var("input_datasets", required=True, input_datasets="")
load_vars = read_load_dag_vars(
    load_max_active_runs=1,
)
for dataset in datasets.split(","):
    prefix = dataset.replace("-", "_")
    for n in range(1, 6):
        dag_id = "load_{prefix}_{n}_gram".format(prefix=prefix, n=n)
        globals()[dag_id] = build_load_ngram_dag(
            dag_id=dag_id,
            dataset=dataset,
            ngram=n,
            input_file="gs://books/ngrams/books/20200217/{dataset}/{ngram}-*.gz".format(
                dataset=dataset, ngram=n
            ),
            output_table="{prefix}_{ngram}".format(prefix=prefix, ngram=n),
            **load_vars
        )
