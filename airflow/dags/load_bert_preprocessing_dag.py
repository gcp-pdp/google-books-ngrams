from __future__ import print_function

from ngrametl_airflow.build_bert_preprocessing_dag import build_bert_preprocessing_dag
from ngrametl_airflow.variables import (
    read_var,
    read_bert_preprocessing_dag_vars,
)

datasets = read_var("input_datasets", required=True, input_datasets="")
load_vars = read_bert_preprocessing_dag_vars(
    load_max_active_runs=1,
)
for dataset in datasets.split(","):
    dag_id = f"load_{dataset}_bert_preprocessing"
    globals()[dag_id] = build_bert_preprocessing_dag(
        dag_id=dag_id,
        ngram_dataset=dataset,
        input_table=",".join([f"{dataset}_{n}" for n in range(1, 6)]),
        output_table=f"{dataset}_bert_preprocessed",
        **load_vars,
    )
