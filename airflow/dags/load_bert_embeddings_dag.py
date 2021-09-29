from __future__ import print_function

from ngrametl_airflow.build_bert_embeddings_dag import build_bert_embeddings_dag
from ngrametl_airflow.variables import (
    read_var,
    read_bert_embeddings_dag_vars,
)

datasets = read_var("input_datasets", required=True, input_datasets="")
load_vars = read_bert_embeddings_dag_vars(
    load_max_active_runs=1,
    batch_size=1000,
)
for dataset in datasets.split(","):
    dag_id = f"load_{dataset}_bert_embeddings"
    globals()[dag_id] = build_bert_embeddings_dag(
        dag_id=dag_id,
        ngram_dataset=dataset,
        input_table=f"{dataset}_bert_preprocessed",
        output_table=f"{dataset}_bert_embeddings",
        **load_vars,
    )
