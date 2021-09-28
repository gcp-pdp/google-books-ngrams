from datetime import datetime
from airflow.models import Variable


def read_load_ngram_dag_vars(**kwargs):
    """Read Airflow variables for Load NGram DAG"""
    var = read_var("ngram", required=True, deserialize_json=True)
    return {
        "dataflow_template_path": get_str(var, "dataflow_template_path", **kwargs),
        "dataflow_environment": get_str(var, "dataflow_environment", **kwargs),
        "dataset_project_id": get_str(var, "dataset_project_id", **kwargs),
        "dataset_name": get_str(var, "dataset_name", **kwargs),
        "notification_emails": get_str(var, "notification_emails", **kwargs),
        "load_max_active_runs": get_int(var, "load_max_active_runs", **kwargs),
    }


def read_bert_preprocessing_dag_vars(**kwargs):
    """Read Airflow variables for BERT preprocessing DAG"""
    var = read_var("bert_preprocessing", required=True, deserialize_json=True)
    return {
        "input_dataset_project_id": get_str(var, "input_dataset_project_id", **kwargs),
        "input_dataset_name": get_str(var, "input_dataset_name", **kwargs),
        "output_dataset_project_id": get_str(
            var, "output_dataset_project_id", **kwargs
        ),
        "output_dataset_name": get_str(var, "output_dataset_name", **kwargs),
        "dataflow_template_path": get_str(var, "dataflow_template_path", **kwargs),
        "dataflow_environment": get_str(var, "dataflow_environment", **kwargs),
        "notification_emails": get_str(var, "notification_emails", **kwargs),
        "load_max_active_runs": get_int(var, "load_max_active_runs", **kwargs),
    }


def read_var(
    var_name, var_prefix=None, required=False, deserialize_json=False, **kwargs
):
    """Read Airflow variable"""
    full_var_name = f"{var_prefix}{var_name}" if var_prefix is not None else var_name
    var = Variable.get(full_var_name, default_var="", deserialize_json=deserialize_json)
    if var == "":
        var = None
    if var_prefix and var is None:
        var = read_var(var_name, None, required, deserialize_json, **kwargs)
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f"{full_var_name} variable is required")
    return var


def get_str(dict_var, key, **kwargs):
    var = dict_var.get(key)
    return var if var is not None else kwargs.get(key)


def get_date(dict_var, key, **kwargs):
    date_string = dict_var.get(key)
    return (
        datetime.strptime(date_string, "%Y-%m-%d")
        if date_string is not None
        else kwargs.get(key)
    )


def get_int(dict_var, key, **kwargs):
    int_string = dict_var.get(key)
    return int(int_string) if int_string is not None else kwargs.get(key)
