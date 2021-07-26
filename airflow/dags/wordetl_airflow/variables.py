from datetime import datetime
from airflow.models import Variable


def read_load_dag_vars(var_prefix="", **kwargs):
    """Read Airflow variables for Load DAG"""
    load_max_active_runs = read_var("load_max_active_runs", var_prefix, False, **kwargs)
    load_max_active_runs = (
        int(load_max_active_runs) if load_max_active_runs is not None else None
    )

    vars = {
        "dataflow_template_path": read_var(
            "dataflow_template_path", var_prefix, True, **kwargs
        ),
        "dataflow_environment": read_var(
            "dataflow_environment", var_prefix, True, True, **kwargs
        ),
        "dataset_project_id": read_var(
            "dataset_project_id", var_prefix, True, **kwargs
        ),
        "dataset_name": read_var("dataset_name", var_prefix, True, **kwargs),
        "input_file": read_var("input_file", var_prefix, True, **kwargs),
        "output_table": read_var("output_table", var_prefix, True, **kwargs),
        "notification_emails": read_var("notification_emails", None, False, **kwargs),
        "load_schedule_interval": read_var(
            "load_schedule_interval", var_prefix, False, **kwargs
        ),
        "load_max_active_runs": load_max_active_runs,
    }

    load_start_date = read_var("load_start_date", var_prefix, False, **kwargs)
    if load_start_date is not None:
        vars["load_start_date"] = datetime.strptime(load_start_date, "%Y-%m-%d")

    load_end_date = read_var("load_end_date", var_prefix, False, **kwargs)
    if load_end_date is not None:
        vars["load_end_date"] = datetime.strptime(load_end_date, "%Y-%m-%d")

    return vars


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
