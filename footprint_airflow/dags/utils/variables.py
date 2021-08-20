from datetime import datetime

from airflow.models import Variable


def read_dag_var(var_prefix, **kwargs):
    pass


def read_var(var_name, var_prefix=None, required=False, **kwargs):
    full_var_name = f'{var_prefix}{var_name}' if var_prefix is not None else var_name
    var = Variable.get(full_var_name, '')
    var = var if var != '' else None
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f'{full_var_name} variable is required')
    return var


def parse_bool(bool_string, default=True):
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ["true", "yes"]


def parse_int(val):
    if val is None:
        return None
    return int(val)
