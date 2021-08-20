from datetime import timedelta, datetime
from utils.build_dag_util import BuildDAG
from defi_protocol.defi_llama.defi_llama_scrapy_etl import DefiLlamaScrapy


def python_callable():
    DefiLlamaScrapy().exec()


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 7, 1)
}

dag_params = {
    "dag_id": "footprint_defi_llama",
    "catchup": False,
    "schedule_interval": '0 23 * * *',
    "description": "get defi llama scrapy DAG",
    "default_args": default_dag_args,
    "dagrun_timeout": timedelta(days=30)
}

dag_task_params = [
    {
        "task_id": "defi_llama_scrapy",
        "python_callable": python_callable,
        "execution_timeout": timedelta(minutes=60 * 12)
    }
]

DAG = BuildDAG().build_dag(dag_params=dag_params, dag_task_params=dag_task_params)
