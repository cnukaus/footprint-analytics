from token_stats.coin_gecko.coin_gecko_token_history import get_history_market_from_coin_gecko
from airflow import models
from datetime import timedelta
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 1)
}

dag = models.DAG(
    dag_id='run_coin_gecko_history_market',
    catchup=False,
    default_args=default_dag_args,
    dagrun_timeout=timedelta(days=30)
)

data_operator = PythonOperator(
    task_id='coin_gecko_history_{task}'.format(task='get_data'),
    python_callable=get_history_market_from_coin_gecko,
    execution_timeout=timedelta(days=180),
    dag=dag
)


