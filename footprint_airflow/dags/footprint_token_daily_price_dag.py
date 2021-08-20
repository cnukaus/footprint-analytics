from token_stats.coin_paprika.etl_token_daily_price import TokenDailyPrice
from utils.build_dag_util import BuildDAG

task = TokenDailyPrice()

DAG = BuildDAG().build_dag_with_ops(dag_params=task.airflow_dag_params(), ops=task.airflow_steps())
