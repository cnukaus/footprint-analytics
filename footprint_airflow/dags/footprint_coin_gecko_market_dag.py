from utils.build_dag_util import BuildDAG
from token_stats.coin_gecko.coin_gecko_token_market_etl import CoinGeckoTokenMarketETL

task = CoinGeckoTokenMarketETL()

DAG = BuildDAG().build_dag_with_ops(dag_params=task.airflow_dag_params(), ops=task.airflow_steps())
