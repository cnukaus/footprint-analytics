from dex.etl_dex_pair_bsc import DexPairBSC
from dex.etl_dex_pair_ethereum import DexPairEthereum
from utils.build_dag_util import BuildDAG

task1 = DexPairBSC()
task2 = DexPairEthereum()

DAG1 = BuildDAG().build_dag_with_ops(dag_params=task1.airflow_dag_params(), ops=task1.airflow_steps())
DAG2 = BuildDAG().build_dag_with_ops(dag_params=task2.airflow_dag_params(), ops=task2.airflow_steps())
