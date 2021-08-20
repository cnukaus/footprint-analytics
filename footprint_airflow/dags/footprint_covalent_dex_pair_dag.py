from utils.build_dag_util import BuildDAG
from dex.etl_covalent_dex_pair import ETLCovalentDexPair

task = ETLCovalentDexPair()

DAG = BuildDAG().build_dag_with_ops(dag_params=task.airflow_dag_params(), ops=task.airflow_steps())
