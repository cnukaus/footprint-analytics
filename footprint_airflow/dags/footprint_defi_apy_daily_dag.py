from defi_apy.king_data.king_data_apy_daily import ApyDaily
from utils.build_dag_util import BuildDAG

task = ApyDaily()

params = task.airflow_dag_params()
DAG = BuildDAG().build_dag_with_ops(dag_params=task.airflow_dag_params(), ops=task.airflow_steps())