from utils.build_dag_util import BuildDAG
from lending.lending_asset.covalent_lending import CovalentLending

task = CovalentLending()

DAG = BuildDAG().build_dag_with_ops(dag_params=task.airflow_dag_params(), ops=task.airflow_steps())
