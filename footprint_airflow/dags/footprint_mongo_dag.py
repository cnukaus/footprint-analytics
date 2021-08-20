from utils.build_dag_util import BuildDAG
from migrate_data.footprint.import_from_mongo import ImportFromMongo
importFromMongo = ImportFromMongo()
DAG = BuildDAG().build_dag_with_ops(dag_params=importFromMongo.airflow_dag_params(), ops=importFromMongo.airflow_steps())
