import subprocess
import os
import json
from datetime import timedelta, datetime
from config import project_config
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.import_gsc_to_bigquery import load_csv_to_bigquery
from utils.bigquery_utils import read_bigquery_schema_from_file
import sys


class ImportFromMongo():

    def __init__(self):
        self.mongodb_uri = project_config.mongodb_footprint_uri
        self.temp_csv_dir = os.path.join(project_config.dags_folder, 'migrate_data/footprint/csv')
        self.gen_dir()
        self.dataset_id = 'xed-project-237404.metabase'
        self.schema_json_path = os.path.join(project_config.dags_folder, 'migrate_data/footprint/schemas')
        self.task_name = 'migrate_footprint_mongo_data'
        self.task_airflow_execution_time = '*/5 * * * *'
        self.schema_info =[]

    def gen_dir(self):
        if not os.path.exists(self.temp_csv_dir):
            os.mkdir(self.temp_csv_dir)


    def gen_schema_info(self):
        for file in os.listdir(self.schema_json_path):
            file_path = os.path.join(self.schema_json_path, file)
            fields = []
            with open(file_path) as f:
                data = json.load(f)
                for i in data:
                    fields.append(i.get('description'))
            field = ','.join(fields)
            self.schema_info.append({'name': file.split('.json')[0], 'fields':field})

    def export_csv(self, name, field, source_path):
        subprocess.call(
            f'mongoexport --uri="{self.mongodb_uri}" --collection="{name}" --type=csv --fields="{field}" --out="{source_path}"',
            shell=True)

    def trans_objectid_to_string(self, source_file_path):
        if sys.platform == 'darwin':
            subprocess.call(["sed", "-i","", r"s/ObjectId(\([[:alnum:]]*\))/\1/g", f"{source_file_path}"])
        else:
            subprocess.call(["sed", "-i", r"s/ObjectId(\([[:alnum:]]*\))/\1/g", f"{source_file_path}"])

    def get_schema(self,schema_name):
        schema_path = f'{self.schema_json_path}/{schema_name}.json'
        schema = read_bigquery_schema_from_file(schema_path)
        return schema

    def remove_temp_csv(self):
        pass

    def exc(self):
        for info in self.schema_info:
            schema_name, schema_fields = info.get('name'), info.get('fields')
            source_file_path = os.path.join(self.temp_csv_dir, f'{schema_name}.csv')
            destination_file_path = f'footprint_prod_{schema_name}/{schema_name}.csv'
            self.export_csv(schema_name, schema_fields, source_file_path)
            self.trans_objectid_to_string(source_file_path)
            upload_csv_to_gsc(source_file_path, destination_file_path)
            table_name = f'{self.dataset_id}.footprint_prod_{schema_name}'
            schema = self.get_schema(schema_name)
            print(table_name,schema)
            load_csv_to_bigquery(table_name,source_file_path,schema)

    def airflow_dag_params(self, task_airflow_execution_time: str = None):
        dag_params = {
            "dag_id": "footprint_{}_dag".format(self.task_name),
            "catchup": False,
            "schedule_interval": task_airflow_execution_time or self.task_airflow_execution_time,
            "description": "{}_dag".format(self.task_name),
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 7, 1)
            },
            "dagrun_timeout": timedelta(days=30)
        }
        return dag_params

    def airflow_steps(self):
        return [
            self.gen_schema_info,
            self.exc,
            self.remove_temp_csv
        ]
