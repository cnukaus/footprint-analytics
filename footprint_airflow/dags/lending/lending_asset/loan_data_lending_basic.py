from basic.etl_basic import ETLBasic
import moment
import os
from datetime import timedelta, datetime
from config import project_config
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.query_bigquery import query_bigquery
from models import BigQueryCheckPoint


class LoanDataLendingBasic(ETLBasic):

    def __init__(self, execution_date=None, date_filter=None):
        super().__init__(execution_date)
        if date_filter is None:
            date_filter = "= '{}'".format(self.execution_date.strftime("%Y-%m-%d"))

        self.date_filter = date_filter

    def get_execution_date(self):
        now = moment.utcnow().datetime
        yesterday = now - timedelta(days=1)  # yesterday time
        execution_date = yesterday.strftime("%Y-%m-%d")
        print('now', now)
        print('yesterday', yesterday)
        print('execution_date', execution_date)
        return yesterday

    def build_query_sql(self):
        return ''

    def get_lending_from_big_query_to_gsc(self):
        query_string = self.build_query_sql()
        dataframe = query_bigquery(query_string)
        return dataframe

    def get_csv_file_name(self):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, '../data/lending/{}-{}.csv'.format(self.task_name, self.execution_date_str))

    def do_scrapy_data(self):
        if os.path.isfile(self.get_csv_file_name()):
            return
        df = self.get_lending_from_big_query_to_gsc()
        df.to_csv(self.get_csv_file_name(), index=False)

    def do_upload_csv_to_gsc(self):
        source_csv_file = self.get_csv_file_name()
        destination_file_path = '{name}/date={execution_date}/{name}_{execution_date}.csv'.format(
            name=self.task_name,
            execution_date=self.execution_date_str)
        if BigQueryCheckPoint.has_check_point(destination_file_path, self.execution_date_str):
            return False
        upload_csv_to_gsc(source_csv_file, destination_file_path)
        BigQueryCheckPoint.set_check_point(destination_file_path, self.execution_date_str)

    def airflow_dag_params(self):
        dag_params = {
            "dag_id": "footprint_{}_dag".format(self.task_name),
            "catchup": False,
            "schedule_interval": '0 2 * * *',
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
        print('dag_params', dag_params)
        return dag_params

    def airflow_steps(self):
        return [
            self.do_start,
            self.do_scrapy_data,
            self.do_upload_csv_to_gsc,
            self.do_import_gsc_to_bigquery,
            self.do_common_valid,
            self.do_customize_valid,
            self.do_end,
        ]
