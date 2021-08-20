"""develop(default) config"""
from config.base import BaseConfig


class DevelopConfig(BaseConfig):
    environment = 'develop'

    dags_folder = '/home/airflow/gcs/dags'

    bigquery_bucket_name = 'footprint_bigquery_test'

    bigquery_database = 'footprint_test'

    mongodb_defi_up_uri = 'xxxx'

    mongodb_footprint_monitor_uri = "xxxx"
