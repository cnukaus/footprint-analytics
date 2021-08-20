"""develop(default) config"""
from config.base import BaseConfig


class ProductionConfig(BaseConfig):
    environment = 'production'

    dags_folder = '/home/airflow/gcs/dags'

    bigquery_bucket_name = 'footprint_bigquery'

    bigquery_database = 'footprint'

    bigquery_etl_database = 'footprint_etl'

    db_name = 'footprint_etl'

    mongodb_defi_up_uri = 'xxxxx'

    mongodb_footprint_monitor_uri = 'xxxx'

    mongodb_footprint_uri = 'xxxx'
