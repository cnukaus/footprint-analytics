"""develop(default) config"""
from config.base import BaseConfig
import os


class RestoreConfig(BaseConfig):
    environment = 'restore'

    dags_folder = os.getcwd()

    bigquery_bucket_name = 'footprint_bigquery_test'

    bigquery_database = 'footprint_test'

    bigquery_etl_database = 'footprint_test'

    db_name = 'footprint_etl'

    mongodb_defi_up_uri = 'xxxx'

