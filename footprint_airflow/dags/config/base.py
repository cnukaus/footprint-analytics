"""BaseConfig(default) config"""
import os


class BaseConfig(object):
    environment = 'default'

    bigquery_bucket_name = 'footprint_bigquery_test'

    bigquery_database = 'footprint_test'

    mongodb_defi_up_uri = 'mongodb://localhost:27017'

    mongodb_footprint_monitor_uri = "mongodb://localhost:27017"

    dags_folder = os.getcwd()

    db_name = 'defi_up'
