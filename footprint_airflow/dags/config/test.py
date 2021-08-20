"""test config"""
import os
from config.base import BaseConfig


class TestConfig(BaseConfig):
    dags_folder = os.getcwd()

    bigquery_bucket_name = 'footprint_bigquery_test'

    bigquery_database = 'footprint_test'

    mongodb_defi_up_uri = 'mongodb://localhost:27017'
