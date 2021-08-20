from utils.bigquery_utils import read_bigquery_schema_from_file
from google.cloud import bigquery
import os
from config import project_config

bucket_name = project_config.bigquery_bucket_name
data_base = project_config.bigquery_etl_database

client = bigquery.Client()


def load_csv_to_bigquery(table_id, csv_file, schema):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
        write_disposition='WRITE_TRUNCATE'
    )

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def import_gsc_to_bigquery(name, custom_data_base: str = None):
    if not custom_data_base:
        custom_data_base = data_base
    bigquery_table_name = 'xed-project-237404.{data_base}.{name}'.format(data_base=custom_data_base, name=name)
    job_config = bigquery.LoadJobConfig()
    uri = "gs://{bucket_name}/{gsc_folder}/*.csv".format(bucket_name=bucket_name, gsc_folder=name)
    schema = get_schema(name)
    job_config.skip_leading_rows = 1
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = 'WRITE_TRUNCATE'
    load_job = client.load_table_from_uri(
        uri, bigquery_table_name, job_config=job_config
    )
    load_job.result()

    destination_table = client.get_table(bigquery_table_name)
    print("Loaded {} rows.".format(destination_table.num_rows))


def get_schema(schema_name):
    dags_folder = project_config.dags_folder
    schema_path = os.path.join(dags_folder, 'resources/stages/raw/schemas/{schema_name}.json'.format(schema_name=schema_name))
    schema = read_bigquery_schema_from_file(schema_path)
    return schema
