from google.cloud import storage
from config import project_config

bucket_name = project_config.bigquery_bucket_name


def upload_csv_to_gsc(source_file_name, destination_file_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_file_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_file_name
        )
    )
