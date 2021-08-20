from google.cloud import bigquery

from utils.import_gsc_to_bigquery import get_schema

big_query_client = bigquery.Client()


def load_to_bigquery(table_id, csv_file, schema):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
        write_disposition='WRITE_TRUNCATE'
    )

    with open(csv_file, "rb") as source_file:
        job = big_query_client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.
    table = big_query_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def upload_all():
    for file_name in [
        # 'erc20_tokens',
        # 'erc20_stablecoins',
        # 'compound_view_ctokens',
        # 'makermcd_collateral_addresses',
        'yearn_vaults',
    ]:
        table_name = 'xed-project-237404.footprint_etl.{}'.format(file_name)
        csv_path = './{}.csv'.format(file_name)
        schema = get_schema(file_name)
        print(table_name, csv_path)
        load_to_bigquery(table_name, csv_path, schema)


if __name__ == '__main__':
    upload_all()
