from google.cloud import bigquery


client = bigquery.Client()
project_id = 'xed-project-237404'
bqclient = bigquery.Client(project=project_id)


def query_bigquery(query_string: str):
    print('query_string', query_string)
    dataframe = (
        bqclient.query(query_string)
            .result()
            .to_dataframe(create_bqstorage_client=False)
    )
    return dataframe
