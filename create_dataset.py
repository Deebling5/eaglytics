from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "eaglytics-340010.Assessment.assessment"

job_config = bigquery.LoadJobConfig(
    #schema=[
    #    bigquery.SchemaField("name", "STRING"),
    #    bigquery.SchemaField("post_abbr", "STRING"),
    #],
    autodetect=True,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
uri = "gs://eaglytics_assessment_abhijay/data_orders"

load_job = client.load_table_from_uri(
    uri,
    table_id,
    location="US",  # Must match the destination dataset location.
    job_config=job_config,
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))
