from google.cloud import bigquery

# Construct a BigQuery client object.
service_account_key="your-service-account-key.json"

client = bigquery.Client.from_service_account_json(service_account_key)

query = """
    SELECT date, count
    FROM `your-project-1234.oklahoma_trailcam.oklahoma_trailcam_observations`
    LIMIT 20
"""
rows = client.query_and_wait(query)  # Make an API request.

print("The query data:")
for row in rows:
    # Row values can be accessed by field name or index.
    print("date={}, count={}".format(row[0], row["count"]))
