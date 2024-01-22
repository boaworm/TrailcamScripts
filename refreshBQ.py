from google.cloud import bigquery
from google.cloud import storage
import pandas
import pytz
import json

f = open("refreshBQ.conf.json")
config = json.load(f)

service_account_key = config["google_cloud_service_account_key"]

def refresh_BQ_oklahoma_observations():
    f = open("Web/oklahoma_observations.json")
    records = json.load(f)

    """
    records = [
    {
        "date": "2023-12-07",
        "date_time": "2023-12-07 00:47:00",
        "year": "2023",
        "month": "12",
        "day": 7,
        "camera": "HOUSE",
        "hour": 0,
        "minute": 47,
        "second": 0,
        "image": "HOUSE_2023-12-07T004700_PICT1123_202312070759V2Y2I.JPG",
        "image_path": "2023-12-07",
        "count": 0,
        "confidence": []
    },
    {
        "date": "2023-12-07",
        "date_time": "2023-12-07 02:07:00",
        "year": 2021,
        "month": 12,
        "day": 7,
        "camera": "MIDDLE",
        "hour": 2,
        "minute": 7,
        "second": 00,
        "image": "MIDDLE_2023-12-07T020700_PICT1477_202312070901CFXWW.JPG",
        "image_path": "2023-12-07",
        "count": 2,
        "confidence": [
            0.7093038558959961,
            0.9000000000000000
        ]
    }
    ]
    """

    client = bigquery.Client.from_service_account_json(service_account_key)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("date_time", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("month", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("day", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("camera", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hour", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("minute", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("second", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("image", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("image_path", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("count", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("confidence",bigquery.enums.SqlTypeNames.FLOAT,mode="REPEATED"),
            ],
        write_disposition="WRITE_TRUNCATE"
    )

    dataframe = pandas.DataFrame(
        records,
        columns=[
            "date",
            "date_time",
            "year",
            "month",
            "day",
            "camera",
            "hour",
            "minute",
            "second",
            "image",
            "image_path",
            "count",
            "confidence",
            ]
    
            )
    dataframe["date"] = pandas.to_datetime(dataframe["date"])
    dataframe["date_time"] = pandas.to_datetime(dataframe["date_time"])
    dataframe["year"] = pandas.to_numeric(dataframe["year"])
    dataframe["month"] = pandas.to_numeric(dataframe["month"])
    dataframe["day"] = pandas.to_numeric(dataframe["day"])
    dataframe["hour"] = pandas.to_numeric(dataframe["hour"])
    dataframe["minute"] = pandas.to_numeric(dataframe["minute"])
    dataframe["second"] = pandas.to_numeric(dataframe["second"])
    dataframe["count"] = pandas.to_numeric(dataframe["count"])
    table_id = config["animal_obs_table_id"]

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )

    table = client.get_table(table_id)
    print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

def refresh_BQ_deer_observations():
    f = open("Web/deer_observations.json")
    records = json.load(f)

    """
    records =
    [
        {
            "image": "TOP_2023-12-07T181500_PICT3856_202312080200AZTAN.JPG",
            "confidence": 0.9999928753738866,
            "classification": "1.Deer"
        },
        {
            "image": "TOP_2023-12-07T192500_PICT3861_202312080200JHETQ.JPG",
            "classification": "0.NoDeer",
            "confidence": 0.9691270361652979
        },
    ]
    """

    client = bigquery.Client.from_service_account_json(service_account_key)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("image", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("confidence",bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("classification", bigquery.enums.SqlTypeNames.STRING),
            ],
        write_disposition="WRITE_TRUNCATE"
    )

    dataframe = pandas.DataFrame(
        records,
        columns=[
            "image",
            "confidence",
            "classification",
            ]
    )

    table_id = config["deer_obs_table_id"]

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )

    table = client.get_table(table_id)
    print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

def refresh_BQ_timeseries():
    f = open("Web/time_series_dimension.json")
    records = json.load(f)

    """
       [
       {
           "date": "2023-12-01"
       },
       {
           "date": "2023-12-02"
       },
    """

    client = bigquery.Client.from_service_account_json(service_account_key)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
            ],
        write_disposition="WRITE_TRUNCATE"
    )

    dataframe = pandas.DataFrame(
        records,
        columns=[
            "date",
        ]
    )

    table_id = config["time_series_table_id"]

    dataframe["date"] = pandas.to_datetime(dataframe["date"])

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )

    table = client.get_table(table_id)
    print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)



refresh_BQ_oklahoma_observations()
refresh_BQ_deer_observations()
refresh_BQ_timeseries()
