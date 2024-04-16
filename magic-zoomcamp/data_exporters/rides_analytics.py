from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path
from pyspark.sql import functions as F
from pyspark.sql.functions import col,avg,concat,lit
from pyspark.sql.window import Window

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
# def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
def export_data_to_big_query(data, *args, **kwargs):
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    bucket_name = kwargs['context']['bucket_name']
    project_id = kwargs['context']['project_id']
    bigquery_dataset = kwargs['context']['bigquery_dataset']

    # Read the tables into DataFrames
    rides_fact = kwargs['spark'].read.format("bigquery") \
        .option("table", "forward-ace-411913.zoomcamp_bigquery.Rides_Fact") \
        .load()

    dim_stations = kwargs['spark'].read.format("bigquery") \
        .option("table", "forward-ace-411913.zoomcamp_bigquery.Dim_Stations") \
        .load()

    # Join the DataFrames and perform calculations
    from_loc = dim_stations.alias("from_loc")
    to_loc = dim_stations.alias("to_loc")

    df = rides_fact \
        .join(from_loc, rides_fact["Origin_Id"] == from_loc["id"]) \
        .join(to_loc, rides_fact["Destination_Id"] == to_loc["id"]) \
        .withColumn("Route", concat(col("from_loc.obcn"), lit(" to "), col("to_loc.obcn"))) \
        .selectExpr("Trip_Id","User_Id","Gender","Year_of_Birth","Trip_starttime","Trip_endtime","Origin_Id","Destination_Id","Duration_Mins","Route") \
        .withColumn("Avg_Ride_Duration_By_Route", F.round(avg("Duration_Mins").over(Window.partitionBy("Route")),2))


    # table_id = f'{project_id}.{bigquery_dataset}.rides_analytics'
    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'default'

    # BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
    #     df,
    #     table_id,
    #     if_exists='replace',  # Specify resolution policy if table name already exists
    # )
# [
#   "export_data_to_big_query() takes 1 positional argument but 2 were given"
# ]
    df.write \
        .format("bigquery") \
        .option("project", project_id) \
        .option("dataset", bigquery_dataset) \
        .option("partitionField", "Trip_starttime") \
        .option("partitionType","MONTH") \
        .option("table", "rides_analytics") \
        .mode("overwrite") \
        .save()

