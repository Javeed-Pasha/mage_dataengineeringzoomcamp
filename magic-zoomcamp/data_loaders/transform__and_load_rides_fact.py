if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col,avg
    from pyspark.sql import types
    bucket_name = kwargs['context']['bucket_name']
    project_id = kwargs['context']['project_id']
    bigquery_dataset = kwargs['context']['bigquery_dataset']
    rides_schema = types.StructType([
        types.StructField('Trip_Id', types.LongType(),True),
        types.StructField('User_Id', types.LongType(),True),
        types.StructField('Gender', types.StringType(),True),
        types.StructField('Year_of_Birth', types.LongType(),True),
        types.StructField('Trip_starttime', types.TimestampType(),True),
        types.StructField('Trip_endtime', types.TimestampType(),True),
        types.StructField('Origin_Id', types.LongType(),True),
        types.StructField('Destination_Id', types.LongType(), True)
    ])
    df = kwargs['spark'] \
            .read \
            .schema(rides_schema) \
            .parquet(f'gs://{bucket_name}/bike_dataset/raw/rides/*/*/') 

    avg_year_of_birth = df.select(avg(col('Year_of_Birth'))).collect()[0][0]

    data = df \
        .withColumn('Trip_start_date', F.to_date(df.Trip_starttime)) \
        .withColumn('Trip_end_date', F.to_date(df.Trip_endtime)) \
        .withColumn('DurationDays', F.datediff(df.Trip_endtime, df.Trip_starttime)) \
        .withColumn('Duration_Mins', F.round((F.unix_timestamp(df.Trip_endtime) - F.unix_timestamp(df.Trip_starttime)) / 60, 2)) \
        .na.fill({'Gender': 'NOT_SPECIFIED'}) \
        .fillna({'Year_of_Birth': avg_year_of_birth}) \
        .filter(col('DurationDays') <1) \
        .select('Trip_Id','User_Id','Gender','Year_of_Birth', 'Trip_starttime', 'Trip_endtime', 'Origin_Id', 'Destination_Id','Duration_Mins')

    # data.show()
    data.write \
        .format("bigquery") \
        .option("project", project_id) \
        .option("dataset", bigquery_dataset) \
        .option("partitionField", "Trip_starttime") \
        .option("partitionType","MONTH") \
        .option("table", "Rides_Fact") \
        .mode("overwrite") \
        .save()

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
