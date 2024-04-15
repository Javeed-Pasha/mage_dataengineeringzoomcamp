if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    bucket_name = kwargs['context']['bucket_name']
    project_id = kwargs['context']['project_id']
    bigquery_dataset = kwargs['context']['bigquery_dataset']

    df = kwargs['spark'] \
        .read \
        .parquet(f'gs://{bucket_name}/bike_dataset/raw/nomenclature/*') 
    

    from pyspark.sql.functions import regexp_extract, col, when
    pattern = r'^\((.*?)\)\s*(.*)$'  # Matches "(XXX-XXX) Any text"

# Apply regexp_extract to split the input string into two columns
    data = df \
            .withColumn("station_name", regexp_extract(col("name"), pattern, 2)) \
            .withColumn("station_name", when(col("station_name") == "", 'Juan Manuel / C.General Coronado').otherwise(col("station_name"))) \
            .select('id','station_name','obcn','location','latitude','longitude','status')
            
    data.write \
            .format("bigquery") \
            .option("project", project_id) \
            .option("dataset", bigquery_dataset) \
            .option("table", "Dim_Stations") \
            .mode("overwrite") \
            .save()
    # Specify your data exporting logic here


