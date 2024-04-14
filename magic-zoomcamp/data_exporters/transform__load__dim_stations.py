if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """

    df = kwargs['spark'] \
        .read \
        .parquet('gs://zoomcamp_b/bike_dataset/raw/nomenclature/*') 
    

    from pyspark.sql.functions import regexp_extract, col, when
    pattern = r'^\((.*?)\)\s*(.*)$'  # Matches "(XXX-XXX) Any text"

# Apply regexp_extract to split the input string into two columns
    data = df \
            .withColumn("station_name", regexp_extract(col("name"), pattern, 2)) \
            .withColumn("station_name", when(col("station_name") == "", 'Juan Manuel / C.General Coronado').otherwise(col("station_name"))) \
            .select('id','station_name','obcn','location','latitude','longitude','status')
            
    data.write \
            .format("bigquery") \
            .option("project", "forward-ace-411913") \
            .option("dataset", "zoomcamp_bigquery") \
            .option("table", "Dim_Stations") \
            .mode("overwrite") \
            .save()
    # Specify your data exporting logic here


