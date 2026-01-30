from pyspark.sql.functions import monotonically_increasing_id,  lit, concat_ws, col
from common import write_to_postgres, write_to_gold

def process_dim_location(df_silver):
    print("Processing Dim_Location...")

  
    current_location = df_silver.select(
        col("road_street").alias("street"),
        col("road_district").alias("district"),
        col("road_city").alias("city"),
    )


    dest_location = df_silver.select(
        col("destination_street").alias("street"),
        col("destination_district").alias("district"),
        col("destination_city").alias("city"),
    )
     # Note: unionByName requires that the column names in both DataFrames must match exactly.
    dim_location = current_location.unionByName(dest_location) \
        .withColumn("postal_code", lit('00700')) \
        .withColumn("country", lit("Vietnam")) \
        .dropDuplicates() \
        .withColumn("location_id", monotonically_increasing_id())

    write_to_gold(dim_location, "dim_location", "overwrite")
    write_to_postgres(dim_location, "dim_location", "overwrite")
    print(f"Dim_Location completed: {dim_location.count()} records")
    return dim_location