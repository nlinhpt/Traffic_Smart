from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.types import FloatType, LongType
from pyspark.sql.window import Window
from common import write_to_postgres, write_to_gold

def process_dim_vehicle(df_silver):
    """Process dim_vehicle from cached silver DataFrame"""
    print("Processing Dim_Vehicle...")
    
    w = Window.partitionBy("license_number").orderBy(col("timestamp").desc())
    
    df_vehicle = df_silver \
        .withColumn("rn", row_number().over(w)) \
        .filter(col("rn") == 1) \
        .select(
            # col("vehicle_id"),
            # col("owner_name"),
            col("license_number"),
            col("vehicle_type"),
            col("vehicle_classification"),
            col("vehicle_length").cast(FloatType()),
            col("vehicle_width").cast(FloatType()),
            col("vehicle_height").cast(FloatType())
        )
        
    dim_vehicle = df_vehicle.withColumn(
        "vehicle_id", monotonically_increasing_id()
    ).dropDuplicates(["license_number"])
    
    write_to_gold(dim_vehicle, "dim_vehicle", "overwrite")
    write_to_postgres(dim_vehicle, "dim_vehicle", "overwrite")
    print(f"Dim_Vehicle completed: {dim_vehicle.count()} records")

