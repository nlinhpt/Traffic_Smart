from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, from_json, when, date_format,
    input_file_name, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    LongType, BooleanType, ArrayType, DecimalType
)
from pyspark.sql.utils import AnalysisException

# MinIO Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# Paths
BRONZE_PATH =  "s3a://bronze/raw/2026-01-15/traffic_data_1.json" #Reading all raw data  "s3a://bronze/raw_sample/2026-01-15/traffic_data_1_sample.json"   # Reading all raw data "s3a://bronze/raw/*/*.json" #
SILVER_PATH = "s3a://silver/traffic_data"
METADATA_PATH = "s3a://silver/_metadata/processed_files"
def create_spark_session():
    """Create optimized SparkSession for Bronze to Silver processing"""
    return SparkSession.builder \
        .appName("Traffic-Bronze-To-Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.read.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.commit.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
        .config("spark.hadoop.fs.s3a.threads.max", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.network.timeout", "300s") \
        .getOrCreate()

def get_schema():
    # Define schema based on README description to ensure correct types
    return StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("owner", StructType([
            StructField("name", StringType(), True),
            StructField("license_number", StringType(), True),
            StructField("contact_info", StructType([
                StructField("phone", StringType(), True),
                StructField("email", StringType(), True)
            ]), True)
        ]), True),
        StructField("speed_kmph", DoubleType(), True),
        StructField("road", StructType([
            StructField("street", StringType(), True),
            StructField("district", StringType(), True),
            StructField("city", StringType(), True)
        ]), True),
        StructField("timestamp", TimestampType(), True),
        StructField("vehicle_size", StructType([
            StructField("length_meters", DoubleType(), True),
            StructField("width_meters", DoubleType(), True),
            StructField("height_meters", DoubleType(), True)
        ]), True),
        StructField("vehicle_type", StringType(), True),
        StructField("vehicle_classification", StringType(), True),
        StructField("coordinates", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True) # vehicle's current location
        ]), True),
        StructField("engine_status", StructType([
            StructField("is_running", BooleanType(), True),
            StructField("rpm", LongType(), True),
            StructField("oil_pressure", StringType(), True)
        ]), True),
        StructField("fuel_level_percentage", LongType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("internal_temperature_celsius", DoubleType(), True),
        StructField("weather_condition", StructType([
            StructField("temperature_celsius", DoubleType(), True),
            StructField("humidity_percentage", DoubleType(), True),
            StructField("condition", StringType(), True)
        ]), True),
        StructField("estimated_time_of_arrival", StructType([
            StructField("destination", StructType([
                StructField("street", StringType(), True),
                StructField("district", StringType(), True),
                StructField("city", StringType(), True)
            ]), True),
            StructField("eta", TimestampType(), True),
        ]), True),
        StructField("traffic_status", StructType([
            StructField("congestion_level", StringType(), True),
            StructField("estimated_delay_minutes", LongType(), True)
        ]), True),
        StructField("alerts", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])), True)
    ])

def transform_data(df):
    return df.select(
        col("vehicle_id"),
        col("owner.name").alias("owner_name"),
        col("owner.license_number").alias("license_number"),
        col("owner.contact_info.phone").alias("phone"),
        col("owner.contact_info.email").alias("email"),
        col("vehicle_type"),
        col("vehicle_classification"),
        col("speed_kmph"),
        # Road Info
        col("road.street").alias("road_street"),
        col("road.district").alias("road_district"),
        col("road.city").alias("road_city"),
        
        # Time (Convert here)
        to_timestamp(col("timestamp")).alias("timestamp"),
        to_date(to_timestamp(col("timestamp"))).alias("date"),
        # Vehicle Size
        col("vehicle_size.length_meters").alias("vehicle_length"),
        col("vehicle_size.width_meters").alias("vehicle_width"),
        col("vehicle_size.height_meters").alias("vehicle_height"),
        # Coordinates
        col("coordinates.latitude"),
        col("coordinates.longitude"),
        # Engine
        col("engine_status.rpm").alias("rpm"),
        col("engine_status.oil_pressure").alias("oil_pressure"),
        col("engine_status.is_running").alias("is_running"),
        col("fuel_level_percentage"),
        # Weather
        col("weather_condition.condition").alias("weather_condition"),
        col("weather_condition.temperature_celsius").alias("temperature"),
        col("weather_condition.humidity_percentage").alias("humidity"),
        # Estimated time of arrival
        col("estimated_time_of_arrival.destination.street").alias("destination_street"),
        col("estimated_time_of_arrival.destination.district").alias("destination_district"),
        col("estimated_time_of_arrival.destination.city").alias("destination_city"),
        col("estimated_time_of_arrival.eta").alias("eta"),
        # Traffic
        col("traffic_status.congestion_level").alias("congestion_level"),
        col("traffic_status.estimated_delay_minutes").alias("estimated_delay_minutes"),
        # Misc
        col("passenger_count"),
        col("internal_temperature_celsius"),
        # Alerts
        col("alerts") 
    )

def get_validation_condition():
    return [
        col("vehicle_id").isNotNull(),
        col("timestamp").isNotNull(),
        col("road_street").isNotNull(),
        col("road_district").isNotNull(),

        (col("speed_kmph").isNull() | ((col("speed_kmph") >= 0) & (col("speed_kmph") < 300))),
        
        (col("vehicle_length").isNull() | (col("vehicle_length") >= 0)),
        (col("vehicle_width").isNull() | (col("vehicle_width") >= 0)),
        (col("vehicle_height").isNull() | (col("vehicle_height") >= 0)),

        (col("fuel_level_percentage").isNull() | (col("fuel_level_percentage").between(0, 100))),

        (col("passenger_count").isNull() | (col("passenger_count") >= 0)),
        (col("rpm").isNull() | (col("rpm") >= 0)),
        
        col('eta').isNotNull() &
        (col('eta') >= col('timestamp')),
        
        (col("latitude").isNull() | (col("latitude").between(-90, 90))) &
        (col("longitude").isNull() | (col("longitude").between(-180, 180))),
    ]

def clean_data(df):
    return df.dropDuplicates(["vehicle_id", "timestamp"])

def read_processed_files(spark):
    try:
        return spark.read.parquet(METADATA_PATH)
    except AnalysisException:
        schema = StructType([
            StructField("file_path", StringType(), True),
            StructField("processed_at", StringType(), True)
        ])
        return spark.createDataFrame([], schema)



# def main():
#     spark = create_spark_session()
#     spark.sparkContext.setLogLevel("WARN")

#     print("Reading data from Bronze layer...")
#     # schema = get_schema()
#     df = spark.read \
#             .option("multiLine", "true") \
#             .json("s3a://bronze/raw_sample/2026-01-15/traffic_data_1_sample.json")


#     # 1. Transformation (Flattening)
#     print("Transforming and flattening data...")

#     df_flat = transform_data(df)

#     # 2. Cleaning & Validation
#     print("Validating flattened data...")
#     validation_conditions = get_validation_condition()
    
#     # Initialize with the first condition
#     combined_condition = validation_conditions[0]
#     for condition in validation_conditions[1:]:
#         combined_condition = combined_condition & condition
        
#     df_valid = df_flat.filter(combined_condition)
#     df_invalid = df_flat.filter(~combined_condition)

#     # if df_invalid.count() > 0:
#     #     print(f"Found {df_invalid.count()} invalid records. Writing to bad_data...")
#     df_invalid.write.mode("append").json(f"{SILVER_PATH}_bad_data")
    
#     # 3. Clean duplicates
#     print("Removing duplicates...")
#     df_clean = clean_data(df_valid)

#     # 4. Write data with Optimized Partitioning
#     print("Writing data to Silver layer (Parquet)...")
    
#     # Optimization: Sort within partitions to cluster data by district physically
#     # This helps downstream jobs that filter by district even without physical district partitioning
#     df_clean \
#         .sortWithinPartitions("road_district", "timestamp") \
#         .write \
#         .mode("append") \
#         .partitionBy("date") \
#         .parquet(SILVER_PATH)
    
#     print(f"Silver layer processing completed. Data written to {SILVER_PATH}")
#     spark.stop()

def main():
    import os
    import time
    from datetime import datetime
    
    print("="*60)
    print("Starting Bronze to Silver ETL")
    print(f"Timestamp: {datetime.now()}")
    print("="*60)
    
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n[1/5] Reading data from Bronze layer...")
    read_start = time.time()
    schema = get_schema()
    
    df = spark.read \
        .option("multiline", "true") \
        .schema(schema) \
        .json(BRONZE_PATH)
    read_time = time.time() - read_start
    print(f"✓ Data read completed ({read_time:.2f}s)")
    # df = df.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    df.printSchema()

    # 2. Transformation (Flattening)
    print("\n[2/5] Transforming and flattening data...")
    transform_start = time.time()
    df_flat = transform_data(df).persist()
    transform_time = time.time() - transform_start
    print(f"✓ Transformation completed ({transform_time:.2f}s)")

    # 3. Validation
    print("\n[3/5] Validating data...")
    validate_start = time.time()
    validation_conditions = get_validation_condition()
    
    # Initialize with the first condition
    combined_condition = validation_conditions[0]
    for condition in validation_conditions[1:]:
        combined_condition = combined_condition & condition
    print(f"✓ Get validation conditions completed.")
    
    df_valid = df_flat.filter(combined_condition)
    df_invalid = df_flat.filter(~combined_condition)
    invalid_count = df_invalid.count()
    validate_time = time.time() - validate_start
    print(f"✓ Validation completed ({validate_time:.2f}s)")
    print(f"  Valid: {df_flat.count() - invalid_count:,} | Invalid: {invalid_count:,}")
    
    if invalid_count > 0:
        print(f"\n⚠️  Writing {invalid_count} invalid records to bad_data...")
        df_invalid.write.mode("append").json(f"{SILVER_PATH}_bad_data")
    
    # 4. Clean duplicates
    print("\n[4/5] Removing duplicates...")
    clean_start = time.time()
    df_clean = clean_data(df_valid)
    clean_time = time.time() - clean_start
    print(f"✓ Deduplication completed ({clean_time:.2f}s)")

    # 5. Write data with Optimized Partitioning
    print("\n[5/5] Writing data to Silver layer (Parquet)...")
    write_start = time.time()
    
    df_flat.unpersist()
    
    df_clean \
        .repartition("date") \
        .write \
        .mode("append") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)
    
    write_time = time.time() - write_start
    total_time = time.time() - start_time
    
    print(f"✓ Write completed ({write_time:.2f}s)")
    print("\n" + "="*60)
    print("Silver Layer Processing Completed Successfully")
    print(f"Total execution time: {total_time:.2f}s ({total_time/60:.2f} min)")
    print(f"  - Read: {read_time:.2f}s")
    print(f"  - Transform: {transform_time:.2f}s")
    print(f"  - Validate: {validate_time:.2f}s")
    print(f"  - Clean: {clean_time:.2f}s")
    print(f"  - Write: {write_time:.2f}s")
    print(f"Output path: {SILVER_PATH}")
    print("="*60)
    
    df_test = spark.read.parquet("s3a://silver/traffic_data/")
    df_test.select("date").distinct().show()
    
    spark.stop()


if __name__ == "__main__":
    main()
