from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, dayofweek, unix_timestamp, date_format
from pyspark.sql.types import IntegerType
from common import write_to_postgres, write_to_gold
from pyspark.sql.types import StructType, StructField, TimestampType
import datetime

def process_dim_time(spark):

    print("Processing Dim_Time...")
    # dim_time = df_silver.select("timestamp").distinct() \
    #     .withColumn(
    #         "time_sk",
    #         date_format(col("timestamp"), "yyyyMMddHH").cast(IntegerType())
    #     ) \
    #     .dropDuplicates(["time_sk"]) \
    #     .withColumn("date", to_timestamp(col("timestamp"))) \
    #     .select(
    #         col("time_sk"),
    #         year("date").cast(IntegerType()).alias("year"),
    #         month("date").cast(IntegerType()).alias("month"),
    #         dayofmonth("date").cast(IntegerType()).alias("day"),
    #         hour("date").cast(IntegerType()).alias("hour"),
    #         dayofweek("date").cast(IntegerType()).alias("day_of_week")
    #     )
    
    start_date = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end_date = datetime.datetime(2024, 6, 30, 23, 59, 59)
    time_data = []
    current_time = start_date
    while current_time <= end_date:
        time_data.append((current_time,))
        current_time += datetime.timedelta(minutes=1)

    schema = StructType([
        StructField("timestamp", TimestampType(), True)
    ])

    time_df = spark.createDataFrame(time_data, schema)

    dim_time = time_df \
        .withColumn("time_sk", unix_timestamp(col("timestamp")).cast(IntegerType())) \
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("month_name", date_format(col("timestamp"), "MMMM")) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("minute", minute(col("timestamp"))) \

    write_to_gold(dim_time, "dim_time", "overwrite")
    write_to_postgres(dim_time, "dim_time", "overwrite")
    print(f"Dim_Time completed: {dim_time.count()} records")
