from pyspark.sql.functions import col, year, month, dayofmonth, hour, dayofweek, to_timestamp, date_format
from pyspark.sql.types import IntegerType
from common import write_to_postgres, write_to_gold

def process_dim_time(df_silver):

    print("Processing Dim_Time...")
    dim_time = df_silver.select("timestamp").distinct() \
        .withColumn(
            "time_sk",
            date_format(col("timestamp"), "yyyyMMddHH").cast(IntegerType())
        ) \
        .withColumn("date", to_timestamp(col("timestamp"))) \
        .select(
            col("time_sk"),
            year("date").cast(IntegerType()).alias("year"),
            month("date").cast(IntegerType()).alias("month"),
            dayofmonth("date").cast(IntegerType()).alias("day"),
            hour("date").cast(IntegerType()).alias("hour"),
            dayofweek("date").cast(IntegerType()).alias("day_of_week")
        )
    
    write_to_gold(dim_time, "dim_time", "overwrite")
    write_to_postgres(dim_time, "dim_time", "overwrite")
    print(f"Dim_Time completed: {dim_time.count()} records")

