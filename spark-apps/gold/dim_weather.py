from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import FloatType
from common import write_to_postgres, write_to_gold

def process_dim_weather(df_silver):
    print("Processing Dim_Weather...")
    dim_weather = df_silver.select(
        col("weather_condition"),
        col("temperature").cast(FloatType()),
        col("humidity").cast(FloatType())
    ).distinct().withColumn("weather_sk", monotonically_increasing_id())
    
    write_to_gold(dim_weather, "dim_weather", "overwrite")
    write_to_postgres(dim_weather, "dim_weather", "overwrite")
    print(f"Dim_Weather completed: {dim_weather.count()} records")
    
    return dim_weather
