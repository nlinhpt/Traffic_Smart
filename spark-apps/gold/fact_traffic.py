from pyspark.sql.functions import col, when, avg, count
from pyspark.sql.types import FloatType, IntegerType
from common import write_to_postgres, write_to_gold

def process_fact_traffic(df_silver, dim_location,dim_owner, dim_weather):

    print("Processing Fact_Traffic with Joins...")
    
    # Fix here
    # Join with dim_owner to get owner_id
    df_joined = df_silver.join(
        dim_owner,
        (df_silver.owner_name == dim_owner.owner_name) &
        (df_silver.phone == dim_owner.phone) &
        (df_silver.email == dim_owner.email),
        "left"
    ).drop(dim_owner.owner_name).drop(dim_owner.phone).drop(dim_owner.email)


    # Join with dim_location to get 
    
    # cur_location_id
    cur_loc = dim_location.select(
    col("location_id").alias("cur_location_id"),
    col("street").alias("cur_street"),
    col("district").alias("cur_district"),
    col("city").alias("cur_city")
    )   

    df_joined  = df_joined .join(
        cur_loc,
        ( df_joined.road_street == cur_loc.cur_street) &
        (df_joined.road_district == cur_loc.cur_district) &
        (df_joined.road_city == cur_loc.cur_city),
        "left"
    ).drop(cur_loc.cur_street).drop(cur_loc.cur_district).drop(cur_loc.cur_city)
    
    # dest_location
    dest_loc = dim_location.select(
        col("location_id").alias("dest_location_id"),
        col("street").alias("dest_street"),
        col("district").alias("dest_district"),
        col("city").alias("dest_city")
    )
    df_joined = df_joined.join(
        dest_loc,
        (df_joined.destination_street == dest_loc.dest_street) &
        (df_joined.destination_district == dest_loc.dest_district) &
        (df_joined.destination_city == dest_loc.dest_city),
        "left"
    ).drop(dest_loc.dest_street).drop(dest_loc.dest_district).drop(dest_loc.dest_city)
    
   
    
    
    # Join with dim_weather to get weather_id
    df_joined = df_joined.join(
        dim_weather,
        (df_joined.weather_condition == dim_weather.weather_condition) & 
        (df_joined.temperature.cast(FloatType()) == dim_weather.temperature) & 
        (df_joined.humidity.cast(FloatType()) == dim_weather.humidity),
        "left"
    ).drop(dim_weather.weather_condition).drop(dim_weather.temperature).drop(dim_weather.humidity)

    # Select and cast measures
    df_fact = df_joined.select(
        col("timestamp").alias("time_id"),
        col("vehicle_id"), 
        col("owner_id"), # add owner_id
        col("cur_location_id"),
        col("dest_location_id"),
        col("weather_id"),
        col("speed_kmph").cast(FloatType()),
        col("rpm").cast(IntegerType()),
        col("fuel_level_percentage").cast(FloatType()),
        col("passenger_count").cast(IntegerType()),
        when(col("congestion_level") == "Low", 1)
            .when(col("congestion_level") == "Moderate", 2)
            .when(col("congestion_level") == "High", 3)
            .when(col("congestion_level") == "Heavy", 4)
            .otherwise(0).cast(IntegerType()).alias("congestion_score"
        ),
        col("estimated_delay_minutes").cast(IntegerType()),
        col("eta")
    )

    write_to_gold(df_fact, "fact_traffic", "append")
    write_to_postgres(df_fact, "fact_traffic", "append")
    
    # Aggregation: Hourly Average Speed and Traffic Count per Road
    print("Processing Fact_Traffic Aggregation (Hourly Metrics)...")
    df_agg = df_fact.groupBy("cur_location_id", "time_id") \
        .agg(
            avg("speed_kmph").alias("avg_speed"),
            avg("congestion_score").alias("avg_congestion"),
            count("vehicle_id").alias("traffic_count")
        )
    write_to_gold(df_agg, "fact_traffic_hourly_agg", "overwrite")
    write_to_postgres(df_agg, "fact_traffic_hourly_agg", "overwrite")
    print(f"Fact_Traffic completed: {df_fact.count()} records, Aggregated: {df_agg.count()} records")

