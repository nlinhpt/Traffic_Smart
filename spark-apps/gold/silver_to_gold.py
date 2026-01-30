import sys
import os
import time
from datetime import datetime
# Add local directory to path to allow importing sibling modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from common import create_spark_session, SILVER_PATH
from dim_time import process_dim_time
from dim_vehicle import process_dim_vehicle
from dim_location import process_dim_location
from dim_weather import process_dim_weather
from fact_traffic import process_fact_traffic
from dim_owner import process_dim_owner


def main():
    print("="*60)
    print("Starting Optimized Gold Layer ETL")
    print(f"Timestamp: {datetime.now()}")
    print("="*60)
    
    start_time = time.time()
    spark = create_spark_session("Gold-Layer")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("\n[1/6] Reading Silver layer data...")
        read_start = time.time()
        df_silver = spark.read.parquet(SILVER_PATH)
        df_silver.persist()
        
        # Trigger caching with a count
        record_count = df_silver.count()
        read_time = time.time() - read_start
        print(f"✓ Silver data loaded and cached: {record_count:,} records ({read_time:.2f}s)")
        
        # Process all dimension tables (these can be parallelized further if needed)
        print("\n[2/6] Processing Dimension Tables...")
        dim_start = time.time()
        
        # Dimension tables
        process_dim_time(df_silver)
        dim_owner = process_dim_owner(df_silver)
        dim_vehicle = process_dim_vehicle(df_silver)
        dim_location = process_dim_location(df_silver)
        dim_weather = process_dim_weather(df_silver)
        
        dim_time = time.time() - dim_start
        print(f"✓ All dimensions completed ({dim_time:.2f}s)")
        
        # Fact table
        print("\n[3/6] Processing Fact Table...")
        fact_start = time.time()
        process_fact_traffic(df_silver, dim_location, dim_weather, dim_owner, dim_vehicle)
        fact_time = time.time() - fact_start
        print(f"✓ Fact table completed ({fact_time:.2f}s)")
        
        print("\n[4/6] Releasing cached data...")
        df_silver.unpersist()
        
    except Exception as e:
        print(f"\nx Error during Gold layer processing: {e}")
        raise e
    finally:
        print("\n[5/6] Stopping Spark session...")
        spark.stop()
    
    total_time = time.time() - start_time
    print("\n" + "="*60)
    print("Gold Layer ETL Completed Successfully")
    print(f"Total execution time: {total_time:.2f}s ({total_time/60:.2f} min)")
    print(f"  - Silver read & cache: {read_time:.2f}s")
    print(f"  - Dimension tables: {dim_time:.2f}s")
    print(f"  - Fact table: {fact_time:.2f}s")
    print("="*60)

if __name__ == "__main__":
    main()
