from pyspark.sql.functions import monotonically_increasing_id, xxhash64
from common import write_to_postgres, write_to_gold

def process_dim_owner(df_silver):
    print("Processing Dim_Owner...")
    df_owner = (df_silver.select(
        "owner_name", 
        "phone", 
        "email"
        )
        .dropDuplicates(["owner_name", "phone", "email"])
    )

    dim_owner = df_owner.withColumn(
        "owner_sk",
        monotonically_increasing_id()
    )

    write_to_gold(dim_owner, "dim_owner", "overwrite")
    write_to_postgres(dim_owner, "dim_owner", "overwrite")
    print(f"Dim_Owner completed: {dim_owner.count()} records")
    
    return dim_owner
