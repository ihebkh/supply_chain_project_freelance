from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, when
from config import (
    SILVER_DELIVERIES,
    SILVER_CARRIERS,
    GOLD_FACT_DELIVERIES,
    POSTGRES_ENABLED,
    write_dataframe_to_postgres,
)

def load_fact_deliveries(spark: SparkSession):
    print("⭐ Création de la table de Faits Livraisons (Gold)...")
    
    # Lire depuis Silver
    deliveries_df = spark.read.parquet(SILVER_DELIVERIES)
    carriers_df = spark.read.parquet(SILVER_CARRIERS)
    
    print("Données source (Deliveries):")
    deliveries_df.show(3)
    
    # Joindre avec les dimensions
    fact_deliveries = deliveries_df \
        .join(carriers_df.select("carrier_id", "carrier_name", "carrier_type"), 
              on="carrier_id", how="left") \
        .withColumn("days_to_deliver", 
                   datediff(col("actual_date"), col("expected_date"))) \
        .withColumn("is_on_time", 
                   when(col("actual_date") <= col("expected_date"), True).otherwise(False)) \
        .select(
            col("delivery_id"),
            col("order_id"),
            col("order_type"),
            col("expected_date"),
            col("actual_date"),
            col("carrier_id"),
            col("carrier_name"),
            col("carrier_type"),
            col("status"),
            col("days_to_deliver"),
            col("is_on_time")
        )
    
    print("Schéma Fact Deliveries:")
    fact_deliveries.printSchema()
    
    print("Données après transformation:")
    fact_deliveries.show(5)
    
    print(f"Total records: {fact_deliveries.count()}")
    
    # Écrire en Gold
    fact_deliveries.write.mode("overwrite").parquet(GOLD_FACT_DELIVERIES)
    print("✅ Fact Deliveries saved successfully")

    if POSTGRES_ENABLED:
        write_dataframe_to_postgres(fact_deliveries, "fact_deliveries", ["delivery_id"])
        print("✅ Fact Deliveries upserted into Postgres")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Fact Deliveries Gold").getOrCreate()
    load_fact_deliveries(spark)
    spark.stop()
