from pyspark.sql import SparkSession
from config import RAW_PURCHASES, BRONZE_PURCHASES

def load_purchase_orders_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des commandes d'achat (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_PURCHASES)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_PURCHASES)
    print(f"✅ Purchase Orders saved successfully in {BRONZE_PURCHASES}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Purchase Orders Bronze").getOrCreate()
    load_purchase_orders_raw_to_bronze(spark)
    spark.stop()
