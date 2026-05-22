from pyspark.sql import SparkSession
from config import RAW_DELIVERIES, BRONZE_DELIVERIES

def load_deliveries_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des livraisons (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_DELIVERIES)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_DELIVERIES)
    print(f"✅ Deliveries saved successfully in {BRONZE_DELIVERIES}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Deliveries Bronze").getOrCreate()
    load_deliveries_raw_to_bronze(spark)
    spark.stop()
