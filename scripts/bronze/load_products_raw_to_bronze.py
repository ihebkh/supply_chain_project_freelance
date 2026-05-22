from pyspark.sql import SparkSession
from config import RAW_PRODUCTS, BRONZE_PRODUCTS

def load_products_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des produits (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_PRODUCTS)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_PRODUCTS)
    print(f"✅ Products saved successfully in {BRONZE_PRODUCTS}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Products Bronze").getOrCreate()
    load_products_raw_to_bronze(spark)
    spark.stop()
