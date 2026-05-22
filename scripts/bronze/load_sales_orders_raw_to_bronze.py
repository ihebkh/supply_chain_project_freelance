from pyspark.sql import SparkSession
from config import RAW_SALES, BRONZE_SALES

def load_sales_orders_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des commandes de vente (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_SALES)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_SALES)
    print(f"✅ Sales Orders saved successfully in {BRONZE_SALES}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Sales Orders Bronze").getOrCreate()
    load_sales_orders_raw_to_bronze(spark)
    spark.stop()
