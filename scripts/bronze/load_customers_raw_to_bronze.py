from pyspark.sql import SparkSession
from config import RAW_CUSTOMERS, BRONZE_CUSTOMERS

def load_customers_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des clients (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_CUSTOMERS)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_CUSTOMERS)
    print(f"✅ Customers saved successfully in {BRONZE_CUSTOMERS}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Customers Bronze").getOrCreate()
    load_customers_raw_to_bronze(spark)
    spark.stop()
