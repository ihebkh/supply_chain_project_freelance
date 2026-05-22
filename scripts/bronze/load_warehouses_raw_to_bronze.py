from pyspark.sql import SparkSession
from config import RAW_WAREHOUSES, BRONZE_WAREHOUSES

def load_warehouses_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des entrepôts (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_WAREHOUSES)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_WAREHOUSES)
    print(f"✅ Warehouses saved successfully in {BRONZE_WAREHOUSES}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Warehouses Bronze").getOrCreate()
    load_warehouses_raw_to_bronze(spark)
    spark.stop()
