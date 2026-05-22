from pyspark.sql import SparkSession
from config import RAW_CARRIERS, BRONZE_CARRIERS

def load_carriers_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des transporteurs (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_CARRIERS)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_CARRIERS)
    print(f"✅ Carriers saved successfully in {BRONZE_CARRIERS}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Carriers Bronze").getOrCreate()
    load_carriers_raw_to_bronze(spark)
    spark.stop()
