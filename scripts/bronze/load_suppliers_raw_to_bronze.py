from pyspark.sql import SparkSession
from config import RAW_SUPPLIERS, BRONZE_SUPPLIERS

def load_suppliers_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des fournisseurs (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_SUPPLIERS)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_SUPPLIERS)
    print(f"✅ Suppliers saved successfully in {BRONZE_SUPPLIERS}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Suppliers Bronze").getOrCreate()
    load_suppliers_raw_to_bronze(spark)
    spark.stop()
