from pyspark.sql import SparkSession

def load_warehouses_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des entrepôts (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/warehouses.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/warehouses")
    print("✅ Warehouses saved successfully in data/bronze/warehouses")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Warehouses Bronze").getOrCreate()
    load_warehouses_raw_to_bronze(spark)
    spark.stop()
