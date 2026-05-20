from pyspark.sql import SparkSession

def load_deliveries_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des livraisons (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/deliveries.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/deliveries")
    print("✅ Deliveries saved successfully in data/bronze/deliveries")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Deliveries Bronze").getOrCreate()
    load_deliveries_raw_to_bronze(spark)
    spark.stop()
