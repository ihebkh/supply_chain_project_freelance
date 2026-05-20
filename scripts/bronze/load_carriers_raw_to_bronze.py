from pyspark.sql import SparkSession

def load_carriers_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des transporteurs (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/carriers.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/carriers")
    print("✅ Carriers saved successfully in data/bronze/carriers")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Carriers Bronze").getOrCreate()
    load_carriers_raw_to_bronze(spark)
    spark.stop()
