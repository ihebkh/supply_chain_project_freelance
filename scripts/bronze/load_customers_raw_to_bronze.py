from pyspark.sql import SparkSession

def load_customers_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des clients (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/customers.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/customers")
    print("✅ Customers saved successfully in data/bronze/customers")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Customers Bronze").getOrCreate()
    load_customers_raw_to_bronze(spark)
    spark.stop()
