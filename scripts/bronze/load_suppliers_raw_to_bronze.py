from pyspark.sql import SparkSession

def load_suppliers_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des fournisseurs (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/suppliers.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/suppliers")
    print("✅ Suppliers saved successfully in data/bronze/suppliers")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Suppliers Bronze").getOrCreate()
    load_suppliers_raw_to_bronze(spark)
    spark.stop()
