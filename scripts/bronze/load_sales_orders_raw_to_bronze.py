from pyspark.sql import SparkSession

def load_sales_orders_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des commandes de vente (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/sales_orders.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/sales_orders")
    print("✅ Sales Orders saved successfully in data/bronze/sales_orders")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Sales Orders Bronze").getOrCreate()
    load_sales_orders_raw_to_bronze(spark)
    spark.stop()
