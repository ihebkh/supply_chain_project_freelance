from pyspark.sql import SparkSession

def load_purchase_orders_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des commandes d'achat (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/purchase_orders.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/purchase_orders")
    print("✅ Purchase Orders saved successfully in data/bronze/purchase_orders")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Purchase Orders Bronze").getOrCreate()
    load_purchase_orders_raw_to_bronze(spark)
    spark.stop()
