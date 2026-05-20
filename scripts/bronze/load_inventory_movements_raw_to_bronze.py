from pyspark.sql import SparkSession

def load_inventory_movements_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des mouvements de stock (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/inventory_movements.csv")
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("data/bronze/inventory_movements")
    print("✅ Inventory Movements saved successfully in data/bronze/inventory_movements")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Inventory Movements Bronze").getOrCreate()
    load_inventory_movements_raw_to_bronze(spark)
    spark.stop()
