from pyspark.sql import SparkSession
from config import RAW_INVENTORY, BRONZE_INVENTORY

def load_inventory_movements_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des mouvements de stock (Raw -> Bronze)...")
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_INVENTORY)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet(BRONZE_INVENTORY)
    print(f"✅ Inventory Movements saved successfully in {BRONZE_INVENTORY}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Inventory Movements Bronze").getOrCreate()
    load_inventory_movements_raw_to_bronze(spark)
    spark.stop()
