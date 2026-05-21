from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date, row_number, cast, upper
from pyspark.sql.window import Window

def load_inventory_movements_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des mouvements de stock (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/inventory_movements")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("movement_date", to_date(col("movement_date"))) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("movement_type", trim(upper(col("movement_type")))) \
        .withColumn("quantity", when(col("quantity") == 0, None).otherwise(col("quantity"))) \
        .filter(col("movement_id").isNotNull()) \
        .filter(col("product_id").isNotNull()) \
        .filter(col("warehouse_id").isNotNull()) \
        .filter(col("movement_date").isNotNull()) \
        .filter(col("quantity").isNotNull()) \
        .filter(col("movement_type").isNotNull())
    
    # Déduplication sur movement_id
    window_spec = Window.partitionBy("movement_id").orderBy("movement_date")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/inventory_movements")
    print("✅ Inventory Movements saved successfully in data/silver/inventory_movements")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Inventory Movements Silver").getOrCreate()
    load_inventory_movements_bronze_to_silver(spark)
    spark.stop()
