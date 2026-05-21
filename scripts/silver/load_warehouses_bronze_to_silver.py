from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper, row_number
from pyspark.sql.window import Window

def load_warehouses_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des entrepôts (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/warehouses")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("warehouse_name", trim(col("warehouse_name"))) \
        .withColumn("region", trim(col("region"))) \
        .withColumn("capacity", col("capacity").cast("int")) \
        .filter(col("warehouse_id").isNotNull()) \
        .filter(col("warehouse_name").isNotNull()) \
        .filter(col("region").isNotNull())
    
    # Déduplication sur warehouse_id
    window_spec = Window.partitionBy("warehouse_id").orderBy("warehouse_id")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/warehouses")
    print("✅ Warehouses saved successfully in data/silver/warehouses")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Warehouses Silver").getOrCreate()
    load_warehouses_bronze_to_silver(spark)
    spark.stop()
