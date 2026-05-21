from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from config import SILVER_WAREHOUSES, GOLD_DIM_WAREHOUSES

def load_dim_warehouses(spark: SparkSession):
    print("📊 Création de la dimension Entrepôts (Gold)...")
    
    # Lire depuis Silver
    df = spark.read.parquet(SILVER_WAREHOUSES)
    
    print("Données source:")
    df.show()
    
    # Ajouter des clés surrogates
    df_dim = df \
        .withColumn("warehouse_key", row_number().over(Window.orderBy("warehouse_id"))) \
        .select(
            col("warehouse_key"),
            col("warehouse_id"),
            col("warehouse_name"),
            col("region"),
            col("capacity")
        )
    
    print("Schéma Dimension Warehouses:")
    df_dim.printSchema()
    
    print("Données après transformation:")
    df_dim.show()
    
    # Écrire en Gold
    df_dim.write.mode("overwrite").parquet(GOLD_DIM_WAREHOUSES)
    print("✅ Dimension Warehouses saved successfully")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Dim Warehouses Gold").getOrCreate()
    load_dim_warehouses(spark)
    spark.stop()
