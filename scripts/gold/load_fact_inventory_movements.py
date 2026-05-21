from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import SILVER_INVENTORY, SILVER_PRODUCTS, SILVER_WAREHOUSES, GOLD_FACT_INVENTORY

def load_fact_inventory_movements(spark: SparkSession):
    print("⭐ Création de la table de Faits Mouvements de Stock (Gold)...")
    
    # Lire depuis Silver
    inventory_df = spark.read.parquet(SILVER_INVENTORY)
    products_df = spark.read.parquet(SILVER_PRODUCTS)
    warehouses_df = spark.read.parquet(SILVER_WAREHOUSES)
    
    print("Données source (Inventory Movements):")
    inventory_df.show(3)
    
    # Joindre avec les dimensions (sélectionner les colonnes spécifiques pour éviter les ambiguités)
    fact_inventory = inventory_df \
        .join(products_df.select(
            col("product_id"), 
            col("product_name"), 
            col("category")), 
              on="product_id", how="left") \
        .join(warehouses_df.select(
            col("warehouse_id"), 
            col("warehouse_name"), 
            col("region")), 
              on="warehouse_id", how="left") \
        .select(
            col("movement_id"),
            col("movement_date"),
            col("product_id"),
            col("product_name"),
            col("category").alias("product_category"),
            col("warehouse_id"),
            col("warehouse_name"),
            col("region").alias("warehouse_region"),
            col("movement_type"),
            col("quantity").alias("movement_quantity")
        )
    
    print("Schéma Fact Inventory Movements:")
    fact_inventory.printSchema()
    
    print("Données après transformation:")
    fact_inventory.show(5)
    
    print(f"Total records: {fact_inventory.count()}")
    
    # Écrire en Gold
    fact_inventory.write.mode("overwrite").parquet(GOLD_FACT_INVENTORY)
    print("✅ Fact Inventory Movements saved successfully")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Fact Inventory Gold").getOrCreate()
    load_fact_inventory_movements(spark)
    spark.stop()
