from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import (
    SILVER_PURCHASES,
    SILVER_PRODUCTS,
    SILVER_SUPPLIERS,
    GOLD_FACT_PURCHASES,
    POSTGRES_ENABLED,
    write_dataframe_to_postgres,
)

def load_fact_purchases(spark: SparkSession):
    print("⭐ Création de la table de Faits Achats (Gold)...")
    
    # Lire depuis Silver
    purchases_df = spark.read.parquet(SILVER_PURCHASES)
    products_df = spark.read.parquet(SILVER_PRODUCTS)
    suppliers_df = spark.read.parquet(SILVER_SUPPLIERS)
    
    print("Données source (Purchases):")
    purchases_df.show(3)
    
    # Joindre avec les dimensions (renommer les colonnes pour éviter les ambiguités)
    fact_purchases = purchases_df \
        .join(products_df.select(
            col("product_id"), 
            col("product_name"), 
            col("category")), 
              on="product_id", how="left") \
        .join(suppliers_df.select(
            col("supplier_id"), 
            col("supplier_name"), 
            col("country")), 
              on="supplier_id", how="left") \
        .withColumn("total_cost", col("quantity") * col("purchase_price")) \
        .select(
            col("purchase_id"),
            col("order_date").alias("purchase_date"),
            col("supplier_id"),
            col("supplier_name"),
            col("country").alias("supplier_country"),
            col("product_id"),
            col("product_name"),
            col("category").alias("product_category"),
            col("quantity").alias("quantity_purchased"),
            col("purchase_price").alias("unit_cost"),
            col("total_cost").alias("purchase_amount")
        )
    
    print("Schéma Fact Purchases:")
    fact_purchases.printSchema()
    
    print("Données après transformation:")
    fact_purchases.show(5)
    
    print(f"Total records: {fact_purchases.count()}")
    
    # Écrire en Gold
    fact_purchases.write.mode("overwrite").parquet(GOLD_FACT_PURCHASES)
    print("✅ Fact Purchases saved successfully")

    if POSTGRES_ENABLED:
        write_dataframe_to_postgres(fact_purchases, "fact_purchases", ["purchase_id"])
        print("✅ Fact Purchases upserted into Postgres")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Fact Purchases Gold").getOrCreate()
    load_fact_purchases(spark)
    spark.stop()
