from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from config import SILVER_SALES, SILVER_PRODUCTS, SILVER_CUSTOMERS, GOLD_FACT_SALES

def load_fact_sales(spark: SparkSession):
    print("⭐ Création de la table de Faits Ventes (Gold)...")
    
    # Lire depuis Silver
    sales_df = spark.read.parquet(SILVER_SALES)
    products_df = spark.read.parquet(SILVER_PRODUCTS)
    customers_df = spark.read.parquet(SILVER_CUSTOMERS)
    
    print("Données source (Sales):")
    sales_df.show(3)
    
    # Joindre avec les dimensions (renommer les colonnes pour éviter les ambiguités)
    fact_sales = sales_df \
        .join(products_df.select(
            col("product_id"), 
            col("product_name"), 
            col("category"), 
            col("unit_price").alias("product_unit_price")), 
              on="product_id", how="left") \
        .join(customers_df.select(
            col("customer_id"), 
            col("customer_name"), 
            col("region")), 
              on="customer_id", how="left") \
        .withColumn("total_amount", col("quantity") * col("unit_price")) \
        .select(
            col("sale_id"),
            col("sale_date"),
            col("customer_id"),
            col("customer_name"),
            col("region").alias("customer_region"),
            col("product_id"),
            col("product_name"),
            col("category").alias("product_category"),
            col("quantity").alias("quantity_sold"),
            col("unit_price").alias("sale_unit_price"),
            col("total_amount").alias("sale_amount")
        )
    
    print("Schéma Fact Sales:")
    fact_sales.printSchema()
    
    print("Données après transformation:")
    fact_sales.show(5)
    
    print(f"Total records: {fact_sales.count()}")
    
    # Écrire en Gold
    fact_sales.write.mode("overwrite").parquet(GOLD_FACT_SALES)
    print("✅ Fact Sales saved successfully")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Fact Sales Gold").getOrCreate()
    load_fact_sales(spark)
    spark.stop()
