from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from config import SILVER_PRODUCTS, GOLD_DIM_PRODUCTS

def load_dim_products(spark: SparkSession):
    print("📊 Création de la dimension Produits (Gold)...")
    
    # Lire depuis Silver
    df = spark.read.parquet(SILVER_PRODUCTS)
    
    print("Données source:")
    df.show()
    
    # Ajouter des clés surrogates
    df_dim = df \
        .withColumn("product_key", row_number().over(Window.orderBy("product_id"))) \
        .withColumn("is_active", (col("unit_price") > 0).cast("boolean")) \
        .select(
            col("product_key"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("brand"),
            col("unit_price"),
            col("stock_minimum"),
            col("is_active")
        )
    
    print("Schéma Dimension Products:")
    df_dim.printSchema()
    
    print("Données après transformation:")
    df_dim.show()
    
    # Écrire en Gold
    df_dim.write.mode("overwrite").parquet(GOLD_DIM_PRODUCTS)
    print("✅ Dimension Products saved successfully")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Dim Products Gold").getOrCreate()
    load_dim_products(spark)
    spark.stop()
