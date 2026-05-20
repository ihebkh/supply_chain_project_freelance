from pyspark.sql import SparkSession

def load_products_raw_to_bronze(spark: SparkSession):
    print("📥 Ingestion des produits (Raw -> Bronze)...")
    
    # Lire le fichier CSV depuis raw
    df_products = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/raw/products.csv")

    # Vérifier les données
    df_products.show()
    df_products.printSchema()

    # Écrire dans Bronze au format Parquet
    df_products.write \
        .mode("overwrite") \
        .parquet("data/bronze/products")
    
    print("✅ Products saved successfully in data/bronze/products")

if __name__ == "__main__":
    # Permet d'exécuter ce script de façon autonome
    spark = SparkSession.builder \
        .appName("Load Products Bronze") \
        .getOrCreate()
    load_products_raw_to_bronze(spark)
    spark.stop()
