from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from config import SILVER_SUPPLIERS, GOLD_DIM_SUPPLIERS

def load_dim_suppliers(spark: SparkSession):
    print("📊 Création de la dimension Fournisseurs (Gold)...")
    
    # Lire depuis Silver
    df = spark.read.parquet(SILVER_SUPPLIERS)
    
    print("Données source:")
    df.show()
    
    # Ajouter des clés surrogates
    df_dim = df \
        .withColumn("supplier_key", row_number().over(Window.orderBy("supplier_id"))) \
        .select(
            col("supplier_key"),
            col("supplier_id"),
            col("supplier_name"),
            col("country"),
            col("city"),
            col("supplier_type")
        )
    
    print("Schéma Dimension Suppliers:")
    df_dim.printSchema()
    
    print("Données après transformation:")
    df_dim.show()
    
    # Écrire en Gold
    df_dim.write.mode("overwrite").parquet(GOLD_DIM_SUPPLIERS)
    print("✅ Dimension Suppliers saved successfully")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Dim Suppliers Gold").getOrCreate()
    load_dim_suppliers(spark)
    spark.stop()
