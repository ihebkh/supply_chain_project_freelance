from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from config import SILVER_CARRIERS, GOLD_DIM_CARRIERS

def load_dim_carriers(spark: SparkSession):
    print("📊 Création de la dimension Transporteurs (Gold)...")
    
    # Lire depuis Silver
    df = spark.read.parquet(SILVER_CARRIERS)
    
    print("Données source:")
    df.show()
    
    # Ajouter des clés surrogates
    df_dim = df \
        .withColumn("carrier_key", row_number().over(Window.orderBy("carrier_id"))) \
        .select(
            col("carrier_key"),
            col("carrier_id"),
            col("carrier_name"),
            col("carrier_type")
        )
    
    print("Schéma Dimension Carriers:")
    df_dim.printSchema()
    
    print("Données après transformation:")
    df_dim.show()
    
    # Écrire en Gold
    df_dim.write.mode("overwrite").parquet(GOLD_DIM_CARRIERS)
    print("✅ Dimension Carriers saved successfully")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Dim Carriers Gold").getOrCreate()
    load_dim_carriers(spark)
    spark.stop()
