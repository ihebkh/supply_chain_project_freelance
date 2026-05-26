from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from config import (
    SILVER_CUSTOMERS,
    GOLD_DIM_CUSTOMERS,
    POSTGRES_ENABLED,
    write_dataframe_to_postgres,
)

def load_dim_customers(spark: SparkSession):
    print("📊 Création de la dimension Clients (Gold)...")
    
    # Lire depuis Silver
    df = spark.read.parquet(SILVER_CUSTOMERS)
    
    print("Données source:")
    df.show()
    
    # Ajouter des clés surrogates et métadonnées
    df_dim = df \
        .withColumn("customer_key", row_number().over(Window.orderBy("customer_id"))) \
        .withColumn("is_active", col("segment").isNotNull()) \
        .select(
            col("customer_key"),
            col("customer_id"),
            col("customer_name"),
            col("region"),
            col("segment"),
            col("is_active")
        )
    
    print("Schéma Dimension Customers:")
    df_dim.printSchema()
    
    print("Données après transformation:")
    df_dim.show()
    
    # Écrire en Gold
    df_dim.write.mode("overwrite").parquet(GOLD_DIM_CUSTOMERS)
    print("✅ Dimension Customers saved successfully")

    if POSTGRES_ENABLED:
        write_dataframe_to_postgres(df_dim, "dim_customers", ["customer_id"])
        print("✅ Dimension Customers upserted into Postgres")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Dim Customers Gold").getOrCreate()
    load_dim_customers(spark)
    spark.stop()
