from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper, row_number
from pyspark.sql.window import Window

def load_carriers_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des transporteurs (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/carriers")
    
    # Afficher les données avant transformation
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("carrier_name", trim(col("carrier_name"))) \
        .withColumn("carrier_type", trim(upper(col("carrier_type")))) \
        .filter(col("carrier_id").isNotNull()) \
        .filter(col("carrier_name").isNotNull()) \
        .filter(col("carrier_type").isNotNull())
    
    # Déduplication sur carrier_id (garder le premier)
    window_spec = Window.partitionBy("carrier_id").orderBy("carrier_id")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/carriers")
    print("✅ Carriers saved successfully in data/silver/carriers")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Carriers Silver").getOrCreate()
    load_carriers_bronze_to_silver(spark)
    spark.stop()
