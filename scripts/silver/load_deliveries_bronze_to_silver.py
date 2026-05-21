from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date, row_number, cast, upper
from pyspark.sql.window import Window

def load_deliveries_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des livraisons (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/deliveries")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("order_id", col("order_id")) \
        .withColumn("order_type", trim(upper(col("order_type")))) \
        .withColumn("expected_date", to_date(col("expected_date"))) \
        .withColumn("actual_date", when(col("actual_date").isNotNull(), to_date(col("actual_date"))).otherwise(None)) \
        .withColumn("status", trim(upper(col("status")))) \
        .filter(col("delivery_id").isNotNull()) \
        .filter(col("order_id").isNotNull()) \
        .filter(col("carrier_id").isNotNull()) \
        .filter(col("expected_date").isNotNull())
    
    # Déduplication sur delivery_id
    window_spec = Window.partitionBy("delivery_id").orderBy("expected_date")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/deliveries")
    print("✅ Deliveries saved successfully in data/silver/deliveries")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Deliveries Silver").getOrCreate()
    load_deliveries_bronze_to_silver(spark)
    spark.stop()
