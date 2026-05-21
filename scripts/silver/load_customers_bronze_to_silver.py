from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper, row_number
from pyspark.sql.window import Window

def load_customers_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des clients (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/customers")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .withColumn("region", trim(upper(col("region")))) \
        .withColumn("segment", trim(upper(col("segment")))) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("customer_name").isNotNull()) \
        .filter(col("region").isNotNull())
    
    # Déduplication sur customer_id
    window_spec = Window.partitionBy("customer_id").orderBy("customer_id")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/customers")
    print("✅ Customers saved successfully in data/silver/customers")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Customers Silver").getOrCreate()
    load_customers_bronze_to_silver(spark)
    spark.stop()
