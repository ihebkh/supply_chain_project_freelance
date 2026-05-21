from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper, row_number
from pyspark.sql.window import Window

def load_suppliers_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des fournisseurs (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/suppliers")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("supplier_name", trim(col("supplier_name"))) \
        .withColumn("country", trim(upper(col("country")))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("supplier_type", trim(upper(col("supplier_type")))) \
        .filter(col("supplier_id").isNotNull()) \
        .filter(col("supplier_name").isNotNull()) \
        .filter(col("country").isNotNull())
    
    # Déduplication sur supplier_id
    window_spec = Window.partitionBy("supplier_id").orderBy("supplier_id")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/suppliers")
    print("✅ Suppliers saved successfully in data/silver/suppliers")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Suppliers Silver").getOrCreate()
    load_suppliers_bronze_to_silver(spark)
    spark.stop()
