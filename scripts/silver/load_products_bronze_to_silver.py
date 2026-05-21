from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper, row_number, cast
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

def load_products_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des produits (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/products")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("product_name", trim(col("product_name"))) \
        .withColumn("category", trim(upper(col("category")))) \
        .withColumn("brand", trim(col("brand"))) \
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2))) \
        .withColumn("stock_minimum", col("stock_minimum").cast("int")) \
        .withColumn("unit_price", when(col("unit_price") < 0, None).otherwise(col("unit_price"))) \
        .withColumn("stock_minimum", when(col("stock_minimum") < 0, 0).otherwise(col("stock_minimum"))) \
        .filter(col("product_id").isNotNull()) \
        .filter(col("product_name").isNotNull()) \
        .filter(col("category").isNotNull())
    
    # Déduplication sur product_id
    window_spec = Window.partitionBy("product_id").orderBy("product_id")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/products")
    print("✅ Products saved successfully in data/silver/products")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Products Silver").getOrCreate()
    load_products_bronze_to_silver(spark)
    spark.stop()
