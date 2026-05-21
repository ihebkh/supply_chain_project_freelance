from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date, row_number, cast, upper
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

def load_sales_orders_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des commandes de vente (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/sales_orders")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("sale_date", to_date(col("sale_date"))) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2))) \
        .withColumn("quantity", when(col("quantity") <= 0, None).otherwise(col("quantity"))) \
        .withColumn("unit_price", when(col("unit_price") < 0, None).otherwise(col("unit_price"))) \
        .filter(col("sale_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("product_id").isNotNull()) \
        .filter(col("sale_date").isNotNull()) \
        .filter(col("quantity").isNotNull())
    
    # Déduplication sur sale_id
    window_spec = Window.partitionBy("sale_id").orderBy("sale_date")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/sales_orders")
    print("✅ Sales Orders saved successfully in data/silver/sales_orders")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Sales Orders Silver").getOrCreate()
    load_sales_orders_bronze_to_silver(spark)
    spark.stop()
