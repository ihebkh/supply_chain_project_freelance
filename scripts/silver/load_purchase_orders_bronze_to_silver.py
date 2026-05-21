from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date, row_number, cast, upper
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

def load_purchase_orders_bronze_to_silver(spark: SparkSession):
    print("📥 Transformation des commandes d'achat (Bronze -> Silver)...")
    
    # Lire depuis Bronze
    df = spark.read.parquet("data/bronze/purchase_orders")
    
    print("Données avant transformation:")
    df.show()
    
    # Transformations
    df_silver = df \
        .withColumn("order_date", to_date(col("order_date"))) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("purchase_price", col("purchase_price").cast(DecimalType(10, 2))) \
        .withColumn("quantity", when(col("quantity") <= 0, None).otherwise(col("quantity"))) \
        .withColumn("purchase_price", when(col("purchase_price") < 0, None).otherwise(col("purchase_price"))) \
        .filter(col("purchase_id").isNotNull()) \
        .filter(col("supplier_id").isNotNull()) \
        .filter(col("product_id").isNotNull()) \
        .filter(col("order_date").isNotNull()) \
        .filter(col("quantity").isNotNull())
    
    # Déduplication sur purchase_id
    window_spec = Window.partitionBy("purchase_id").orderBy("order_date")
    df_silver = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print("Schéma Silver:")
    df_silver.printSchema()
    
    print("Données après transformation:")
    df_silver.show()
    
    # Écrire en Silver
    df_silver.write.mode("overwrite").parquet("data/silver/purchase_orders")
    print("✅ Purchase Orders saved successfully in data/silver/purchase_orders")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load Purchase Orders Silver").getOrCreate()
    load_purchase_orders_bronze_to_silver(spark)
    spark.stop()
