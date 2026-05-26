import sys
import os

# Force UTF-8 encoding for console output (fixes emoji display on Windows)
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ==================== CHARGEMENT ENV ====================
from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession

# Ajouter le répertoire de base au sys.path pour les imports locaux
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Imports config centralisée
from config import (
    get_spark_session,
    POSTGRES_ENABLED,
    path_exists,
    RAW_PRODUCTS,
    RAW_CARRIERS,
    RAW_CUSTOMERS,
    RAW_DELIVERIES,
    RAW_INVENTORY,
    RAW_PURCHASES,
    RAW_SALES,
    RAW_SUPPLIERS,
    RAW_WAREHOUSES,
)

# Imports de toutes les fonctions d'ingestion Bronze
from scripts.bronze.load_products_raw_to_bronze import load_products_raw_to_bronze
from scripts.bronze.load_carriers_raw_to_bronze import load_carriers_raw_to_bronze
from scripts.bronze.load_customers_raw_to_bronze import load_customers_raw_to_bronze
from scripts.bronze.load_deliveries_raw_to_bronze import load_deliveries_raw_to_bronze
from scripts.bronze.load_inventory_movements_raw_to_bronze import load_inventory_movements_raw_to_bronze
from scripts.bronze.load_purchase_orders_raw_to_bronze import load_purchase_orders_raw_to_bronze
from scripts.bronze.load_sales_orders_raw_to_bronze import load_sales_orders_raw_to_bronze
from scripts.bronze.load_suppliers_raw_to_bronze import load_suppliers_raw_to_bronze
from scripts.bronze.load_warehouses_raw_to_bronze import load_warehouses_raw_to_bronze

# Imports de toutes les fonctions de transformation Silver
from scripts.silver.load_products_bronze_to_silver import load_products_bronze_to_silver
from scripts.silver.load_carriers_bronze_to_silver import load_carriers_bronze_to_silver
from scripts.silver.load_customers_bronze_to_silver import load_customers_bronze_to_silver
from scripts.silver.load_deliveries_bronze_to_silver import load_deliveries_bronze_to_silver
from scripts.silver.load_inventory_movements_bronze_to_silver import load_inventory_movements_bronze_to_silver
from scripts.silver.load_purchase_orders_bronze_to_silver import load_purchase_orders_bronze_to_silver
from scripts.silver.load_sales_orders_bronze_to_silver import load_sales_orders_bronze_to_silver
from scripts.silver.load_suppliers_bronze_to_silver import load_suppliers_bronze_to_silver
from scripts.silver.load_warehouses_bronze_to_silver import load_warehouses_bronze_to_silver

# Imports de toutes les fonctions Gold - Dimensions
from scripts.gold.load_dim_customers import load_dim_customers
from scripts.gold.load_dim_products import load_dim_products
from scripts.gold.load_dim_carriers import load_dim_carriers
from scripts.gold.load_dim_suppliers import load_dim_suppliers
from scripts.gold.load_dim_warehouses import load_dim_warehouses

# Imports de toutes les fonctions Gold - Facts
from scripts.gold.load_fact_sales import load_fact_sales
from scripts.gold.load_fact_purchases import load_fact_purchases
from scripts.gold.load_fact_deliveries import load_fact_deliveries
from scripts.gold.load_fact_inventory_movements import load_fact_inventory_movements


def main():
    # ==================== AFFICHAGE CONFIG ====================
    print("=" * 60)
    print("🚀 PIPELINE DE DONNÉES SUPPLY CHAIN")
    print("=" * 60)
    print(f"💾 Mode         : LOCAL")
    print("=" * 60)

    # ==================== SESSION SPARK ====================
    # Before creating the Spark session, verify raw input files exist
    def verify_raw_files():
        raw_files = {
            "products": RAW_PRODUCTS,
            "carriers": RAW_CARRIERS,
            "customers": RAW_CUSTOMERS,
            "deliveries": RAW_DELIVERIES,
            "inventory_movements": RAW_INVENTORY,
            "purchase_orders": RAW_PURCHASES,
            "sales_orders": RAW_SALES,
            "suppliers": RAW_SUPPLIERS,
            "warehouses": RAW_WAREHOUSES,
        }

        missing = []
        for name, p in raw_files.items():
            if not path_exists(p):
                missing.append((name, p))

        if missing:
            print("\n❌ Fichiers RAW manquants ou inaccessibles :")
            for name, p in missing:
                print(f" - {name}: {p}")
            print("\nConseils :")
            print(" - Vérifiez que les fichiers existent sous le dossier 'data/raw'.")
            raise RuntimeError("Fichiers RAW manquants - interrompre l'exécution")

    verify_raw_files()

    spark = get_spark_session("Supply Chain Complete Master Pipeline")
    print(f"✅ Spark initialisé : {spark.version}")

    try:
        # ==================================================
        # COUCHE BRONZE
        # ==================================================
        print("\n" + "=" * 60)
        print("⚡ COUCHE BRONZE — INGESTION RAW → BRONZE")
        print("=" * 60)

        print("\n--- [1/9] INGESTION DES PRODUITS ---")
        load_products_raw_to_bronze(spark)

        print("\n--- [2/9] INGESTION DES TRANSPORTEURS ---")
        load_carriers_raw_to_bronze(spark)

        print("\n--- [3/9] INGESTION DES CLIENTS ---")
        load_customers_raw_to_bronze(spark)

        print("\n--- [4/9] INGESTION DES LIVRAISONS ---")
        load_deliveries_raw_to_bronze(spark)

        print("\n--- [5/9] INGESTION DES MOUVEMENTS DE STOCK ---")
        load_inventory_movements_raw_to_bronze(spark)

        print("\n--- [6/9] INGESTION DES COMMANDES D'ACHAT ---")
        load_purchase_orders_raw_to_bronze(spark)

        print("\n--- [7/9] INGESTION DES COMMANDES DE VENTE ---")
        load_sales_orders_raw_to_bronze(spark)

        print("\n--- [8/9] INGESTION DES FOURNISSEURS ---")
        load_suppliers_raw_to_bronze(spark)

        print("\n--- [9/9] INGESTION DES ENTREPÔTS ---")
        load_warehouses_raw_to_bronze(spark)

        print("\n🏆 COUCHE BRONZE COMPLÈTE !")

        # ==================================================
        # COUCHE SILVER
        # ==================================================
        print("\n" + "=" * 60)
        print("⚡ COUCHE SILVER — TRANSFORMATION BRONZE → SILVER")
        print("=" * 60)

        print("\n--- [1/9] TRANSFORMATION DES PRODUITS ---")
        load_products_bronze_to_silver(spark)

        print("\n--- [2/9] TRANSFORMATION DES TRANSPORTEURS ---")
        load_carriers_bronze_to_silver(spark)

        print("\n--- [3/9] TRANSFORMATION DES CLIENTS ---")
        load_customers_bronze_to_silver(spark)

        print("\n--- [4/9] TRANSFORMATION DES LIVRAISONS ---")
        load_deliveries_bronze_to_silver(spark)

        print("\n--- [5/9] TRANSFORMATION DES MOUVEMENTS DE STOCK ---")
        load_inventory_movements_bronze_to_silver(spark)

        print("\n--- [6/9] TRANSFORMATION DES COMMANDES D'ACHAT ---")
        load_purchase_orders_bronze_to_silver(spark)

        print("\n--- [7/9] TRANSFORMATION DES COMMANDES DE VENTE ---")
        load_sales_orders_bronze_to_silver(spark)

        print("\n--- [8/9] TRANSFORMATION DES FOURNISSEURS ---")
        load_suppliers_bronze_to_silver(spark)

        print("\n--- [9/9] TRANSFORMATION DES ENTREPÔTS ---")
        load_warehouses_bronze_to_silver(spark)

        print("\n🏆 COUCHE SILVER COMPLÈTE !")

        # ==================================================
        # COUCHE GOLD — DIMENSIONS
        # ==================================================
        print("\n" + "=" * 60)
        print("✨ COUCHE GOLD — DIMENSIONS")
        print("=" * 60)

        print("\n--- [1/5] DIMENSION CUSTOMERS ---")
        load_dim_customers(spark)

        print("\n--- [2/5] DIMENSION PRODUCTS ---")
        load_dim_products(spark)

        print("\n--- [3/5] DIMENSION CARRIERS ---")
        load_dim_carriers(spark)

        print("\n--- [4/5] DIMENSION SUPPLIERS ---")
        load_dim_suppliers(spark)

        print("\n--- [5/5] DIMENSION WAREHOUSES ---")
        load_dim_warehouses(spark)

        print("\n✅ Toutes les dimensions créées !")

        # ==================================================
        # COUCHE GOLD — FACTS
        # ==================================================
        print("\n" + "=" * 60)
        print("✨ COUCHE GOLD — TABLES DE FAITS")
        print("=" * 60)

        print("\n--- [1/4] FACT SALES ---")
        load_fact_sales(spark)

        print("\n--- [2/4] FACT PURCHASES ---")
        load_fact_purchases(spark)

        print("\n--- [3/4] FACT DELIVERIES ---")
        load_fact_deliveries(spark)

        print("\n--- [4/4] FACT INVENTORY MOVEMENTS ---")
        load_fact_inventory_movements(spark)

        print("\n🏆 COUCHE GOLD COMPLÈTE !")
        if POSTGRES_ENABLED:
            print("\n✅ Toutes les tables GOLD ont été écrites dans PostgreSQL.")
        # ==================================================
        # RÉSUMÉ FINAL
        # ==================================================
        print("\n" + "=" * 60)
        print("🎉 PIPELINE TERMINÉ AVEC SUCCÈS !")
        print("=" * 60)
        print("📂 Données disponibles dans : data/")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Erreur critique lors de l'exécution du pipeline : {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        print("\n✅ Session Spark arrêtée proprement.")


if __name__ == "__main__":
    main()