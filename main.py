import sys
import os
from pyspark.sql import SparkSession

# Ajouter le répertoire de base au sys.path pour les imports locaux
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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
    print("🚀 Initialisation du pipeline de données Supply Chain complet...")
    
    # Création de la session Spark unique
    spark = SparkSession.builder \
        .appName("Supply Chain Complete Master Pipeline") \
        .getOrCreate()
    
    try:
        print("\n================================================")
        print("⚡ DÉMARRAGE DE L'INGESTION COMPLÈTE (COUCHE BRONZE) ⚡")
        print("================================================")
        
        # 1. Produits
        print("\n--- [1/9] INGESTION DES PRODUITS ---")
        load_products_raw_to_bronze(spark)
        
        # 2. Transporteurs
        print("\n--- [2/9] INGESTION DES TRANSPORTEURS ---")
        load_carriers_raw_to_bronze(spark)
        
        # 3. Clients
        print("\n--- [3/9] INGESTION DES CLIENTS ---")
        load_customers_raw_to_bronze(spark)
        
        # 4. Livraisons
        print("\n--- [4/9] INGESTION DES LIVRAISONS ---")
        load_deliveries_raw_to_bronze(spark)
        
        # 5. Mouvements de stock
        print("\n--- [5/9] INGESTION DES MOUVEMENTS DE STOCK ---")
        load_inventory_movements_raw_to_bronze(spark)
        
        # 6. Commandes d'achat
        print("\n--- [6/9] INGESTION DES COMMANDES D'ACHAT ---")
        load_purchase_orders_raw_to_bronze(spark)
        
        # 7. Commandes de vente
        print("\n--- [7/9] INGESTION DES COMMANDES DE VENTE ---")
        load_sales_orders_raw_to_bronze(spark)
        
        # 8. Fournisseurs
        print("\n--- [8/9] INGESTION DES FOURNISSEURS ---")
        load_suppliers_raw_to_bronze(spark)
        
        # 9. Entrepôts
        print("\n--- [9/9] INGESTION DES ENTREPÔTS ---")
        load_warehouses_raw_to_bronze(spark)
        
        print("\n================================================")
        print("🏆 TOUTE LA COUCHE BRONZE A ÉTÉ INGÉRÉE AVEC SUCCÈS !")
        print("================================================")
        
        print("\n================================================")
        print("⚡ DÉMARRAGE DES TRANSFORMATIONS (COUCHE SILVER) ⚡")
        print("================================================")
        
        # 1. Produits
        print("\n--- [1/9] TRANSFORMATION DES PRODUITS ---")
        load_products_bronze_to_silver(spark)
        
        # 2. Transporteurs
        print("\n--- [2/9] TRANSFORMATION DES TRANSPORTEURS ---")
        load_carriers_bronze_to_silver(spark)
        
        # 3. Clients
        print("\n--- [3/9] TRANSFORMATION DES CLIENTS ---")
        load_customers_bronze_to_silver(spark)
        
        # 4. Livraisons
        print("\n--- [4/9] TRANSFORMATION DES LIVRAISONS ---")
        load_deliveries_bronze_to_silver(spark)
        
        # 5. Mouvements de stock
        print("\n--- [5/9] TRANSFORMATION DES MOUVEMENTS DE STOCK ---")
        load_inventory_movements_bronze_to_silver(spark)
        
        # 6. Commandes d'achat
        print("\n--- [6/9] TRANSFORMATION DES COMMANDES D'ACHAT ---")
        load_purchase_orders_bronze_to_silver(spark)
        
        # 7. Commandes de vente
        print("\n--- [7/9] TRANSFORMATION DES COMMANDES DE VENTE ---")
        load_sales_orders_bronze_to_silver(spark)
        
        # 8. Fournisseurs
        print("\n--- [8/9] TRANSFORMATION DES FOURNISSEURS ---")
        load_suppliers_bronze_to_silver(spark)
        
        # 9. Entrepôts
        print("\n--- [9/9] TRANSFORMATION DES ENTREPÔTS ---")
        load_warehouses_bronze_to_silver(spark)
        
        print("\n================================================")
        print("🏆 TOUTE LA COUCHE SILVER A ÉTÉ TRANSFORMÉE AVEC SUCCÈS !")
        print("================================================")
        
        print("\n================================================")
        print("✨ DÉMARRAGE DE LA COUCHE GOLD (DATA WAREHOUSE) ✨")
        print("================================================")
        
        # ============== DIMENSIONS ==============
        print("\n=== CRÉATION DES DIMENSIONS ===")
        
        # 1. Dimension Customers
        print("\n--- [1/5] CRÉATION DIMENSION CUSTOMERS ---")
        load_dim_customers(spark)
        
        # 2. Dimension Products
        print("\n--- [2/5] CRÉATION DIMENSION PRODUCTS ---")
        load_dim_products(spark)
        
        # 3. Dimension Carriers
        print("\n--- [3/5] CRÉATION DIMENSION CARRIERS ---")
        load_dim_carriers(spark)
        
        # 4. Dimension Suppliers
        print("\n--- [4/5] CRÉATION DIMENSION SUPPLIERS ---")
        load_dim_suppliers(spark)
        
        # 5. Dimension Warehouses
        print("\n--- [5/5] CRÉATION DIMENSION WAREHOUSES ---")
        load_dim_warehouses(spark)
        
        print("\n✅ Toutes les dimensions ont été créées avec succès !")
        
        # ============== TABLES DE FAITS ==============
        print("\n=== CRÉATION DES TABLES DE FAITS ===")
        
        # 1. Fact Sales
        print("\n--- [1/4] CRÉATION FACT SALES ---")
        load_fact_sales(spark)
        
        # 2. Fact Purchases
        print("\n--- [2/4] CRÉATION FACT PURCHASES ---")
        load_fact_purchases(spark)
        
        # 3. Fact Deliveries
        print("\n--- [3/4] CRÉATION FACT DELIVERIES ---")
        load_fact_deliveries(spark)
        
        # 4. Fact Inventory Movements
        print("\n--- [4/4] CRÉATION FACT INVENTORY MOVEMENTS ---")
        load_fact_inventory_movements(spark)
        
        print("\n================================================")
        print("🏆 TOUTE LA COUCHE GOLD A ÉTÉ CRÉÉE AVEC SUCCÈS !")
        print("================================================")
        
    except Exception as e:
        print(f"\n❌ Erreur critique lors de l'exécution du pipeline : {e}")
    finally:
        # Assurer la libération des ressources Spark
        spark.stop()

if __name__ == "__main__":
    main()