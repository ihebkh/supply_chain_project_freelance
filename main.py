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
        
    except Exception as e:
        print(f"\n❌ Erreur critique lors de l'exécution du pipeline : {e}")
    finally:
        # Assurer la libération des ressources Spark
        spark.stop()

if __name__ == "__main__":
    main()