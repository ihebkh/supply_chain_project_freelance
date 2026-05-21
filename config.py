"""
Configuration centralisée pour les chemins de données
Supporte local et HDFS
"""

import os

# ==================== CONFIGURATION ====================
# Activer/désactiver HDFS
USE_HDFS = False  # Changé à False pour utiliser le système de fichiers local

# Configuration HDFS
HDFS_NAMENODE = "hdfs://localhost:9000"  # Modifier selon votre cluster
HDFS_BASE_PATH = f"{HDFS_NAMENODE}/supply-chain"

# Configuration Local
LOCAL_BASE_PATH = "data"

# ==================== FONCTIONS UTILITAIRES ====================

def get_path(layer: str, entity: str) -> str:
    """
    Retourne le chemin complet selon la configuration (HDFS ou Local)
    
    Args:
        layer: 'raw', 'bronze', 'silver', 'gold'
        entity: 'carriers', 'products', etc.
    
    Returns:
        Chemin complet (local ou HDFS)
    """
    if USE_HDFS:
        return f"{HDFS_BASE_PATH}/{layer}/{entity}"
    else:
        return f"{LOCAL_BASE_PATH}/{layer}/{entity}"

def get_layer_path(layer: str) -> str:
    """Retourne le chemin de base pour une couche"""
    if USE_HDFS:
        return f"{HDFS_BASE_PATH}/{layer}"
    else:
        return f"{LOCAL_BASE_PATH}/{layer}"

# ==================== CHEMINS PREDÉFINIS ====================

# RAW LAYER
RAW_CARRIERS = get_path("raw", "carriers.csv")
RAW_CUSTOMERS = get_path("raw", "customers.csv")
RAW_PRODUCTS = get_path("raw", "products.csv")
RAW_DELIVERIES = get_path("raw", "deliveries.csv")
RAW_INVENTORY = get_path("raw", "inventory_movements.csv")
RAW_PURCHASES = get_path("raw", "purchase_orders.csv")
RAW_SALES = get_path("raw", "sales_orders.csv")
RAW_SUPPLIERS = get_path("raw", "suppliers.csv")
RAW_WAREHOUSES = get_path("raw", "warehouses.csv")

# BRONZE LAYER
BRONZE_CARRIERS = get_path("bronze", "carriers")
BRONZE_CUSTOMERS = get_path("bronze", "customers")
BRONZE_PRODUCTS = get_path("bronze", "products")
BRONZE_DELIVERIES = get_path("bronze", "deliveries")
BRONZE_INVENTORY = get_path("bronze", "inventory_movements")
BRONZE_PURCHASES = get_path("bronze", "purchase_orders")
BRONZE_SALES = get_path("bronze", "sales_orders")
BRONZE_SUPPLIERS = get_path("bronze", "suppliers")
BRONZE_WAREHOUSES = get_path("bronze", "warehouses")

# SILVER LAYER
SILVER_CARRIERS = get_path("silver", "carriers")
SILVER_CUSTOMERS = get_path("silver", "customers")
SILVER_PRODUCTS = get_path("silver", "products")
SILVER_DELIVERIES = get_path("silver", "deliveries")
SILVER_INVENTORY = get_path("silver", "inventory_movements")
SILVER_PURCHASES = get_path("silver", "purchase_orders")
SILVER_SALES = get_path("silver", "sales_orders")
SILVER_SUPPLIERS = get_path("silver", "suppliers")
SILVER_WAREHOUSES = get_path("silver", "warehouses")

# GOLD LAYER - DIMENSIONS
GOLD_DIM_CUSTOMERS = get_path("gold", "dim_customers")
GOLD_DIM_PRODUCTS = get_path("gold", "dim_products")
GOLD_DIM_CARRIERS = get_path("gold", "dim_carriers")
GOLD_DIM_SUPPLIERS = get_path("gold", "dim_suppliers")
GOLD_DIM_WAREHOUSES = get_path("gold", "dim_warehouses")
GOLD_DIM_DATE = get_path("gold", "dim_date")

# GOLD LAYER - FACTS
GOLD_FACT_SALES = get_path("gold", "fact_sales")
GOLD_FACT_PURCHASES = get_path("gold", "fact_purchases")
GOLD_FACT_DELIVERIES = get_path("gold", "fact_deliveries")
GOLD_FACT_INVENTORY = get_path("gold", "fact_inventory_movements")
