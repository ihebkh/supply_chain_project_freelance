import os
from dotenv import load_dotenv
load_dotenv()


# ==================== CONFIGURATION ====================

# Activer/désactiver HDFS via variable d'environnement ou directement ici
USE_HDFS = os.getenv("USE_HDFS", "false").lower() == "true"

# ==================== CONFIGURATION HDFS ====================

HDFS_NAMENODE_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_NAMENODE_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_WEB_PORT      = os.getenv("HDFS_WEB_PORT", "9870")   # port WebHDFS / UI
HDFS_USER          = os.getenv("HDFS_USER", "hadoop")

HDFS_NAMENODE  = f"hdfs://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}"
HDFS_BASE_PATH = f"{HDFS_NAMENODE}/supply-chain"

# ==================== CONFIGURATION LOCALE ====================

LOCAL_BASE_PATH = os.getenv("LOCAL_BASE_PATH", "data")

# ==================== FONCTIONS UTILITAIRES ====================

def get_path(layer: str, entity: str) -> str:
    """
    Retourne le chemin complet selon la configuration (HDFS ou Local).

    Args:
        layer  : 'raw', 'bronze', 'silver', 'gold'
        entity : 'carriers', 'carriers.csv', 'dim_customers', etc.

    Returns:
        Chemin complet (local ou HDFS)
    """
    if USE_HDFS:
        return f"{HDFS_BASE_PATH}/{layer}/{entity}"
    else:
        return f"{LOCAL_BASE_PATH}/{layer}/{entity}"


def get_layer_path(layer: str) -> str:
    """Retourne le chemin de base pour une couche (raw / bronze / silver / gold)."""
    if USE_HDFS:
        return f"{HDFS_BASE_PATH}/{layer}"
    else:
        return f"{LOCAL_BASE_PATH}/{layer}"


def get_spark_session(app_name: str = "SupplyChainPipeline"):
    """
    Crée et retourne une SparkSession préconfigurée pour HDFS ou local.

    Nécessite : pyspark installé (`pip install pyspark`).
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    if USE_HDFS:
        builder = (
            builder
            .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
            # Force DataNode communication via IP address instead of hostname
            # (fixes Docker hostname resolution issues when Spark runs on host)
            .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "false")
            # Désactiver les permissions HDFS si votre cluster est en mode simple
            .config("spark.hadoop.dfs.permissions.enabled", "false")
        )

    return builder.getOrCreate()


def get_hdfs_client():
    """
    Retourne un client WebHDFS léger (bibliothèque `hdfs`).
    Utile pour : vérifier l'existence d'un fichier, créer des dossiers,
    lister des chemins, uploader de petits fichiers.

    Nécessite : pip install hdfs
    """
    if not USE_HDFS:
        raise RuntimeError("HDFS désactivé. Passez USE_HDFS=true pour l'utiliser.")

    from hdfs import InsecureClient
    url = f"http://{HDFS_NAMENODE_HOST}:{HDFS_WEB_PORT}"
    return InsecureClient(url, user=HDFS_USER)


def ensure_dir(path: str):
    """
    - En mode LOCAL  : crée le répertoire s'il n'existe pas.
    - En mode HDFS   : Spark crée les dossiers automatiquement à l'écriture.
                       Cette fonction ne fait rien (no-op).
    """
    if not USE_HDFS:
        os.makedirs(path, exist_ok=True)


def path_exists(path: str) -> bool:
    """
    Vérifie si un chemin existe (local ou HDFS).
    Pour HDFS, utilise le client WebHDFS.
    """
    if USE_HDFS:
        client = get_hdfs_client()
        # Supprimer le préfixe hdfs://host:port pour WebHDFS
        hdfs_path = "/" + path.split(f":{HDFS_NAMENODE_PORT}/", 1)[-1]
        return client.status(hdfs_path, strict=False) is not None
    else:
        return os.path.exists(path)


def read_parquet(path: str, spark=None):
    """Lit un fichier Parquet (local ou HDFS) via Spark."""
    if spark is None:
        spark = get_spark_session()
    return spark.read.parquet(path)


def write_parquet(df, path: str, mode: str = "overwrite", partition_by: list = None):
    """Écrit un DataFrame Spark en Parquet (local ou HDFS)."""
    ensure_dir(path)
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def read_csv(path: str, spark=None, **kwargs):
    """Lit un fichier CSV (local ou HDFS) via Spark."""
    if spark is None:
        spark = get_spark_session()
    return spark.read.csv(path, header=True, inferSchema=True, **kwargs)


# ==================== CHEMINS PRÉDÉFINIS ====================

# ---------- RAW LAYER ----------
RAW_CARRIERS   = get_path("raw", "carriers.csv")
RAW_CUSTOMERS  = get_path("raw", "customers.csv")
RAW_PRODUCTS   = get_path("raw", "products.csv")
RAW_DELIVERIES = get_path("raw", "deliveries.csv")
RAW_INVENTORY  = get_path("raw", "inventory_movements.csv")
RAW_PURCHASES  = get_path("raw", "purchase_orders.csv")
RAW_SALES      = get_path("raw", "sales_orders.csv")
RAW_SUPPLIERS  = get_path("raw", "suppliers.csv")
RAW_WAREHOUSES = get_path("raw", "warehouses.csv")

# ---------- BRONZE LAYER ----------
BRONZE_CARRIERS   = get_path("bronze", "carriers")
BRONZE_CUSTOMERS  = get_path("bronze", "customers")
BRONZE_PRODUCTS   = get_path("bronze", "products")
BRONZE_DELIVERIES = get_path("bronze", "deliveries")
BRONZE_INVENTORY  = get_path("bronze", "inventory_movements")
BRONZE_PURCHASES  = get_path("bronze", "purchase_orders")
BRONZE_SALES      = get_path("bronze", "sales_orders")
BRONZE_SUPPLIERS  = get_path("bronze", "suppliers")
BRONZE_WAREHOUSES = get_path("bronze", "warehouses")

# ---------- SILVER LAYER ----------
SILVER_CARRIERS   = get_path("silver", "carriers")
SILVER_CUSTOMERS  = get_path("silver", "customers")
SILVER_PRODUCTS   = get_path("silver", "products")
SILVER_DELIVERIES = get_path("silver", "deliveries")
SILVER_INVENTORY  = get_path("silver", "inventory_movements")
SILVER_PURCHASES  = get_path("silver", "purchase_orders")
SILVER_SALES      = get_path("silver", "sales_orders")
SILVER_SUPPLIERS  = get_path("silver", "suppliers")
SILVER_WAREHOUSES = get_path("silver", "warehouses")

# ---------- GOLD LAYER — DIMENSIONS ----------
GOLD_DIM_CUSTOMERS  = get_path("gold", "dim_customers")
GOLD_DIM_PRODUCTS   = get_path("gold", "dim_products")
GOLD_DIM_CARRIERS   = get_path("gold", "dim_carriers")
GOLD_DIM_SUPPLIERS  = get_path("gold", "dim_suppliers")
GOLD_DIM_WAREHOUSES = get_path("gold", "dim_warehouses")
GOLD_DIM_DATE       = get_path("gold", "dim_date")

# ---------- GOLD LAYER — FACTS ----------
GOLD_FACT_SALES      = get_path("gold", "fact_sales")
GOLD_FACT_PURCHASES  = get_path("gold", "fact_purchases")
GOLD_FACT_DELIVERIES = get_path("gold", "fact_deliveries")
GOLD_FACT_INVENTORY  = get_path("gold", "fact_inventory_movements")

# ==================== RÉSUMÉ AU DÉMARRAGE ====================

if __name__ == "__main__":
    mode = "HDFS" if USE_HDFS else "LOCAL"
    base = HDFS_BASE_PATH if USE_HDFS else LOCAL_BASE_PATH
    print(f"[config] Mode actif    : {mode}")
    print(f"[config] Chemin de base: {base}")
    if USE_HDFS:
        print(f"[config] NameNode      : {HDFS_NAMENODE}")
        print(f"[config] WebHDFS port  : {HDFS_WEB_PORT}")
        print(f"[config] Utilisateur   : {HDFS_USER}")
    print()
    print(f"  RAW_CARRIERS    → {RAW_CARRIERS}")
    print(f"  BRONZE_CARRIERS → {BRONZE_CARRIERS}")
    print(f"  SILVER_CARRIERS → {SILVER_CARRIERS}")
    print(f"  GOLD_DIM_DATE   → {GOLD_DIM_DATE}")
    print(f"  GOLD_FACT_SALES → {GOLD_FACT_SALES}")