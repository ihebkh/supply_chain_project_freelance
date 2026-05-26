import os
from dotenv import load_dotenv
load_dotenv()


# ==================== CONFIGURATION ====================

# ==================== CONFIGURATION POSTGRES ====================
POSTGRES_ENABLED  = os.getenv("POSTGRES_ENABLED", "false").lower() == "true"
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "supply_chain")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

POSTGRES_URI = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ==================== CONFIGURATION LOCALE ====================

LOCAL_BASE_PATH = os.getenv("LOCAL_BASE_PATH", "data")

# ==================== FONCTIONS UTILITAIRES ====================

def get_path(layer: str, entity: str) -> str:
    """
    Retourne le chemin complet en mode local.

    Args:
        layer  : 'raw', 'bronze', 'silver', 'gold'
        entity : 'carriers', 'carriers.csv', 'dim_customers', etc.

    Returns:
        Chemin complet local
    """
    return f"{LOCAL_BASE_PATH}/{layer}/{entity}"


def get_layer_path(layer: str) -> str:
    """Retourne le chemin de base pour une couche (raw / bronze / silver / gold)."""
    return f"{LOCAL_BASE_PATH}/{layer}"


def get_spark_session(app_name: str = "SupplyChainPipeline"):
    """
    Crée et retourne une SparkSession locale.

    Nécessite : pyspark installé (`pip install pyspark`).
    """
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).getOrCreate()


def get_postgres_connection():
    """
    Retourne une connexion PostgreSQL utilisable par psycopg2.
    """
    import psycopg2

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def write_dataframe_to_postgres(df, table_name: str, pk_columns: list[str]):
    """Crée ou met à jour une table PostgreSQL à partir d'un DataFrame Spark."""
    if not POSTGRES_ENABLED:
        return

    columns = df.columns
    rows = [tuple(None if value is None else value for value in row) for row in df.collect()]
    quoted_columns = ", ".join([f'"{col}" TEXT' for col in columns])
    quoted_names = ", ".join([f'"{col}"' for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))

    pk_definition = ""
    conflict_target = ""
    update_clause = ""
    if pk_columns:
        quoted_pk = ", ".join([f'"{col}"' for col in pk_columns])
        pk_definition = f", PRIMARY KEY ({quoted_pk})"
        conflict_target = f"ON CONFLICT ({quoted_pk})"
        update_columns = [col for col in columns if col not in pk_columns]
        if update_columns:
            update_clause = " DO UPDATE SET " + ", ".join([
                f'"{col}" = EXCLUDED."{col}"' for col in update_columns
            ])
        else:
            update_clause = " DO NOTHING"

    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({quoted_columns}{pk_definition})'
    if not pk_columns:
        insert_sql = f'INSERT INTO "{table_name}" ({quoted_names}) VALUES ({placeholders})'
    else:
        insert_sql = f'INSERT INTO "{table_name}" ({quoted_names}) VALUES ({placeholders}) {conflict_target}{update_clause}'

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            if rows:
                cur.executemany(insert_sql, rows)
        conn.commit()


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def path_exists(path: str) -> bool:
    """
    Vérifie si un chemin existe en local.
    """
    return os.path.exists(path)


def read_parquet(path: str, spark=None):
    """Lit un fichier Parquet via Spark."""
    if spark is None:
        spark = get_spark_session()
    return spark.read.parquet(path)


def write_parquet(df, path: str, mode: str = "overwrite", partition_by: list = None):
    """Écrit un DataFrame Spark en Parquet."""
    ensure_dir(path)
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def read_csv(path: str, spark=None, **kwargs):
    """Lit un fichier CSV via Spark."""
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
    mode = "LOCAL"
    base = LOCAL_BASE_PATH
    print(f"[config] Mode actif    : {mode}")
    print(f"[config] Chemin de base: {base}")
    if POSTGRES_ENABLED:
        print(f"[config] Postgres      : {POSTGRES_USER}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print()
    print(f"  RAW_CARRIERS    → {RAW_CARRIERS}")
    print(f"  BRONZE_CARRIERS → {BRONZE_CARRIERS}")
    print(f"  SILVER_CARRIERS → {SILVER_CARRIERS}")
    print(f"  GOLD_DIM_DATE   → {GOLD_DIM_DATE}")
    print(f"  GOLD_FACT_SALES → {GOLD_FACT_SALES}")