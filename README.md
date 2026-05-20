# 📦 Supply Chain Project Analyst - Data Pipeline

Ce projet implémente un pipeline d'ingestion et de traitement de données de bout en bout pour une chaîne logistique (**Supply Chain**). Conçu en **Python** et **PySpark**, il repose sur une **Architecture Medallion** (Raw ➔ Bronze ➔ Silver ➔ Gold) pour structurer, nettoyer et valoriser les données opérationnelles.

---

## 🏗️ Architecture du Projet (Medallion)

Le pipeline organise les données en couches successives pour garantir la qualité et la traçabilité des indicateurs :

* **Raw** (`data/raw/`) : Données sources brutes au format CSV (fichiers opérationnels de la chaîne logistique).
* **Bronze** (`data/bronze/`) : Ingestion brute des fichiers CSV convertis au format **Parquet** (historisation et rapidité de lecture).
* **Silver** (`data/silver/`) : Données nettoyées, dédoublonnées, typées et enrichies.
* **Gold** (`data/gold/`) : Tables analytiques finales (faits et dimensions) prêtes pour la création de tableaux de bord décisionnels.

---

## 📊 Modèle de Données (9 Entités Sources)

Le système analyse 9 sources de données clés :
1. **Products** (`products.csv`) : Catalogue de produits, prix unitaires et seuils de stock minimum.
2. **Customers** (`customers.csv`) : Profils clients, régions et segments de marché (B2B, Retail...).
3. **Warehouses** (`warehouses.csv`) : Liste des entrepôts et leurs capacités de stockage.
4. **Suppliers** (`suppliers.csv`) : Fournisseurs locaux et internationaux.
5. **Carriers** (`carriers.csv`) : Transporteurs et modes de livraison (Route, Air, Mer).
6. **Sales Orders** (`sales_orders.csv`) : Flux de commandes de ventes clients.
7. **Purchase Orders** (`purchase_orders.csv`) : Commandes d'approvisionnement auprès des fournisseurs.
8. **Inventory Movements** (`inventory_movements.csv`) : Entrées, sorties et transferts de stocks dans les entrepôts.
9. **Deliveries** (`deliveries.csv`) : Suivi logistique et délais de livraison des commandes.

---

## 📂 Structure des Répertoires

```text
supply chain/
├── data/
│   ├── raw/             # Sources CSV brutes
│   ├── bronze/          # Couche Bronze (Parquet)
│   ├── silver/          # Couche Silver (Nettoyée, Parquet)
│   └── gold/            # Couche Gold (Analytique, Parquet)
├── scripts/
│   ├── bronze/          # Scripts d'ingestion individuelle
│   │   ├── load_products_raw_to_bronze.py
│   │   ├── load_carriers_raw_to_bronze.py
│   │   ├── ...
│   ├── silver/          # Scripts de nettoyage (ex: clean_products_silver.py)
│   └── view_parquet.py  # Utilitaire de visualisation des données Parquet
├── main.py              # Orchestrateur central du pipeline
└── README.md            # Présentation du projet
```

---

## 🚀 Prise en Main & Utilisation

### Prérequis
* Python 3.8+
* Apache Spark (PySpark)
* Java JDK (8, 11, ou 17)

### 1. Lancer l'ingestion complète (Raw ➔ Bronze)
Pour exécuter l'intégralité du pipeline d'ingestion pour les 9 tables d'un seul coup :
```bash
python main.py
```

### 2. Exécuter un chargement spécifique
Chaque script d'ingestion est autonome. Pour ingérer uniquement les commandes de vente :
```bash
python scripts/bronze/load_sales_orders_raw_to_bronze.py
```

### 3. Visualiser rapidement les données Parquet
Un outil d'inspection rapide est inclus pour visualiser vos données stockées sous format Parquet directement dans la console (affiche le schéma, les 20 premières lignes et le nombre total de lignes) :
```bash
# Exemple pour visualiser les stocks ingérés
python scripts/view_parquet.py data/bronze/inventory_movements

# Exemple pour visualiser les produits nettoyés (couche Silver)
python scripts/view_parquet.py data/silver/products
```

---

## 🗺️ Feuille de Route / Next Steps

1. **Généraliser la couche Silver** : Implémenter les scripts de nettoyage, typage des données et déduplication pour les 8 autres tables (sur le modèle de `clean_products`).
2. **Construire la couche Gold (Analytics)** :
   * Créer des vues consolidées (ex. Taux de service des transporteurs, Rotation et alertes de rupture de stock, Rentabilité et Analyse des ventes).
3. **Orchestration Avancée** : Intégrer un outil d'orchestration de flux de données (ex. Apache Airflow ou Prefect).
