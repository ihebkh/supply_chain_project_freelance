# 📊 Couche GOLD - Data Warehouse Model

## Architecture: Star Schema (Schéma en Étoile)

La couche Gold implémente un modèle de data warehouse professionnel optimisé pour l'analyse et le reporting.

```
                        ┌─────────────────────┐
                        │  dim_customers      │
                        │  ├─ customer_key    │
                        │  ├─ customer_id     │
                        │  ├─ customer_name   │
                        │  ├─ region          │
                        │  ├─ segment         │
                        │  └─ is_active       │
                        └─────────────────────┘
                               ▲
                               │
        ┌──────────────────────┴──────────────────────┐
        │                                             │
        │ (customer_id)                      (customer_id)
        │                                             │
   ┌─────────────┐                           ┌──────────────┐
   │ fact_sales  │                           │fact_inventory│
   │ ├─ sale_id  │◄─────────────────────────►│ movements    │
   │ ├─ sale_date│                           └──────────────┘
   │ ├─ quantity │                
   │ └─ amount   │                           
   └─────────────┘
        ▲                                     
        │ (product_id)                       
        │
        │
   ┌────────────────────┐
   │  dim_products      │
   │  ├─ product_key    │
   │  ├─ product_id     │
   │  ├─ product_name   │
   │  ├─ category       │
   │  ├─ brand          │
   │  ├─ unit_price     │
   │  └─ is_active      │
   └────────────────────┘

+ dim_carriers, dim_suppliers, dim_warehouses
+ fact_purchases, fact_deliveries
```

---

## 📋 DIMENSIONS (Tables de Référence)

### **dim_customers**
- `customer_key` (clé surrogate)
- `customer_id` (clé métier)
- `customer_name`
- `region`
- `segment` (B2B, B2C, Education, Healthcare)
- `is_active`

### **dim_products**
- `product_key` (clé surrogate)
- `product_id`
- `product_name`
- `category` (Electronics, etc.)
- `brand`
- `unit_price`
- `stock_minimum`
- `is_active`

### **dim_carriers**
- `carrier_key`
- `carrier_id`
- `carrier_name`
- `carrier_type` (Road, Air, Sea)

### **dim_suppliers**
- `supplier_key`
- `supplier_id`
- `supplier_name`
- `country`
- `city`
- `supplier_type` (International, Local)

### **dim_warehouses**
- `warehouse_key`
- `warehouse_id`
- `warehouse_name`
- `region`
- `capacity`

---

## ⭐ TABLES DE FAITS (Événements Transactionnels)

### **fact_sales**
- `sale_id` (clé primaire)
- `sale_date`
- `customer_id` (FK → dim_customers)
- `customer_name`, `customer_region`
- `product_id` (FK → dim_products)
- `product_name`, `product_category`
- `quantity_sold`
- `sale_unit_price`
- `sale_amount` (quantité × prix)

**Métriques possibles:**
- Total des ventes par client/région/catégorie
- Nombre de transactions
- Prix moyen

---

### **fact_purchases**
- `purchase_id`
- `purchase_date`
- `supplier_id` (FK → dim_suppliers)
- `supplier_name`, `supplier_country`
- `product_id` (FK → dim_products)
- `product_name`, `product_category`
- `quantity_purchased`
- `unit_cost`
- `purchase_amount`

**Métriques possibles:**
- Coûts d'achat par fournisseur
- Performance fournisseur

---

### **fact_deliveries**
- `delivery_id`
- `order_id`
- `order_type` (SALE, PURCHASE)
- `expected_date`
- `actual_date`
- `carrier_id` (FK → dim_carriers)
- `carrier_name`, `carrier_type`
- `status` (On Time, Delayed)
- `days_to_deliver` (calculé)
- `is_on_time` (boolean)

**Métriques possibles:**
- Taux de ponctualité par transporteur
- Délai moyen
- Performance logistique

---

### **fact_inventory_movements**
- `movement_id`
- `movement_date`
- `product_id` (FK → dim_products)
- `product_name`, `product_category`
- `warehouse_id` (FK → dim_warehouses)
- `warehouse_name`, `warehouse_region`
- `movement_type` (IN, OUT, TRANSFER)
- `movement_quantity`

**Métriques possibles:**
- Stock par produit/entrepôt
- Rotation des stocks
- Mouvements par période

---

## 🗄️ Structure de Stockage HDFS

```
hdfs://namenode:8020/supply-chain/gold/
├── dim_customers/        (stockage Parquet)
├── dim_products/
├── dim_carriers/
├── dim_suppliers/
├── dim_warehouses/
├── fact_sales/
├── fact_purchases/
├── fact_deliveries/
└── fact_inventory_movements/
```

---

## 🚀 Utilisation

### Charger la couche Gold
```bash
python main.py
```

### Lancer individuellement une table
```bash
python scripts/gold/load_dim_customers.py
python scripts/gold/load_fact_sales.py
```

### Requête exemple (Spark SQL)
```python
# Après avoir créé les tables
spark.sql("""
    SELECT 
        c.customer_name,
        c.region,
        COUNT(f.sale_id) as num_sales,
        SUM(f.sale_amount) as total_amount,
        AVG(f.sale_amount) as avg_sale
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.customer_name, c.region
    ORDER BY total_amount DESC
""").show()
```

---

## ✅ Avantages du Star Schema

✨ **Performance** - Jointures faciles et rapides
✨ **Simplicité** - Facile à comprendre pour les analystes
✨ **Scalabilité** - Supporte des millions de lignes
✨ **Maintenance** - Structure claire et dénormalisée
✨ **Réutilisabilité** - Une seule dimension peut servir plusieurs faits

---

## 📈 Prochaines Étapes

1. **Visualisation BI** - Connecter Tableau/PowerBI à HDFS
2. **Agrégations** - Créer des tables pré-agrégées (mart de vente, etc.)
3. **SCD (Slowly Changing Dimensions)** - Historique des dimensions
4. **Partitioning** - Partitionner par date pour performance
5. **Incremental Load** - Charger uniquement les données nouvelles/modifiées
