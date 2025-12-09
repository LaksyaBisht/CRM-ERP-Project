# CRM - ERP - Project

This project implements a robust, scalable data pipeline designed to unify and transform customer and product data from both **Customer Relationship Management (CRM)** and **Enterprise Resource Planning (ERP)** source systems into an optimized, star-schema data warehouse for reporting and analytical purposes.

The pipeline is built using **Delta Live Tables (DLT)** and PySpark, following the **Medallion Architecture** (Bronze, Silver, and Gold layers) to enforce data quality and dimensional modeling principles.

## 🏛️ Architecture and Data Flow

The pipeline follows a multi-layered structure, ensuring data is progressively cleaned, standardized, and modeled.

The overall data flow is visually represented in the `docs/CRM-ERP-dataflow.png` diagram.

### 1. Bronze Layer (Raw Ingestion)
This layer ingests raw, incremental data from the source systems directly into Delta tables.

| Source System | Raw Table Name | Description |
| :--- | :--- | :--- |
| **CRM** | `crm_cust_info_raw` | Raw customer details. |
| **CRM** | `crm_prd_info_raw` | Raw product historical information. |
| **CRM** | `crm_sales_details_raw` | Raw sales transaction records. |
| **ERP** | `erp_loc_a101_raw` | Raw customer location/country data. |
| **ERP** | `erp_cust_az12_raw` | Raw ERP customer details (birthdate, gender). |
| **ERP** | `erp_px_cat_g1v2_raw` | Raw product categories and maintenance info. |

### 2. Silver Layer (Cleaning, Standardization, & Dimensions)

This layer applies cleaning rules, enforces data quality expectations (`@dlt.expect`), and builds the Slowly Changing Dimensions (SCDs) for tracking history.

| Table Name | Source Files | Key Transformations & Modeling |
| :--- | :--- | :--- |
| `silver.crm_cust_info_clean` | `02_Silver_Cleaning.py`, `03_Silver_SCD2_Dimensions.py` | SCD Type 1 (Keep Latest) is used for customer deduplication based on `cst_id`. Gender and marital status are standardized (e.g., 'S' to 'Single', 'F' to 'Female'). |
| `silver.crm_prd_info_clean` | `02_Silver_Cleaning.py` | Calculates product history `prd_end_dt` using a `LEAD` window function to define the historical effective period. `prd_line` is standardized (e.g., 'M' to 'Mountain'). |
| `silver.crm_sales_details_clean` | `02_Silver_Cleaning.py` | Imputes sales values when invalid by calculating `sls_quantity * sls_price`. Validates and transforms date formats (e.g., `yyyyMMdd` to `DATE` type). |
| `silver.dim_customers_scd2` | `03_Silver_SCD2_Dimensions.py` | **SCD Type 2 Dimension.** Unifies CRM and ERP customer/location data. Tracks history for `gender`, `marital_status`, `country`, and `birthdate`. |
| `silver.dim_products_scd2` | `03_Silver_SCD2_Dimensions.py` | **SCD Type 2 Dimension.** Unifies CRM product history and ERP categories. Tracks history for `cost`, `category`, `subcategory`, `maintenance`, and `product_line`. |

### 3. Gold Layer (Reporting)

This layer presents the final, joined, and aggregated data in a star schema, optimized for business intelligence and reporting. Only the currently active dimension records are exposed to the end-users by filtering for `__END_AT IS NULL`.

| Table Name | Type | Description |
| :--- | :--- | :--- |
| `gold.fact_sales` | Fact | Contains sales transactional metrics (quantity, sales amount, price) and foreign keys to the dimension tables (`product_key`, `customer_key`). |
| `gold.dim_customers` | Dimension | Contains the current active view of customer attributes. |
| `gold.dim_products` | Dimension | Contains the current active view of product attributes. |

## 🔗 Data Model

The Gold Layer is structured as a star schema (Fact and two Dimensions). The relationships and schema are detailed in the `docs/CRM-ERP-datamodel.png` file.

| Table | Primary Key | Foreign Keys | Key Attributes |
| :--- | :--- | :--- | :--- |
| **gold.fact\_sales** | `order_number` | `product_key` (FK), `customer_key` (FK) | `order_date`, `sales_amount`, `quantity`, `price` |
| **gold.dim\_customers** | `customer_key` (PK) | | `customer_number`, `first_name`, `country`, `birthdate`, `gender`, `marital_status` |
| **gold.dim\_products** | `product_key` (PK) | | `product_number`, `product_name`, `cost`, `category`, `subcategory`, `product_line` |

## 🔑 Source System Relationships

The data unification relies on establishing key relationships between the CRM and ERP systems, as illustrated in `docs/CRM-ERP-relationship.png`.

* **Customer Unification:** The CRM customer key (`cst_key`) is used to join to the ERP customer/location ID (`cid`) from `erp_cust_az12_raw` and `erp_loc_a101_raw`.
* **Product Unification:** A parsed product category ID (`cat_id`, derived from `prd_key` in the CRM product info) is used to join to the ERP category ID (`id`) from `erp_px_cat_g1v2_raw`.
