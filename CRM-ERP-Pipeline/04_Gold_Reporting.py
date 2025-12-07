import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Sales Fact Table
@dlt.table(
    name="gold.fact_sales",
    comment="Final Fact table for sales transactions, joined to Customer and Product dimensions.",
    table_properties={"quality": "gold"}
)
def fact_sales():
    return (
        dlt.read("silver.crm_sales_details_clean").alias("sd")
        .join(dlt.read("silver.dim_customers_scd2").alias("cu"),
              on=[(col("sd.sls_cust_id") == col("cu.customer_key"))],
              how="left")
        .join(dlt.read("silver.dim_products_scd2").alias("pr"),
              on=[(col("sd.sls_prd_key") == col("pr.product_number"))],
              how="left") 
        .select(
            col("sd.sls_ord_num").alias("order_number"),
            col("pr.product_key").alias("product_key"),
            col("cu.customer_key").alias("customer_key"),
            
            col("sd.sls_order_dt").alias("order_date"),
            col("sd.sls_ship_dt").alias("shipping_date"),
            col("sd.sls_due_dt").alias("due_date"),
            col("sd.sls_sales").alias("sales_amount"),
            col("sd.sls_quantity").alias("quantity"),
            col("sd.sls_price").alias("price")
        )
    )

# Customer Dimension Table
@dlt.table(
    name="gold.dim_customers",
    comment="Current active customer dimension (filters out historical records from SCD2).",
    table_properties={"quality": "gold"}
)
def dim_customers():
    return (
        dlt.read("silver.dim_customers_scd2")
        .filter(col("__END_AT").isNull()) 
        .select(
            "customer_key",
            "customer_number",
            "first_name",
            "last_name",
            "marital_status",
            "gender",
            "birthdate",
            "country",
            "create_date"
        )
    )

# Product Dimension Table
@dlt.table(
    name="gold.dim_products",
    comment="Current active product dimension (filters out historical records from SCD2).",
    table_properties={"quality": "gold"}
)
def dim_products():
    return (
        dlt.read("silver.dim_products_scd2")
        .filter(col("__END_AT").isNull())
        .select(
            "product_key",
            "product_number",
            "product_name",
            "category_id",
            "category",
            "subcategory",
            "maintenance",
            "cost",
            "product_line",
            "start_date"
        )
    )