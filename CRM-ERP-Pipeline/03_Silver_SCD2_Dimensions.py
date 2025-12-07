import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# CUSTOMER DEDUPLICATION (SCD Type 1)

dlt.create_streaming_table(
    name="silver.crm_cust_info_clean", 
    comment="Final deduplicated CRM Customer records (SCD1: Keep Latest)."
)

dlt.apply_changes(
    target = "silver.crm_cust_info_clean",
    source = "silver.crm_cust_info_clean_stage",
    keys = ["cst_id"], 
    sequence_by = col("cst_create_date"),
    stored_as_scd_type = "1" 
)

# CUSTOMER DIMENSION UNIFICATION (SCD Type 2) 

@dlt.table(
    name="silver.customer_unified_stage", 
    comment="Unifies customer data for SCD Type 2 tracking.",
    table_properties={"quality": "silver"}
)
def customer_unified_stage():
    df_crm = dlt.read("silver.crm_cust_info_clean").alias("crm") 
    
    df_join_az = df_crm.join(
        dlt.read("silver.erp_cust_az12_clean").alias("az"),
        on=col("crm.cst_key") == col("az.cid"),
        how="left"
    )
    
    df_final = df_join_az.join(
        dlt.read("silver.erp_loc_a101_clean").alias("loc"),
        on=col("crm.cst_key") == col("loc.cid"),
        how="left"
    )
    
    return (
        df_final.select(
            col("crm.cst_id").alias("customer_key"), 
            
            col("crm.cst_key").alias("customer_number"),
            col("crm.cst_firstname").alias("first_name"),
            col("crm.cst_lastname").alias("last_name"),
            col("crm.cst_marital_status").alias("marital_status"),
            col("crm.cst_create_date").alias("create_date"),
            
            when(col("crm.cst_gndr") != "n/a", col("crm.cst_gndr"))
            .otherwise(coalesce(col("az.gen"), lit("n/a"))).alias("gender"),
            
            col("az.bdate").alias("birthdate"),
            col("loc.cntry").alias("country"),
            coalesce(col("crm.dwh_create_date"), col("az.dwh_create_date"), col("loc.dwh_create_date")).alias("change_timestamp") 
        )
    )

dlt.create_streaming_table(
    name="silver.dim_customers_scd2",
    comment="Final Customer Dimension maintained as a Type 2 Slowly Changing Dimension.",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target = "silver.dim_customers_scd2", 
    source = "silver.customer_unified_stage", 
    keys = ["customer_key"], 
    sequence_by = col("change_timestamp"), 
    track_history_column_list = ["gender", "marital_status", "country", "birthdate"],
    stored_as_scd_type = "2"
)

# PRODUCT DIMENSION UNIFICATION (SCD Type 2) 

@dlt.table(
    name="silver.product_unified_stage",
    comment="Unifies product data for SCD Type 2 tracking.",
    table_properties={"quality": "silver"}
)
def product_unified_stage():
    return (
        dlt.read("silver.crm_prd_info_clean").alias("pn")
        .join(dlt.read("silver.erp_px_cat_g1v2_clean").alias("pc"), 
              on=col("pn.cat_id") == col("pc.id"),
              how="left")
        .select(
            col("pn.prd_id").cast("integer").alias("product_key"), 
            col("pn.prd_key").alias("product_number"),
            col("pn.prd_nm").alias("product_name"),
            col("pn.cat_id").alias("category_id"),
            col("pn.prd_start_dt").alias("start_date"),
            col("pn.prd_cost").alias("cost"),
            col("pn.prd_line").alias("product_line"),
            
            coalesce(col("pc.cat"), lit("n/a")).alias("category"),
            coalesce(col("pc.subcat"), lit("n/a")).alias("subcategory"),
            coalesce(col("pc.maintenance"), lit("n/a")).alias("maintenance"),
            
            col("pn.dwh_create_date").alias("change_timestamp") 
        )
    )

dlt.create_streaming_table(
    name="silver.dim_products_scd2",
    comment="Final Product Dimension maintained as a Type 2 Slowly Changing Dimension.",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target = "silver.dim_products_scd2", 
    source = "silver.product_unified_stage", 
    keys = ["product_key"], 
    sequence_by = col("change_timestamp"), 
    track_history_column_list = ["cost", "category", "subcategory", "maintenance", "product_line"],
    stored_as_scd_type = "2"
)
