import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# CRM Customer Info
@dlt.table(
    name="silver.crm_cust_info_clean_stage",
    comment="Cleaned customer info (no deduplication yet), ready for SCD1 merge.",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_cst_id", "cst_id IS NOT NULL")
def crm_cust_info_clean_stage():
    return (
        dlt.read_stream("crm_cust_info_raw")
        .select(
            col("cst_id").cast("integer").alias("cst_id"),
            replace(col("cst_key"), lit("-"), lit("")).alias("cst_key"),
            
            trim(col("cst_firstname")).alias("cst_firstname"),
            trim(col("cst_lastname")).alias("cst_lastname"),

            when(upper(trim(col("cst_marital_status"))) == 'S', lit('Single'))
            .when(upper(trim(col("cst_marital_status"))) == 'M', lit('Married'))
            .otherwise(lit('n/a')).alias("cst_marital_status"),
            
            when(upper(trim(col("cst_gndr"))) == 'F', lit('Female'))
            .when(upper(trim(col("cst_gndr"))) == 'M', lit('Male'))
            .otherwise(lit('n/a')).alias("cst_gndr"),
            
            col("cst_create_date").cast("date").alias("cst_create_date"),
            current_timestamp().alias("dwh_create_date")
        )
    )

# CRM Product Info
@dlt.table(
    name="silver.crm_prd_info_clean",
    comment="Cleaned product history data with calculated end dates and standardized lines.",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_prd_id", "prd_id IS NOT NULL")
@dlt.expect("non_negative_cost", "prd_cost >= 0")
def crm_prd_info_clean():
    window_spec = Window.partitionBy("prd_key").orderBy(col("prd_start_dt"))
    return (
        dlt.read("crm_prd_info_raw") 
        .select(
            col("prd_id").cast("integer").alias("prd_id"),

            replace(substring(col("prd_key"), 1, 5), lit("-"), lit("_")).alias("cat_id"),
            substring(col("prd_key"), 7, length(col("prd_key"))).alias("prd_key"),
            
            trim(col("prd_nm")).alias("prd_nm"),
            coalesce(col("prd_cost").cast("integer"), lit(0)).alias("prd_cost"), 

            when(upper(trim(col("prd_line"))) == 'M', lit('Mountain'))
            .when(upper(trim(col("prd_line"))) == 'R', lit('Road'))
            .when(upper(trim(col("prd_line"))) == 'T', lit('Touring'))
            .when(upper(trim(col("prd_line"))) == 'S', lit('Other Sales'))
            .otherwise(lit('n/a')).alias("prd_line"),

            col("prd_start_dt").cast("date").alias("prd_start_dt"),

            date_sub(
                lead(col("prd_start_dt"), 1).over(window_spec).cast("date"),
                1
            ).alias("prd_end_dt"),

            current_timestamp().alias("dwh_create_date")
        )
    )

# CRM Sales Details
@dlt.table(
    name="silver.crm_sales_details_clean",
    comment="Cleaned sales transaction data with imputed values and validated dates.",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sls_ord_num", "sls_ord_num IS NOT NULL")
@dlt.expect("positive_quantity", "sls_quantity > 0")
def crm_sales_details_clean():
    df_raw = dlt.read_stream("crm_sales_details_raw")

    def clean_date_field(col_name):
        return (
            when((col(col_name) == 0) | (length(col(col_name).cast("string")) != 8), lit(None))
            .otherwise(to_date(col(col_name).cast("string"), "yyyyMMdd"))
        )
    
    df_imputed = df_raw.withColumn("imputed_sales", col("sls_quantity") * abs(col("sls_price").cast("integer")))
    
    return (
        df_imputed.select(
            col("sls_ord_num").alias("sls_ord_num"),
            col("sls_prd_key").alias("sls_prd_key"),
            col("sls_cust_id").cast("integer").alias("sls_cust_id"),

            clean_date_field("sls_order_dt").alias("sls_order_dt"),
            clean_date_field("sls_ship_dt").alias("sls_ship_dt"),
            clean_date_field("sls_due_dt").alias("sls_due_dt"),

            when((col("sls_sales").isNull()) | (col("sls_sales") <= 0) | (col("sls_sales") != col("imputed_sales")), col("imputed_sales"))
            .otherwise(col("sls_sales")).alias("sls_sales"),
            
            col("sls_quantity").cast("integer").alias("sls_quantity"),
            
            when((col("sls_price").isNull()) | (col("sls_price") <= 0), col("sls_sales") / nullif(col("sls_quantity"), lit(0)))
            .otherwise(abs(col("sls_price"))).alias("sls_price"),

            current_timestamp().alias("dwh_create_date")
        )
    )

# ERP Location Data
@dlt.table(
    name="silver.erp_loc_a101_clean",
    comment="Cleaned ERP location data with standardized country names.",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_cid", "cid IS NOT NULL") 
def erp_loc_a101_clean():
    return (
        dlt.read_stream("erp_loc_a101_raw")
        .select(
            replace(col("cid"), lit("-"), lit("")).alias("cid"),
            when(trim(col("cntry")) == "DE", lit("Germany"))
            .when(trim(col("cntry")).isin("US", "USA"), lit("United States"))
            .when(trim(col("cntry")) == "" , lit("n/a"))
            .when(col("cntry").isNull(), lit("n/a"))
            .otherwise(trim(col("cntry"))).alias("cntry"),
            
            current_timestamp().alias("dwh_create_date")
        )
    )

# ERP Customer Details
@dlt.table(
    name="silver.erp_cust_az12_clean",
    comment="Cleaned ERP customer details with standardized gender and validated birth dates.",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_cid", "cid IS NOT NULL")
@dlt.expect("valid_bdate_range", "bdate IS NULL OR bdate < current_date()")
def erp_cust_az12_clean():
    return (
        dlt.read_stream("erp_cust_az12_raw")
        .select(
            when(col("cid").startswith("NAS"), substring(col("cid"), 4, length(col("cid"))))
            .otherwise(col("cid")).alias("cid"),
            
            when(col("bdate").cast("date") > current_date(), lit(None))
            .otherwise(col("bdate").cast("date")).alias("bdate"),
            
            when(upper(trim(col("gen"))).isin("F", "FEMALE"), lit("Female"))
            .when(upper(trim(col("gen"))).isin("M", "MALE"), lit("Male"))
            .otherwise(lit("n/a")).alias("gen"),
            
            current_timestamp().alias("dwh_create_date")
        )
    )

# ERP Product Categories
@dlt.table(
    name="silver.erp_px_cat_g1v2_clean",
    comment="Cleaned ERP Product Category data with trimmed fields.",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_id", "id IS NOT NULL")
def erp_px_cat_g1v2_clean():
    return (
        dlt.read_stream("erp_px_cat_g1v2_raw")
        .select(
            col("id").alias("id"),
            trim(col("cat")).alias("cat"),
            trim(col("subcat")).alias("subcat"),
            trim(col("maintenance")).alias("maintenance"),
            
            current_timestamp().alias("dwh_create_date")
        )
    )