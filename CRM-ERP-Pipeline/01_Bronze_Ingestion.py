import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

BASE_PATH = "/Volumes/crmerpproject/source/raw_data"

# --- CRM Sources ---

@dlt.table(
    name="crm_cust_info_raw",
    comment="Raw customer information loaded incrementally from CRM.",
    table_properties={"quality": "bronze"}
)
def crm_cust_info_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{BASE_PATH}/crm/_checkpoints/cust_info_raw")
        .option("pathGlobFilter", "*cust_info.csv") 
        .load(f"{BASE_PATH}/crm/")
    )

@dlt.table(
    name="crm_prd_info_raw",
    comment="Raw product information loaded incrementally from CRM.",
    table_properties={"quality": "bronze"}
)
def crm_prd_info_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{BASE_PATH}/crm/_checkpoints/prd_info_raw")
        .option("pathGlobFilter", "*prd_info.csv")
        .load(f"{BASE_PATH}/crm/")
    )

@dlt.table(
    name="crm_sales_details_raw",
    comment="Raw sales transaction data loaded incrementally from CRM.",
    table_properties={"quality": "bronze"}
)
def crm_sales_details_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{BASE_PATH}/crm/_checkpoints/sales_details_raw")
        .option("pathGlobFilter", "*sales_details.csv")
        .load(f"{BASE_PATH}/crm/")
    )

# --- ERP Sources ---

@dlt.table(
    name="erp_loc_a101_raw",
    comment="Raw location data loaded incrementally from ERP.",
    table_properties={"quality": "bronze"}
)
def erp_loc_a101_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{BASE_PATH}/erp/_checkpoints/loc_a101_raw")
        .option("pathGlobFilter", "*loc_a101.csv")
        .load(f"{BASE_PATH}/erp/")
    )

@dlt.table(
    name="erp_cust_az12_raw",
    comment="Raw ERP customer data loaded incrementally from ERP.",
    table_properties={"quality": "bronze"}
)
def erp_cust_az12_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{BASE_PATH}/erp/_checkpoints/cust_az12_raw")
        .option("pathGlobFilter", "*cust_az12.csv")
        .load(f"{BASE_PATH}/erp/")
    )

@dlt.table(
    name="erp_px_cat_g1v2_raw",
    comment="Raw ERP product category data loaded incrementally from ERP.",
    table_properties={"quality": "bronze"}
)
def erp_px_cat_g1v2_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{BASE_PATH}/erp/_checkpoints/px_cat_g1v2_raw")
        .option("pathGlobFilter", "*px_cat_g1v2.csv")
        .load(f"{BASE_PATH}/erp/")
    )
