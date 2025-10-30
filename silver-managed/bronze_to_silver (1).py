# Databricks notebook source
# dbutils.library.restartPython()
# import importlib
# importlib.reload(logging_helper)

# COMMAND ----------

# DBTITLE 1,Import libraries
from utils import logging_helper, curation_helper

# COMMAND ----------

# DBTITLE 1,Establish data lake connection
# fn_adls_connector(kv_secret_scope, storage_account, application_id, service_principal, tenant_id)
curation_helper.fn_adls_connector()

# COMMAND ----------

# DBTITLE 1,Get table metadata for ingestion
# this command is to create the widget/parameter required for this notebook
dbutils.widgets.text("p_entity_name","","")
dbutils.widgets.dropdown("p_is_all_entity", "no", ["yes", "no"], "All tables?")

# initialize variables from the widgets
entity_name = dbutils.widgets.get("p_entity_name")
is_all_entity = dbutils.widgets.get("p_is_all_entity")

table_metadata = curation_helper.fn_get_entity_list(entity_name,is_all_entity)

# COMMAND ----------

# DBTITLE 1,Perform data ingestion

rows = table_metadata.collect()
print(f"Starting data processing")
for row in rows:
    results_log = []
    stage = "bronze_to_silver"
    try:
        status = "Success"
        table_name = row["EntityName"]
        bronze_name = row["BronzeTableName"]
        bronze_path = row["BronzePath"]
        load_type = row["LoadType"]
        bronze_format = row["BronzeFormat"]
        bronze_catalog = row["BronzeCatalog"]
        bronze_schema = row["BronzeSchema"]
        silver_name = row["SilverTableName"]
        silver_format = row["SilverFormat"]
        silver_catalog = row["SilverCatalog"]
        silver_schema = row["SilverSchema"]

        print(f"Processing entity: {table_name}")

        curation_helper.fn_register_uc_table (
            table_format=bronze_format, 
            table_catalog=bronze_catalog, 
            table_schema=bronze_schema, 
            table_name=bronze_name, 
            table_path=bronze_path)


        # Read all the files in all subdirectories under the bronze path
        bronze_df = curation_helper.fn_read_bronze(bronze_path)

        bronze_count = bronze_df.count()

        # Get schema of the table to be ingested
        schema_df = curation_helper.fn_get_entity_schema(table_name)
        
        # Perform data transformations
        curated_df, primary_key = curation_helper.fn_apply_cleansing(bronze_df,schema_df)
        silver_count = curated_df.count()

        # Perform UPSERT
        operation = curation_helper.fn_write_to_silver(
            cleaned_bronze_df=curated_df,
            load_type=load_type,
            primary_key=primary_key,
            entity_name=table_name,
            table_format = silver_format,
            table_catalog = silver_catalog,
            table_schema = silver_schema,
            silver_name=silver_name)
        
        # Update entity table
        curation_helper.fn_update_entity_metadata(table_name)
        
        logging_helper.log_append(results_log,stage,table_name,operation,status,bronze_count,silver_count)
    except Exception as e:
        status = "Failed"
        operation = "Exception Error"
        error_message = str(e)
        logging_helper.log_append(results_log,stage,table_name,operation,status,error_message)
        
        print(f"Error processing entity: {table_name}: {str(e)}")
        raise
    finally:
        logging_helper.log_write(spark, results_log)
