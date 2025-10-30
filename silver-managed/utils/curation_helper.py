# Import libraries and functions
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# import logging
import logging

# Set up logging config
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_to_silver_logger")
from datetime import datetime

from pyspark.sql.functions import (
    trim,
    lower,
    upper,
    date_format,
    col
)


# Initialize log
results_log = []

def get_spark():
    return SparkSession.builder.getOrCreate()

# For authentication
def fn_adls_connector():
  try:
      spark = get_spark()
      spark.conf.set("fs.azure.account.key.storagec.dfs.core.windows.net",
                     "====")
      print ('Connection to datalake established')

  except Exception as ex:
      print ('Connection to datalake failed')
      print (ex)

# For getting the list of tables in entity
def fn_get_entity_list(entity_name: str, is_all_entity: str):
    spark = get_spark()

    entity_name = entity_name or "" 
    is_all_entity = str(is_all_entity).strip().lower() == "yes"
    if not is_all_entity and not entity_name.strip():
        print("Error: Missing parameter: Entity name cannot be empty when is_all_entity is not 'yes'")
        results_log.append({
                    "ExecutionDate": datetime.now(),
                    "LogDate": datetime.now().date(),
                    "Stage": "bronze_to_silver",
                    "EntityName": "",
                    "Operation": "Parameter Validation",
                    "Status": "Failed",
                    "ErrorMessage": "Missing Parameter"
                })
        results_df = spark.createDataFrame(results_log)
        results_df.write.format("delta").mode("append").saveAsTable("ceo_brz_dev.dbo_managed.entity_log")
        raise ValueError("Entity name cannot be empty when is_all_entity is not 'yes'")
    
    entity_list = [f"'{name.strip().lower().strip('"')}'" for name in entity_name.split(',')]

    entity_list_str = ','.join(entity_list)

    if is_all_entity:
        where_clause = "WHERE IsFileAvailableInBronze = TRUE"
    else:
        where_clause = f"WHERE IsFileAvailableInBronze = TRUE AND LOWER(e.EntityName) IN ({entity_list_str})"

    entity_df = spark.sql(f"""
                          SELECT * FROM `ceo_brz_dev`.`dbo_managed`.`entity` e
                          {where_clause}
                          """)
    return entity_df


def fn_register_uc_table (table_format, table_catalog, table_schema, table_name, table_path):
    spark = get_spark()
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_catalog}.{table_schema}.{table_name}
        USING {table_format}
        LOCATION '{table_path}'
    """)

def fn_write_delta_analytics(table_catalog, table_schema, table_name,df):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{table_catalog}.{table_schema}.{table_name}")

# For applying data cleansing

def fn_apply_cleansing (df, schema_df):
    schema = schema_df.collect()
    primary_key = next((row["SilverColumnName"] for row in schema if str(row["IsPrimaryKey"]).lower()=="true"), None)
    for row in schema:
        source_col = row["BronzeColumnName"]
        target_col = row["SilverColumnName"]
        cleansing_type = row["CleansingRule"]
        data_type = row["SilverDataType"]

        expr = col(source_col)  
    
        # if multiple cleansing rules per column
        if cleansing_type:
            rules = [r.strip().lower() for r in cleansing_type.split(",")]

            for rule in rules:
                if 'trim' in rule:
                    expr = trim(expr)
                elif 'lowercase' in rule:
                    expr = lower(expr)
                elif 'uppercase' in rule:
                    expr = upper(expr)
                elif 'to_date' in rule:
                    expr = date_format(expr, 'yyyy-MM-dd')
                elif 'to_timestamp' in rule:
                    expr = date_format(expr, 'yyyy-MM-dd HH:mm:ss')

        # cast to target data type and set silver column name       
        expr = expr.cast(data_type).alias(target_col)
        df = df.withColumn(target_col, expr)

        # de-duplicate the data
        if primary_key:
            df = df.dropDuplicates([primary_key])

    target_cols = [row["SilverColumnName"] for row in schema]
    df = df.select(*target_cols)
    return df,primary_key

def fn_get_entity_schema(table_name):
    spark = get_spark()
    return spark.sql(f"""
        SELECT      s.* 
        FROM        `ceo_brz_dev`.`dbo_managed`.`entity_schema` s 
        INNER JOIN  `ceo_brz_dev`.`dbo_managed`.`entity` e ON
                    e.EntityId = s.EntityId 
        WHERE e.EntityName = '{table_name}'
        """)

def fn_read_bronze(files_path):
    spark = get_spark()
    return spark.read.option("recursiveFileLookup", "true").parquet(files_path)


def fn_write_to_silver(cleaned_bronze_df,load_type,primary_key,entity_name,table_format,table_catalog,table_schema,silver_name):
    spark = get_spark()
    if spark.catalog.tableExists(f"{table_catalog}.{table_schema}.{silver_name}"):
        if load_type == "Incremental":
            silver_table = DeltaTable.forName(spark, f"{table_catalog}.{table_schema}.{silver_name}")
            merge_condition = f"target.{primary_key} = source.{primary_key}"
            (
                silver_table.alias("target")
                .merge(
                    cleaned_bronze_df.alias("source"),
                    merge_condition
                    )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
                )
            print(f"Merging/Incremental delta: {entity_name}")
            return "Incremental Merge"
        else:
            cleaned_bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{table_catalog}.{table_schema}.{silver_name}")
            print(f"Full Load delta: {entity_name}")
            return "Full Load Overwrite"
            
    else:
        cleaned_bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{table_catalog}.{table_schema}.{silver_name}")
        print(f"Creating delta: {entity_name}")
        return "Delta Table Created"

def fn_update_entity_metadata(entity_name):
    spark = get_spark()
    spark.sql(f"""
        UPDATE ceo_brz_dev.dbo_managed.entity
        SET LastExtractionDate = current_timestamp(),
            LoadType = 'Incremental'
        WHERE EntityName = '{entity_name}'
        """)
