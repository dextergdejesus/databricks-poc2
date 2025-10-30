# Import libraries
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType,TimestampType, LongType
)
from datetime import datetime

# Define the schema for your log entries
log_schema = StructType([
    StructField("LogDate", DateType(), True),
    StructField("ExecutionDate", TimestampType(), True),
    StructField("Stage", StringType(), True),
    StructField("EntityName", StringType(), True),
    StructField("Operation", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("BronzeCount", LongType(), True),
    StructField("SilverCount", LongType(), True),
    StructField("GoldCount", LongType(), True),
    StructField("ErrorMessage", StringType(), True)
])

# COMMAND ----------

def log_append(results_log,stage,table_name,operation,status,
                                bronze_count=None,silver_count=None,gold_count=None,error_message=None):
    # 
    # Appends a log entry to the results_log list.
    # 
    results_log.append({
        "LogDate": datetime.now().date(),
        "ExecutionDate": datetime.now(),
        "Stage": stage,
        "EntityName": table_name,
        "Operation": operation,
        "Status": status,
        "BronzeCount": bronze_count,
        "SilverCount": silver_count,
        "GoldCount": gold_count,
        "ErrorMessage": error_message
                })



def log_write(spark, results_log, log_table="ceo_brz_dev.dbo_managed.entity_log"):
    # 
    # Writes a log entry to the table
    # 
    if results_log:
        results_df = spark.createDataFrame(results_log,schema=log_schema)
        results_df.write.format("delta").mode("append").saveAsTable(log_table)