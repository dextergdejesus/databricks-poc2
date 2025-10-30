# Databricks notebook source
# DBTITLE 1,Import libraries and functions
# import logging
from utils import logging_helper, analytics_helper, curation_helper

# COMMAND ----------

# DBTITLE 1,Reload utils
import importlib
importlib.reload(curation_helper)
importlib.reload(analytics_helper)
importlib.reload(logging_helper)

# COMMAND ----------

analytics_functions = {
    "dim_pillar_categories": analytics_helper.dim_pillar_categories,
    "fact_budget_spending_by_program": analytics_helper.fact_budget_spending_by_program,
    "dim_district_census_tract": analytics_helper.dim_district_census_tract,
    "fact_budget_spending_by_geography": analytics_helper.fact_budget_spending_by_geography
}

for fact_table, func in analytics_functions.items():
    results_log = []
    stage = "silver_to_gold"
    try:
        df = func()
        curation_helper.fn_write_delta_analytics(
                table_catalog="ceo_slv_dev_managed",
                table_schema="insights",
                table_name=fact_table,
                df=df)
        gold_count = df.count()
        print(f"Completed processing table: {fact_table}, Record count: {gold_count}")
        operation = "Data Refresh"
        status = 'Success'
        logging_helper.log_append(
                results_log=results_log,
                stage=stage,
                table_name=fact_table,
                operation=operation,
                status=status,
                gold_count=gold_count)
    except Exception as e:
        status = "Failed"
        operation = "Exception Error"
        error_message = str(e)
        logging_helper.log_append(
                results_log=results_log,
                stage=stage,
                table_name=fact_table,
                operation=operation,
                status=status,
                error_message=error_message)
        print(f"Error processing fact table: {fact_table}: {str(e)}")
        raise
    finally:
        logging_helper.log_write(spark, results_log)
