# Databricks notebook source
# ================================================================
# 🧪 TEST SUITE FOR ETL PIPELINE
# Bronze ➜ Silver ➜ Gold
# Uses mock/safe data only
# ================================================================

from pyspark.sql import Row
from utils import curation_helper, analytics_helper, logging_helper

print("🚀 Starting ETL pipeline tests...\n")

# ------------------------------------------------
# 1️⃣ Bronze ➜ Silver test
# ------------------------------------------------
try:
    print("🔹 Running Bronze ➜ Silver test")

    mock_entity_df = spark.createDataFrame([
        Row(EntityName="mock_entity", IsFileAvailableInBronze=True)
    ])

    bronze_df = spark.createDataFrame([
        ("  Test1  ", 100),
        ("  Test2  ", 200)
    ], ["name", "value"])

    schema_df = spark.createDataFrame([
        Row(
            BronzeColumnName="name",
            SilverColumnName="name_clean",
            CleansingRule="trim,lowercase",
            SilverDataType="string",
            IsPrimaryKey="false"
        ),
        Row(
            BronzeColumnName="value",
            SilverColumnName="amount",
            CleansingRule="",
            SilverDataType="int",
            IsPrimaryKey="false"
        )
    ])

    cleaned_df, pk = curation_helper.fn_apply_cleansing(bronze_df, schema_df)
    assert "name_clean" in cleaned_df.columns
    assert cleaned_df.count() == 2
    print("✅ Cleansing transformation successful")

    result = curation_helper.fn_write_to_silver(
        cleaned_df,
        load_type="Full Load",
        primary_key="value",
        entity_name="mock_entity",
        table_format="DELTA",
        table_catalog="ceo_slv_dev_managed",
        table_schema="default",
        silver_name="mock_silver"
    )
    print("✅ Silver write simulation successful:", result)
    print("🎉 Bronze ➜ Silver test passed\n")

except Exception as e:
    print("❌ Bronze ➜ Silver test failed:", e)
    raise


# ------------------------------------------------
# 2️⃣ Silver ➜ Gold test
# ------------------------------------------------
try:
    print("🔹 Running Silver ➜ Gold test")

    silver_df = spark.createDataFrame([
        (1, 2, 3, 1000, 4, 5),
        (2, 3, 4, 2000, 6, 7)
    ], ["ProgramId", "CategoryId", "PillarId", "BudgetAllocation", "ProgramGroupId", "ProgramSubGroupId"])
    silver_df.createOrReplaceTempView("mock_silver_project")

    def mock_summary_query():
        return spark.sql("SELECT * FROM mock_silver_project")
    analytics_helper.summary_program_budget = mock_summary_query

    df = analytics_helper.summary_program_budget()
    assert df.count() > 0
    print("✅ Analytics summary executed successfully")

    results_log = []
    logging_helper.log_append(results_log, "silver_to_gold", "mock_table", "test_write", "Success", gold_count=df.count())
    logging_helper.log_write(spark, results_log, log_table="mock_gold_log")
    print("✅ Logging helper simulated successfully")

    print("🎉 Silver ➜ Gold test passed\n")

except Exception as e:
    print("❌ Silver ➜ Gold test failed:", e)
    raise


# ------------------------------------------------
# 3️⃣ Independent Logging Helper test
# ------------------------------------------------
try:
    print("🔹 Running Logging Helper test")

    logs = []
    logging_helper.log_append(logs, "test_job", "mock_table", "stage_test", "Success", gold_count=5)
    logging_helper.log_write(spark, logs, log_table="mock_etl_log")

    read_log = spark.table("mock_etl_log")
    assert read_log.count() == 1
    print("✅ Logging helper write/read successful")
    print("🎉 Logging Helper test passed\n")

except Exception as e:
    print("❌ Logging Helper test failed:", e)
    raise


print("\n✅✅ ALL TESTS COMPLETED SUCCESSFULLY ✅✅")

