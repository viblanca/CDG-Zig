# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b39bc734-1a7e-4fb9-a51b-42bf47116ccc",
# META       "default_lakehouse_name": "CdgZigMedallionLakehouse",
# META       "default_lakehouse_workspace_id": "58d7b1b1-59f5-42d7-b8f0-1aa545be8306"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

source_path = "Files/landing/cdg-zig"
batch_id = "cdg-zig-initial"
spark.conf.set("spark.sql.parquet.vorder.default", "false")
for schema_name in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

columns = {
    "trip_activities": ["event_id", "trip_id", "event_timestamp_utc", "event_timestamp_sgt", "vin", "customer_id", "activity_type", "location_id", "latitude", "longitude"],
    "vehicles": ["vin", "fleet_vehicle_id", "vehicle_make", "vehicle_model", "model_year", "vehicle_class", "powertrain", "seating_capacity", "accessibility_enabled", "fleet_status"],
    "customers": ["customer_id", "customer_segment", "loyalty_tier", "member_since", "preferred_booking_channel", "home_region", "marketing_consent"],
    "app_downloads": ["download_id", "customer_id", "device_id", "synthetic_phone_number", "download_date_utc", "platform", "app_version", "acquisition_channel"],
    "locations": ["location_id", "location_name", "location_type", "planning_area", "region", "country_code", "latitude", "longitude"],
    "activity_types": ["activity_type", "activity_group", "lifecycle_sequence", "description"],
}
expected_counts = {"trip_activities": 12000, "vehicles": 300, "customers": 2200, "app_downloads": 2200, "locations": 12, "activity_types": 10}

for dataset_name, column_names in columns.items():
    schema = StructType([StructField(name, StringType(), True) for name in column_names])
    source_file = f"{source_path}/cdg_zig_{dataset_name}.csv"
    dataframe = (spark.read.format("csv").option("header", "true").option("mode", "FAILFAST").schema(schema).load(source_file)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id)))
    actual_count = dataframe.count()
    if actual_count != expected_counts[dataset_name]:
        raise ValueError(f"{dataset_name}: expected {expected_counts[dataset_name]} rows, found {actual_count}")
    (dataframe.write.format("delta").mode("overwrite").partitionBy("_ingestion_date").saveAsTable(f"bronze.{dataset_name}_raw"))

bronze_counts = {name: spark.table(f"bronze.{name}_raw").count() for name in columns}
assert bronze_counts == expected_counts, bronze_counts
notebookutils.notebook.exit(str(bronze_counts))
