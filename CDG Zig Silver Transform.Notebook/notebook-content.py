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
from pyspark.sql.window import Window

spark.conf.set("spark.sql.parquet.vorder.default", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def latest(dataframe, key):
    window = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc())
    return dataframe.withColumn("_row_number", F.row_number().over(window)).filter("_row_number = 1").drop("_row_number")

vehicles = (latest(spark.table("bronze.vehicles_raw"), "vin")
    .withColumn("model_year", F.col("model_year").cast("int"))
    .withColumn("seating_capacity", F.col("seating_capacity").cast("int"))
    .withColumn("accessibility_enabled", F.col("accessibility_enabled").cast("boolean"))
    .filter(F.col("vin").isNotNull() & F.col("model_year").between(2000, 2030) & (F.col("seating_capacity") > 0)))
customers = (latest(spark.table("bronze.customers_raw"), "customer_id")
    .withColumn("member_since", F.to_date("member_since"))
    .withColumn("marketing_consent", F.col("marketing_consent").cast("boolean"))
    .filter(F.col("customer_id").isNotNull() & F.col("member_since").isNotNull()))
locations = (latest(spark.table("bronze.locations_raw"), "location_id")
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .filter(F.col("location_id").isNotNull() & F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)))
activity_types = (latest(spark.table("bronze.activity_types_raw"), "activity_type")
    .withColumn("lifecycle_sequence", F.col("lifecycle_sequence").cast("int"))
    .filter(F.col("activity_type").isNotNull() & (F.col("lifecycle_sequence") > 0)))
downloads = (latest(spark.table("bronze.app_downloads_raw"), "download_id")
    .withColumn("download_timestamp_utc", F.to_timestamp("download_date_utc"))
    .filter(F.col("download_id").isNotNull() & F.col("download_timestamp_utc").isNotNull())
    .join(customers.select("customer_id"), "customer_id", "left_semi"))

activity_candidates = (latest(spark.table("bronze.trip_activities_raw"), "event_id")
    .withColumn("event_timestamp_utc", F.to_timestamp("event_timestamp_utc"))
    .withColumn("event_timestamp_sgt", F.to_timestamp("event_timestamp_sgt"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("event_date", F.to_date("event_timestamp_sgt")))
valid_activities = (activity_candidates
    .filter(F.col("event_id").isNotNull() & F.col("trip_id").isNotNull() & F.col("event_timestamp_utc").isNotNull())
    .filter(F.col("activity_type").isin("Pickup", "Dropoff"))
    .filter(F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
    .join(vehicles.select("vin"), "vin", "left_semi")
    .join(customers.select("customer_id"), "customer_id", "left_semi")
    .join(locations.select("location_id"), "location_id", "left_semi"))
activity_rejections = (activity_candidates.join(valid_activities.select("event_id"), "event_id", "left_anti")
    .withColumn("rejected_at", F.current_timestamp())
    .withColumn("rejection_reason", F.lit("Invalid key, type, range, activity type, or dimension reference")))

vehicles.write.format("delta").mode("overwrite").saveAsTable("silver.vehicles")
customers.write.format("delta").mode("overwrite").saveAsTable("silver.customers")
locations.write.format("delta").mode("overwrite").saveAsTable("silver.locations")
activity_types.write.format("delta").mode("overwrite").saveAsTable("silver.activity_types")
downloads.write.format("delta").mode("overwrite").saveAsTable("silver.app_downloads")
valid_activities.write.format("delta").mode("overwrite").partitionBy("event_date").saveAsTable("silver.trip_activities")
activity_rejections.write.format("delta").mode("overwrite").saveAsTable("silver.trip_activity_rejections")

spark.sql("OPTIMIZE silver.trip_activities ZORDER BY (trip_id, vin, customer_id)")
counts = {"trip_activities": valid_activities.count(), "vehicles": vehicles.count(), "customers": customers.count(), "app_downloads": downloads.count(), "locations": locations.count(), "activity_types": activity_types.count(), "rejections": activity_rejections.count()}
assert counts == {"trip_activities": 12000, "vehicles": 300, "customers": 2200, "app_downloads": 2200, "locations": 12, "activity_types": 10, "rejections": 0}, counts
assert valid_activities.groupBy("event_id").count().filter("count > 1").limit(1).count() == 0
notebookutils.notebook.exit(str(counts))
