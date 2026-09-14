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

spark.conf.set("spark.sql.parquet.vorder.default", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", "1g")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

activities = spark.table("silver.trip_activities")
locations = spark.table("silver.locations")
pickups = activities.filter(F.col("activity_type") == "Pickup").select("trip_id", "vin", "customer_id", F.col("event_timestamp_utc").alias("pickup_timestamp_utc"), F.col("event_timestamp_sgt").alias("pickup_timestamp_sgt"), F.col("location_id").alias("origin_location_id"), F.col("latitude").alias("origin_latitude"), F.col("longitude").alias("origin_longitude"))
dropoffs = activities.filter(F.col("activity_type") == "Dropoff").select("trip_id", F.col("event_timestamp_utc").alias("dropoff_timestamp_utc"), F.col("event_timestamp_sgt").alias("dropoff_timestamp_sgt"), F.col("location_id").alias("destination_location_id"), F.col("latitude").alias("destination_latitude"), F.col("longitude").alias("destination_longitude"))
origin = locations.select(F.col("location_id").alias("origin_location_id"), F.col("location_name").alias("origin_name"), F.col("planning_area").alias("origin_planning_area"), F.col("region").alias("origin_region"))
destination = locations.select(F.col("location_id").alias("destination_location_id"), F.col("location_name").alias("destination_name"), F.col("planning_area").alias("destination_planning_area"), F.col("region").alias("destination_region"))
fact_trips = (pickups.join(dropoffs, "trip_id", "inner").join(origin, "origin_location_id", "left").join(destination, "destination_location_id", "left")
    .withColumn("trip_date", F.to_date("pickup_timestamp_sgt"))
    .withColumn("singapore_hour", F.hour("pickup_timestamp_sgt"))
    .withColumn("duration_minutes", F.round((F.col("dropoff_timestamp_utc").cast("long") - F.col("pickup_timestamp_utc").cast("long")) / 60.0, 1))
    .withColumn("origin_destination_pair", F.concat_ws(" to ", "origin_planning_area", "destination_planning_area"))
    .withColumn("trip_status", F.lit("Completed")))

dim_vehicles = spark.table("silver.vehicles").withColumn("vehicle_make_model", F.concat_ws(" ", "vehicle_make", "vehicle_model"))
dim_customers = spark.table("silver.customers")
dim_locations = locations
dim_activity_types = spark.table("silver.activity_types")
fact_app_downloads = spark.table("silver.app_downloads").withColumn("download_date", F.to_date("download_timestamp_utc"))
date_bounds = fact_trips.select(F.min("trip_date").alias("min_date"), F.max("trip_date").alias("max_date")).first()
dim_date = (spark.sql(f"SELECT explode(sequence(to_date('{date_bounds.min_date}'), to_date('{date_bounds.max_date}'), interval 1 day)) AS date")
    .withColumn("year", F.year("date")).withColumn("quarter", F.concat(F.lit("Q"), F.quarter("date")))
    .withColumn("month_number", F.month("date")).withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("year_month", F.date_format("date", "yyyy-MM")).withColumn("weekday", F.date_format("date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date").isin([1, 7])))
daily_trip_summary = (fact_trips.groupBy("trip_date", "origin_planning_area", "destination_planning_area")
    .agg(F.countDistinct("trip_id").alias("trip_count"), F.countDistinct("customer_id").alias("active_customers"), F.countDistinct("vin").alias("active_vehicles"), F.round(F.avg("duration_minutes"), 1).alias("average_duration_minutes")))
hourly_demand = (fact_trips.groupBy("trip_date", "singapore_hour", "origin_planning_area")
    .agg(F.countDistinct("trip_id").alias("pickup_count"), F.countDistinct("customer_id").alias("active_customers")))

outputs = {"dim_date": dim_date, "dim_vehicles": dim_vehicles, "dim_customers": dim_customers, "dim_locations": dim_locations, "dim_activity_types": dim_activity_types}
for table_name, dataframe in outputs.items():
    dataframe.write.format("delta").mode("overwrite").saveAsTable(f"gold.{table_name}")
spark.sql("DROP TABLE IF EXISTS gold.fact_app_downloads")
fact_app_downloads.coalesce(1).write.format("delta").mode("overwrite").saveAsTable("gold.fact_app_downloads")
fact_trips.write.format("delta").mode("overwrite").partitionBy("trip_date").saveAsTable("gold.fact_trips")
daily_trip_summary.write.format("delta").mode("overwrite").partitionBy("trip_date").saveAsTable("gold.daily_trip_summary")
hourly_demand.write.format("delta").mode("overwrite").partitionBy("trip_date").saveAsTable("gold.hourly_demand")
spark.sql("OPTIMIZE gold.fact_app_downloads")
spark.sql("OPTIMIZE gold.fact_trips ZORDER BY (customer_id, vin, origin_location_id)")
spark.sql("OPTIMIZE gold.daily_trip_summary ZORDER BY (origin_planning_area, destination_planning_area)")

app_download_detail = spark.sql("DESCRIBE DETAIL gold.fact_app_downloads").first().asDict()
assert app_download_detail["numFiles"] <= 10, app_download_detail
counts = {"fact_trips": fact_trips.count(), "dim_vehicles": dim_vehicles.count(), "dim_customers": dim_customers.count(), "fact_app_downloads": fact_app_downloads.count(), "dim_locations": dim_locations.count()}
assert counts == {"fact_trips": 6000, "dim_vehicles": 300, "dim_customers": 2200, "fact_app_downloads": 2200, "dim_locations": 12}, counts
assert fact_trips.groupBy("trip_id").count().filter("count > 1").limit(1).count() == 0
assert fact_trips.filter(F.col("pickup_timestamp_utc") >= F.col("dropoff_timestamp_utc")).limit(1).count() == 0
assert fact_trips.filter(F.col("duration_minutes") <= 0).limit(1).count() == 0
notebookutils.notebook.exit(str({"counts": counts, "app_download_files": app_download_detail["numFiles"]}))
