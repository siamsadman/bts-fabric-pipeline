# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9f302880-d9e8-42a8-9bc2-de27b5220614",
# META       "default_lakehouse_name": "lh_bts",
# META       "default_lakehouse_workspace_id": "befb4d14-5716-492d-adaf-21128aebf048",
# META       "known_lakehouses": [
# META         {
# META           "id": "9f302880-d9e8-42a8-9bc2-de27b5220614"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import os

base = "/lakehouse/default/Files"
print("mount exists:", os.path.exists(base))
print("contents    :", os.listdir(base) if os.path.exists(base) else "n/a")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LANDING = "/lakehouse/default/Files/landing/csv"
SPARK_LANDING = "Files/landing/csv"

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(f"{SPARK_LANDING}/2020_01.csv"))

print("columns:", len(df.columns))
print("rows   :", df.count())

for name, dtype in df.dtypes[:15]:
    print(f"  {name:<40} {dtype}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

suspect = ["DepTime", "ArrTime", "CRSDepTime", "CRSArrTime",
           "WheelsOff", "WheelsOn", "CancellationCode",
           "CarrierDelay", "WeatherDelay", "NASDelay",
           "SecurityDelay", "LateAircraftDelay",
           "Div1Airport", "Div1TailNum", "DivAirportLandings"]

types = dict(df.dtypes)
for c in suspect:
    print(f"  {c:<24} {types.get(c, 'NOT FOUND')}")

print()
print("last 3 columns:", df.dtypes[-3:])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, DateType)

inferred = [(n, t) for n, t in df.dtypes if n != "_c109"]

override_double = {"CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
                   "LateAircraftDelay", "DepDelay", "DepDelayMinutes", "DepDel15",
                   "ArrDelay", "ArrDelayMinutes", "ArrDel15", "TaxiOut", "TaxiIn",
                   "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance",
                   "Cancelled", "Diverted", "DivAirportLandings"}

spark_types = {"int": IntegerType(), "double": DoubleType(),
               "date": DateType(), "string": StringType()}

fields = []
for name, dtype in inferred:
    if name in override_double:
        t = DoubleType()
    elif name.startswith("Div") and dtype in ("int", "double"):
        t = DoubleType()
    else:
        t = spark_types.get(dtype, StringType())
    fields.append(StructField(name, t, True))

bronze_schema = StructType(fields)

print("fields:", len(bronze_schema.fields))
print()
for f in bronze_schema.fields[:5]:
    print(f"  {f.name:<32} {f.dataType.simpleString()}")
print("  ...")
for f in bronze_schema.fields[-4:]:
    print(f"  {f.name:<32} {f.dataType.simpleString()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test = (spark.read
        .option("header", "true")
        .schema(bronze_schema)
        .csv(f"{SPARK_LANDING}/2020_04.csv"))

print("rows:", test.count())

from pyspark.sql import functions as F

check = test.select(
    F.count("*").alias("total"),
    F.sum(F.col("Cancelled")).alias("cancelled"),
    F.sum(F.when(F.col("ArrTime").isNull(), 1).otherwise(0)).alias("null_arrtime"),
    F.sum(F.when(F.col("CarrierDelay").isNotNull(), 1).otherwise(0)).alias("has_carrierdelay"),
)
check.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

def load_bronze(year, month, mode="append"):
    path = f"{SPARK_LANDING}/{year}_{month:02d}.csv"

    df = (spark.read
          .option("header", "true")
          .schema(bronze_schema)
          .csv(path)
          .withColumn("_ingest_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.lit(f"{year}_{month:02d}.csv")))

    (df.write
       .mode(mode)
       .format("delta")
       .saveAsTable("bronze_flights"))

    return {"period": f"{year}-{month:02d}", "rows": df.count()}

print(load_bronze(2020, 1, mode="overwrite"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

b = spark.table("bronze_flights")

b.select("FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
         "Origin", "Dest", "DepTime", "ArrTime", "Cancelled",
         "_source_file").show(5, truncate=False)

from pyspark.sql import functions as F
b.select(
    F.count("*").alias("rows"),
    F.countDistinct("FlightDate").alias("distinct_dates"),
    F.sum(F.when(F.col("Origin").isNull(), 1).otherwise(0)).alias("null_origin"),
    F.sum(F.when(F.col("DepTime").isNull(), 1).otherwise(0)).alias("null_deptime"),
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType

# declare the phantom trailing column, then drop it
bronze_schema_full = StructType(
    bronze_schema.fields + [StructField("_phantom", StringType(), True)]
)

def load_bronze(year, month, mode="append"):
    path = f"{SPARK_LANDING}/{year}_{month:02d}.csv"

    df = (spark.read
          .option("header", "true")
          .schema(bronze_schema_full)
          .csv(path)
          .drop("_phantom")
          .withColumn("_ingest_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.lit(f"{year}_{month:02d}.csv")))

    (df.write
       .mode(mode)
       .format("delta")
       .saveAsTable("bronze_flights"))

    return {"period": f"{year}-{month:02d}", "rows": df.count()}

print(load_bronze(2020, 1, mode="overwrite"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for y, m in [(2020, 4), (2023, 6)]:
    print(load_bronze(y, m))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

b = spark.table("bronze_flights")

(b.groupBy("_source_file")
  .agg(F.count("*").alias("rows"),
       F.min("FlightDate").alias("first_date"),
       F.max("FlightDate").alias("last_date"),
       F.max("_ingest_timestamp").alias("loaded_at"))
  .orderBy("_source_file")
  .show(truncate=False))

print("total rows:", b.count())
print("columns   :", len(b.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
