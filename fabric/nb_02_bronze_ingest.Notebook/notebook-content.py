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

# MARKDOWN ********************

# # Bronze: BTS flight ingest
# 
# Reads a monthly CSV from `Files/landing/csv/` into `bronze_flights` with an
# explicit schema. No cleaning, no type coercion beyond the declared schema —
# bronze preserves the source shape and column names as the source contract.
# 
# ## Schema strategy
# 
# **Explicit, not inferred.** Inference reads the file twice and produces
# inconsistent types between months: a column that is entirely null in one month
# infers as `string` and as `int` in another, and the append then fails on a
# schema mismatch partway through a backfill.
# 
# The schema is generated once from a reference month, hardened, and cached to
# `Files/landing/reference/bronze_schema.json`. Later runs read the cached file
# rather than re-inferring — inference makes two full passes over a 260 MB CSV,
# which is wasted work for a schema that does not change.
# 
# Hardening rules:
# - numeric measures forced to `double`, so a month where a column is entirely
#   null cannot break the append
# - `Div*` numerics likewise — these are the emptiest columns in the file and
#   where inference is least trustworthy
# 
# ## The phantom column
# 
# BTS ends every row with a trailing comma, producing a 110th unnamed field.
# It is declared as `_phantom` and dropped by name. Reading with a 109-field
# schema also works, but Spark then flags every row as containing discarded
# data — a false "corrupted records" warning on 600k rows, which is alarming,
# wrong, and trains you to ignore warnings. Declaring it is also faster:
# 13 seconds against 24.
# 
# ## Idempotency
# 
# Rows for the period are deleted before appending, so a retry or reprocess
# replaces the period rather than duplicating it. Bronze has no business key to
# merge on — it preserves the source shape — so `_source_file` is the natural
# unit of reload, which is what that audit column is for.


# PARAMETERS CELL ********************

# Default values, overridden by the pipeline at runtime
process_year  = 2020
process_month = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, os

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, DateType)

SPARK_LANDING = "Files/landing/csv"
BRONZE_TABLE  = "bronze_flights"

SCHEMA_PATH      = "/lakehouse/default/Files/landing/reference/bronze_schema.json"
SCHEMA_REFERENCE = "2020_01.csv"

# Numeric measures that must be double: null in some months, numeric in others.
OVERRIDE_DOUBLE = {"CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
                   "LateAircraftDelay", "DepDelay", "DepDelayMinutes", "DepDel15",
                   "ArrDelay", "ArrDelayMinutes", "ArrDel15", "TaxiOut", "TaxiIn",
                   "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance",
                   "Cancelled", "Diverted", "DivAirportLandings"}

SPARK_TYPES = {"int": IntegerType(), "double": DoubleType(),
               "date": DateType(), "string": StringType()}


def generate_bronze_schema():
    """
    Infer from the reference month, then harden. Called once; the result is
    cached to SCHEMA_PATH and read from there on subsequent runs.
    """
    sample = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{SPARK_LANDING}/{SCHEMA_REFERENCE}"))

    fields = []
    for name, dtype in sample.dtypes:
        if name == "_c109":          # the phantom, redeclared explicitly below
            continue
        if name in OVERRIDE_DOUBLE or (name.startswith("Div") and dtype in ("int", "double")):
            t = DoubleType()
        else:
            t = SPARK_TYPES.get(dtype, StringType())
        fields.append(StructField(name, t, True))

    fields.append(StructField("_phantom", StringType(), True))
    return StructType(fields)


def load_bronze_schema():
    """Read the cached schema, generating and caching it on first run."""
    if os.path.exists(SCHEMA_PATH):
        with open(SCHEMA_PATH) as f:
            return StructType.fromJson(json.load(f)), "cached"

    schema = generate_bronze_schema()
    os.makedirs(os.path.dirname(SCHEMA_PATH), exist_ok=True)
    with open(SCHEMA_PATH, "w") as f:
        json.dump(schema.jsonValue(), f, indent=2)
    return schema, "generated and cached"


BRONZE_SCHEMA, schema_source = load_bronze_schema()

print(f"schema: {len(BRONZE_SCHEMA.fields)} fields ({schema_source})")
print("        109 source + 1 phantom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_bronze(year, month):
    """
    Ingest one period into bronze.

    Deletes existing rows for this source file before appending, so a retry
    or reprocess replaces the period rather than duplicating it.
    """
    src  = f"{year}_{month:02d}.csv"
    path = f"{SPARK_LANDING}/{src}"

    df = (spark.read
          .option("header", "true")
          .schema(BRONZE_SCHEMA)
          .csv(path)
          .drop("_phantom")
          .withColumn("_ingest_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.lit(src)))

    rows = df.count()

    replaced = 0
    if spark.catalog.tableExists(BRONZE_TABLE):
        replaced = spark.table(BRONZE_TABLE).filter(F.col("_source_file") == src).count()
        if replaced:
            spark.sql(f"DELETE FROM {BRONZE_TABLE} WHERE _source_file = '{src}'")

    df.write.mode("append").format("delta").saveAsTable(BRONZE_TABLE)

    return {"period": f"{year}-{month:02d}", "rows": rows, "replaced": replaced}


result = load_bronze(process_year, process_month)
print(result)

if result["rows"] == 0:
    raise ValueError(f"no rows read from {process_year}_{process_month:02d}.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Scoped to the period just loaded. A full-table scan here would read the
# entire 41M-row bronze table on every monthly run to print numbers nobody acts on.
src = f"{process_year}_{process_month:02d}.csv"

(spark.table(BRONZE_TABLE)
  .filter(F.col("_source_file") == src)
  .agg(F.count("*").alias("rows"),
       F.min("FlightDate").alias("first_date"),
       F.max("FlightDate").alias("last_date"),
       F.countDistinct("FlightDate").alias("distinct_dates"),
       F.max("_ingest_timestamp").alias("loaded_at"))
  .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
