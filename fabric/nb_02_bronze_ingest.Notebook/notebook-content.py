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
# The schema is generated once from a reference month, then hardened — all
# numeric measures forced to `double` so an all-null month cannot break the
# append.
# 
# ## The phantom column
# 
# BTS ends every row with a trailing comma, producing a 110th unnamed field.
# It is declared as `_phantom` and dropped by name. Reading with a 109-field
# schema also works, but Spark then flags every row as containing discarded
# data — a false "corrupted records" warning on 600k rows.
# 
# ## Idempotency
# 
# Rows for the period are deleted before appending, so a retry replaces the
# period rather than duplicating it. Bronze has no business key to merge on;
# `_source_file` is the natural unit of reload.


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

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, DateType)

SPARK_LANDING = "Files/landing/csv"
BRONZE_TABLE  = "bronze_flights"
SCHEMA_REFERENCE = "2020_01.csv"   # month the schema was generated from

# Numeric measures that must be double: null in some months, numeric in others.
OVERRIDE_DOUBLE = {"CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
                   "LateAircraftDelay", "DepDelay", "DepDelayMinutes", "DepDel15",
                   "ArrDelay", "ArrDelayMinutes", "ArrDel15", "TaxiOut", "TaxiIn",
                   "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance",
                   "Cancelled", "Diverted", "DivAirportLandings"}

SPARK_TYPES = {"int": IntegerType(), "double": DoubleType(),
               "date": DateType(), "string": StringType()}


def build_bronze_schema():
    """
    Generate the schema from a reference month, then harden it.
    Div* numerics are also forced to double - they are the emptiest columns
    and where inference is least trustworthy.
    The phantom trailing column is appended so Spark expects 110 fields.
    """
    sample = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{SPARK_LANDING}/{SCHEMA_REFERENCE}"))

    fields = []
    for name, dtype in sample.dtypes:
        if name == "_c109":
            continue
        if name in OVERRIDE_DOUBLE or (name.startswith("Div") and dtype in ("int", "double")):
            t = DoubleType()
        else:
            t = SPARK_TYPES.get(dtype, StringType())
        fields.append(StructField(name, t, True))

    fields.append(StructField("_phantom", StringType(), True))
    return StructType(fields)


bronze_schema = build_bronze_schema()
print("schema fields:", len(bronze_schema.fields), "(109 source + 1 phantom)")

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
          .schema(bronze_schema)
          .csv(path)
          .drop("_phantom")
          .withColumn("_ingest_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.lit(src)))

    replaced = 0
    if spark.catalog.tableExists(BRONZE_TABLE):
        replaced = spark.table(BRONZE_TABLE).filter(F.col("_source_file") == src).count()
        if replaced:
            spark.sql(f"DELETE FROM {BRONZE_TABLE} WHERE _source_file = '{src}'")

    df.write.mode("append").format("delta").saveAsTable(BRONZE_TABLE)

    return {"period": f"{year}-{month:02d}", "rows": df.count(), "replaced": replaced}


print(load_bronze(process_year, process_month))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

b = spark.table(BRONZE_TABLE)

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
