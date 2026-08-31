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

# # Gold: Star Schema
# 
# Builds the dimensional model from `silver_flights`.
# 
# ## Tables
# 
# | Table | Grain | Load pattern |
# |---|---|---|
# | `dim_date` | One row per calendar day | Generated, full rebuild |
# | `dim_airport` | One row per DOT airport ID | Full rebuild from silver |
# | `dim_carrier` | One row per reporting carrier code | Full rebuild from silver |
# | `dim_cancellation_code` | One row per BTS cancellation reason | Static |
# | `dim_delay_cause` | One row per BTS delay cause | Static |
# | `fact_flight` | One row per scheduled flight leg | Incremental merge by period |
# | `fact_delay_attribution` | One row per flight per delay cause | Incremental merge by period |
# 
# ## Key strategy
# 
# Natural keys throughout — `airport_id` (DOT numeric), `carrier_code`, and an
# integer `date_key` of `yyyymmdd`. No generated surrogates.
# 
# `monotonically_increasing_id()` is not stable across reruns, and a proper
# key-assignment step adds pipeline machinery this project does not need. BTS
# already supplies stable identifiers.
# 
# Note: airports are keyed on `airport_id`, **not** the three-letter IATA code.
# BTS reuses IATA codes across different airports over time; `AirportID` is stable.
# 
# ## Grain
# 
# `fact_flight` retains cancelled and diverted flights. Filtering them out is the
# common mistake with this dataset — it destroys cancellation-rate analysis and
# biases every delay average, because the worst operational days are the ones with
# the most cancellations.
# 
# ## Load pattern
# 
# Dimensions are rebuilt in full on every run; they are small and a new airport or
# carrier can appear in any period. Facts are merged incrementally by period. The
# `process_year` / `process_month` parameters therefore apply to the facts only.


# PARAMETERS CELL ********************

# Default values, overridden by the pipeline at runtime
process_year  = 2023
process_month = 6

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_TABLE = "silver_flights"

source_file = f"{process_year}_{process_month:02d}.csv"

silver = spark.table(SILVER_TABLE)
period = silver.filter(F.col("_source_file") == source_file)

print("processing  :", f"{process_year}-{process_month:02d}")
print("silver total:", silver.count())
print("period rows :", period.count())

if period.count() == 0:
    raise ValueError(f"no silver rows for {source_file} - has it been transformed?")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_dim_date(start="2019-12-01", end="2027-12-31"):
    """
    Generated calendar dimension. Deliberately wider than the data so joins
    never fail on a boundary, and so late-arriving periods need no rebuild.
    """
    df = (spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
          .withColumn("date_key",      F.date_format("date", "yyyyMMdd").cast("int"))
          .withColumn("year",          F.year("date"))
          .withColumn("quarter",       F.quarter("date"))
          .withColumn("month",         F.month("date"))
          .withColumn("month_name",    F.date_format("date", "MMMM"))
          .withColumn("month_short",   F.date_format("date", "MMM"))
          .withColumn("day_of_month",  F.dayofmonth("date"))
          .withColumn("day_of_week",   F.dayofweek("date"))
          .withColumn("day_name",      F.date_format("date", "EEEE"))
          .withColumn("day_short",     F.date_format("date", "EEE"))
          .withColumn("is_weekend",    F.dayofweek("date").isin(1, 7))
          .withColumn("year_month",    F.date_format("date", "yyyy-MM"))
          .withColumn("year_month_key", (F.year("date") * 100 + F.month("date")).cast("int"))
    )
    return df.select("date_key", "date", "year", "quarter", "month", "month_name",
                     "month_short", "day_of_month", "day_of_week", "day_name",
                     "day_short", "is_weekend", "year_month", "year_month_key")


dim_date = build_dim_date()
dim_date.write.mode("overwrite").format("delta").saveAsTable("dim_date")

print("rows:", spark.table("dim_date").count())
spark.table("dim_date").orderBy("date_key").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_dim_airport(df):
    """
    Conformed airport dimension, keyed on the DOT numeric airport_id.

    Airports appear in silver as both origin and destination, so both sides are
    unioned. Keyed on airport_id rather than the IATA code because BTS reuses
    IATA codes across different airports over time.

    Where an airport_id has had more than one set of attributes across the
    loaded periods, the most recent flight date wins.
    """
    origin = df.select(
        F.col("origin_airport_id").alias("airport_id"),
        F.col("origin_code").alias("airport_code"),
        F.col("origin_city_name").alias("city_name"),
        F.col("origin_city_market_id").alias("city_market_id"),
        F.col("origin_state_abbr").alias("state_abbr"),
        F.col("origin_state_name").alias("state_name"),
        F.col("flight_date"),
    )

    dest = df.select(
        F.col("dest_airport_id").alias("airport_id"),
        F.col("dest_code").alias("airport_code"),
        F.col("dest_city_name").alias("city_name"),
        F.col("dest_city_market_id").alias("city_market_id"),
        F.col("dest_state_abbr").alias("state_abbr"),
        F.col("dest_state_name").alias("state_name"),
        F.col("flight_date"),
    )

    combined = origin.unionByName(dest)

    w = Window.partitionBy("airport_id").orderBy(F.col("flight_date").desc())

    return (combined
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "flight_date"))


from pyspark.sql.window import Window

dim_airport = build_dim_airport(silver)
dim_airport.write.mode("overwrite").format("delta").saveAsTable("dim_airport")

print("rows:", spark.table("dim_airport").count())
spark.table("dim_airport").orderBy("airport_code").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

a = spark.table("dim_airport")

print("rows                :", a.count())
print("distinct airport_id :", a.select("airport_id").distinct().count())
print("distinct airport_code:", a.select("airport_code").distinct().count())

print()
print("--- IATA codes used by more than one airport_id ---")
(a.groupBy("airport_code").count().filter(F.col("count") > 1).show(truncate=False))

print("--- city markets with multiple airports ---")
(a.groupBy("city_market_id")
  .agg(F.count("*").alias("airports"),
       F.collect_list("airport_code").alias("codes"))
  .filter(F.col("airports") > 1)
  .orderBy(F.desc("airports"))
  .show(5, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_dim_carrier(df):
    """
    Carrier dimension from the reporting airline fields in silver.

    The on-time table carries only the code, not the airline name. Names come
    from the BTS lookup tables and are joined in a later step.

    Carrier codes are reused over time; the most recent attributes win.
    """
    w = Window.partitionBy("carrier_code").orderBy(F.col("flight_date").desc())

    return (df
        .select("carrier_code", "carrier_dot_id", "carrier_iata_code", "flight_date")
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "flight_date"))


dim_carrier = build_dim_carrier(silver)
dim_carrier.write.mode("overwrite").format("delta").saveAsTable("dim_carrier")

print("rows:", spark.table("dim_carrier").count())
spark.table("dim_carrier").orderBy("carrier_code").show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_dim_cancellation_code():
    """
    Four BTS cancellation reasons plus an 'N' member for flights that were not
    cancelled, so the fact table has no nulls in its key column.
    """
    rows = [
        ("N", "Not cancelled"),
        ("A", "Carrier"),
        ("B", "Weather"),
        ("C", "National Air System"),
        ("D", "Security"),
    ]
    return spark.createDataFrame(rows, "cancellation_code STRING, cancellation_reason STRING")


def build_dim_delay_cause():
    """Five BTS delay causes, keyed for the unpivoted attribution fact."""
    rows = [
        ("CARRIER",       "Carrier",       1),
        ("WEATHER",       "Weather",       2),
        ("NAS",           "National Air System", 3),
        ("SECURITY",      "Security",      4),
        ("LATE_AIRCRAFT", "Late Aircraft", 5),
    ]
    return spark.createDataFrame(
        rows, "delay_cause_key STRING, delay_cause_name STRING, sort_order INT")


build_dim_cancellation_code().write.mode("overwrite").format("delta").saveAsTable("dim_cancellation_code")
build_dim_delay_cause().write.mode("overwrite").format("delta").saveAsTable("dim_delay_cause")

spark.table("dim_cancellation_code").show()
spark.table("dim_delay_cause").orderBy("sort_order").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FACT_BUSINESS_KEY = ["date_key", "carrier_code", "flight_number",
                     "origin_airport_id", "dest_airport_id", "crs_dep_time_hhmm"]


def build_fact_flight(df):
    """
    One row per scheduled flight leg as reported, including cancelled and
    diverted flights.

    cancellation_code is coalesced to 'N' so the key column has no nulls and
    every fact row joins to dim_cancellation_code. This is the only coalesce
    in the model - measure nulls are preserved.
    """
    return df.select(
        # dimension keys
        "date_key",
        "carrier_code",
        "origin_airport_id",
        "dest_airport_id",
        F.coalesce(F.col("cancellation_code"), F.lit("N")).alias("cancellation_code"),

        # degenerate dimensions
        "tail_number",
        "flight_number",

        # timestamps
        "crs_dep_ts", "dep_ts", "wheels_off_ts",
        "wheels_on_ts", "crs_arr_ts", "arr_ts",
        "crs_dep_time_hhmm",
        "dep_time_block", "arr_time_block",

        # measures
        "dep_delay", "dep_delay_minutes", "dep_del15",
        "taxi_out", "taxi_in",
        "arr_delay", "arr_delay_minutes", "arr_del15",
        "crs_elapsed_time", "actual_elapsed_time", "air_time", "distance",
        "cancelled", "diverted", "div_airport_landings",

        # audit
        "_source_file",
    )


def merge_fact(df, table, key):
    """Upsert a period into a fact table on its business key."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return {"action": "created", "rows": df.count()}

    target = DeltaTable.forName(spark, table)
    condition = " AND ".join([f"t.{c} <=> s.{c}" for c in key])

    (target.alias("t")
           .merge(df.alias("s"), condition)
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())

    return {"action": "merged", "rows": df.count()}


fact_flight = build_fact_flight(period)
print(merge_fact(fact_flight, "fact_flight", FACT_BUSINESS_KEY))

f = spark.table("fact_flight")
print("fact rows:", f.count())
f.groupBy("_source_file").count().orderBy("_source_file").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DELAY_BUSINESS_KEY = ["date_key", "carrier_code", "flight_number",
                      "origin_airport_id", "dest_airport_id",
                      "crs_dep_time_hhmm", "delay_cause_key"]


def build_fact_delay_attribution(df):
    """
    One row per flight per delay cause.

    BTS reports causes as five parallel columns. Unpivoting turns five
    near-duplicate measures into one, and keeps grain explicit.

    Populated only where arr_del15 = 1 - BTS does not report causes otherwise.
    Nulls are NOT coalesced to zero: doing so would fabricate on-time flights
    with zero-minute delays. Rows with a null or zero cause are dropped, so
    totals here will not reconcile to all delay minutes. That is correct.
    """
    causes = [
        ("CARRIER",       "carrier_delay"),
        ("WEATHER",       "weather_delay"),
        ("NAS",           "nas_delay"),
        ("SECURITY",      "security_delay"),
        ("LATE_AIRCRAFT", "late_aircraft_delay"),
    ]

    stack_expr = ", ".join([f"'{key}', {col}" for key, col in causes])

    return (df
        .filter(F.col("arr_del15") == 1)
        .select(
            "date_key", "carrier_code", "flight_number",
            "origin_airport_id", "dest_airport_id", "crs_dep_time_hhmm",
            F.expr(f"stack({len(causes)}, {stack_expr}) AS (delay_cause_key, delay_minutes)"),
            "_source_file",
        )
        .filter(F.col("delay_minutes").isNotNull() & (F.col("delay_minutes") > 0)))


fact_delay = build_fact_delay_attribution(period)
print(merge_fact(fact_delay, "fact_delay_attribution", DELAY_BUSINESS_KEY))

d = spark.table("fact_delay_attribution")
print("delay rows:", d.count())
d.groupBy("delay_cause_key").agg(
    F.count("*").alias("rows"),
    F.round(F.avg("delay_minutes"), 1).alias("avg_minutes"),
    F.sum("delay_minutes").alias("total_minutes"),
).orderBy(F.desc("total_minutes")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fl = spark.table("fact_flight").filter(F.col("_source_file") == source_file)
dl = spark.table("fact_delay_attribution").filter(F.col("_source_file") == source_file)

total_arr_delay = fl.filter(F.col("arr_delay_minutes") > 0) \
                    .agg(F.sum("arr_delay_minutes")).collect()[0][0]
attributed      = dl.agg(F.sum("delay_minutes")).collect()[0][0]

delayed_15plus  = fl.filter(F.col("arr_del15") == 1).count()
distinct_flights = dl.select("date_key", "carrier_code", "flight_number",
                             "origin_airport_id", "dest_airport_id",
                             "crs_dep_time_hhmm").distinct().count()

print("flights delayed 15+ min      :", delayed_15plus)
print("distinct flights in attribution:", distinct_flights)
print()
print("total arrival delay minutes  :", f"{total_arr_delay:,.0f}")
print("attributed delay minutes     :", f"{attributed:,.0f}")
print("unattributed                 :", f"{total_arr_delay - attributed:,.0f}",
      f"({(1 - attributed/total_arr_delay)*100:.1f}%)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = ["dim_date", "dim_airport", "dim_carrier", "dim_cancellation_code",
          "dim_delay_cause", "fact_flight", "fact_delay_attribution"]

for t in tables:
    print(f"  {t:<26} {spark.table(t).count():>10,}")

print()
print("--- fact_flight by period ---")
spark.table("fact_flight").groupBy("_source_file").count().orderBy("_source_file").show()

print("--- referential integrity ---")
f = spark.table("fact_flight").alias("f")

for key, dim, dim_key in [
    ("date_key",          "dim_date",              "date_key"),
    ("carrier_code",      "dim_carrier",           "carrier_code"),
    ("origin_airport_id", "dim_airport",           "airport_id"),
    ("dest_airport_id",   "dim_airport",           "airport_id"),
    ("cancellation_code", "dim_cancellation_code", "cancellation_code"),
]:
    d = spark.table(dim).alias("d")
    orphans = f.join(d, F.col(f"f.{key}") == F.col(f"d.{dim_key}"), "left_anti").count()
    flag = "OK" if orphans == 0 else "FAIL"
    print(f"  {key:<20} -> {dim:<24} orphans: {orphans:>6}  {flag}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
