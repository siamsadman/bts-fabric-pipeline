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

# # Silver: BTS flight transform
# 
# Reads `bronze_flights`, derives timestamps, and writes the cleaned, typed
# silver table.
# 
# ## Data quality handled here
# 
# 1. Times are `hhmm` integers without leading zeros (`530` = 05:30)
# 2. `2400` appears for midnight and is not a valid time
# 3. BTS supplies no arrival date; it must be derived
# 4. Cancelled flights are null across actual-time fields — retained, not dropped
# 5. Delay causes are null unless `ArrDel15 = 1` — not coalesced to zero
# 6. Header casing varies between download vintages
# 
# ## Timestamp approach
# 
# Departure timestamps are built from `FlightDate` plus the `hhmm` value.
# 
# Arrival timestamps take the **date** from arithmetic (`dep_ts` + elapsed minutes,
# which spans midnight correctly) and the **time of day** from the BTS reported
# value, which is already destination-local.
# 
# Known limitation: `ActualElapsedTime` is gate-to-gate in local clock time at each
# end, so a purely arithmetic arrival would be in origin-local time — out by the
# UTC offset between airports. Taking the clock from BTS avoids this without
# requiring an airport-to-timezone reference dataset. A small residual remains for
# flights whose arithmetic date lands either side of midnight from the
# destination-local date.


# CELL ********************

from pyspark.sql import functions as F

BRONZE_TABLE = "bronze_flights"
SILVER_TABLE = "silver_flights"

bronze = spark.table(BRONZE_TABLE)

print("bronze rows   :", bronze.count())
print("bronze columns:", len(bronze.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def to_timestamp(date_col, time_col):
    """
    Build a timestamp from a date column and a BTS hhmm integer.
    Handles missing leading zeros (530 -> 05:30).
    2400 means midnight at the start of the following day.
    Returns null when the time is null, so cancelled flights stay null.
    """
    t = F.col(time_col)
    d = F.col(date_col)

    rolls_over = t == 2400
    base_date  = F.when(rolls_over, F.date_add(d, 1)).otherwise(d)
    hours      = F.when(rolls_over, F.lit(0)).otherwise((t / 100).cast("int"))
    minutes    = F.when(rolls_over, F.lit(0)).otherwise(t % 100)

    return F.when(
        t.isNull(), F.lit(None).cast("timestamp")
    ).otherwise(
        base_date.cast("timestamp")
        + F.expr("INTERVAL 1 HOUR") * hours
        + F.expr("INTERVAL 1 MINUTE") * minutes
    )


def arrival_timestamp(dep_ts_col, elapsed_col, arr_time_col):
    """
    Build an arrival-side timestamp in destination-local time.

    Date comes from arithmetic: dep_ts + elapsed minutes, which crosses
    midnight correctly. Time of day comes from the BTS reported value,
    which is already destination-local.

    2400 means midnight; the arithmetic has already crossed it, so unlike
    to_timestamp() no further day is added.
    """
    arithmetic = F.col(dep_ts_col) + F.expr("INTERVAL 1 MINUTE") * F.col(elapsed_col)
    arr_date   = F.to_date(arithmetic)

    t       = F.col(arr_time_col)
    hours   = F.when(t == 2400, F.lit(0)).otherwise((t / 100).cast("int"))
    minutes = F.when(t == 2400, F.lit(0)).otherwise(t % 100)

    return F.when(
        t.isNull() | F.col(dep_ts_col).isNull(),
        F.lit(None).cast("timestamp")
    ).otherwise(
        arr_date.cast("timestamp")
        + F.expr("INTERVAL 1 HOUR") * hours
        + F.expr("INTERVAL 1 MINUTE") * minutes
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver = (bronze
    # departure side: built from FlightDate + hhmm
    .withColumn("crs_dep_ts",    to_timestamp("FlightDate", "CRSDepTime"))
    .withColumn("dep_ts",        to_timestamp("FlightDate", "DepTime"))
    .withColumn("wheels_off_ts", to_timestamp("FlightDate", "WheelsOff"))
)

silver = (silver
    # arrival side: date from arithmetic, clock from BTS
    .withColumn("crs_arr_ts",   arrival_timestamp("crs_dep_ts",    "CRSElapsedTime",    "CRSArrTime"))
    .withColumn("arr_ts",       arrival_timestamp("dep_ts",        "ActualElapsedTime", "ArrTime"))
    .withColumn("wheels_on_ts", arrival_timestamp("wheels_off_ts", "AirTime",           "WheelsOn"))
)

print("timestamps:", [c for c in silver.columns if c.endswith("_ts")])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_timestamps(df):
    """
    Two independent checks. Both must pass.
      1. Derived clock matches the BTS reported time of day.
      2. Arrival date is the flight date or the day after - never earlier,
         never two days later.
    """
    clock = (df
        .filter(F.col("arr_ts").isNotNull() & F.col("ArrTime").isNotNull())
        .withColumn("derived_hhmm", F.hour("arr_ts") * 100 + F.minute("arr_ts"))
        .withColumn("reported_hhmm",
                    F.when(F.col("ArrTime") == 2400, F.lit(0)).otherwise(F.col("ArrTime")))
        .withColumn("matches", F.col("derived_hhmm") == F.col("reported_hhmm")))

    span = (df
        .filter(F.col("arr_ts").isNotNull())
        .withColumn("day_offset", F.datediff(F.to_date("arr_ts"), F.col("FlightDate"))))

    print("clock agreement with BTS ArrTime:")
    clock.groupBy("matches").count().show()

    print("arrival date offset from flight date:")
    span.groupBy("day_offset").count().orderBy("day_offset").show()

    bad_clock = clock.filter(~F.col("matches")).count()
    bad_span  = span.filter(~F.col("day_offset").isin(0, 1)).count()

    if bad_clock or bad_span:
        raise ValueError(f"validation failed: {bad_clock} clock, {bad_span} span")

    print("both checks passed")


validate_timestamps(silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
