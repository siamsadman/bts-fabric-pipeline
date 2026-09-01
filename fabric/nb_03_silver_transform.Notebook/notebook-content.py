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

BRONZE_TABLE = "bronze_flights"
SILVER_TABLE = "silver_flights"
WATERMARK_TABLE = "load_watermark"

source_file = f"{process_year}_{process_month:02d}.csv"

bronze = spark.table(BRONZE_TABLE).filter(F.col("_source_file") == source_file)

print("processing     :", f"{process_year}-{process_month:02d}")
print("source file    :", source_file)
print("bronze rows    :", bronze.count())
print("bronze columns :", len(bronze.columns))

if bronze.count() == 0:
    raise ValueError(f"no bronze rows for {source_file} - has it been ingested?")

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

# CELL ********************

SILVER_COLUMNS = [
    # --- date and flight identity ---
    F.col("FlightDate").alias("flight_date"),
    F.date_format("FlightDate", "yyyyMMdd").cast("int").alias("date_key"),
    F.col("Reporting_Airline").alias("carrier_code"),
    F.col("DOT_ID_Reporting_Airline").cast("int").alias("carrier_dot_id"),
    F.col("IATA_CODE_Reporting_Airline").alias("carrier_iata_code"),
    F.col("Tail_Number").alias("tail_number"),
    F.col("Flight_Number_Reporting_Airline").cast("int").alias("flight_number"),

    # --- origin ---
    F.col("OriginAirportID").cast("int").alias("origin_airport_id"),
    F.col("OriginAirportSeqID").cast("int").alias("origin_airport_seq_id"),
    F.col("OriginCityMarketID").cast("int").alias("origin_city_market_id"),
    F.col("Origin").alias("origin_code"),
    F.col("OriginCityName").alias("origin_city_name"),
    F.col("OriginState").alias("origin_state_abbr"),
    F.col("OriginStateName").alias("origin_state_name"),

    # --- destination ---
    F.col("DestAirportID").cast("int").alias("dest_airport_id"),
    F.col("DestAirportSeqID").cast("int").alias("dest_airport_seq_id"),
    F.col("DestCityMarketID").cast("int").alias("dest_city_market_id"),
    F.col("Dest").alias("dest_code"),
    F.col("DestCityName").alias("dest_city_name"),
    F.col("DestState").alias("dest_state_abbr"),
    F.col("DestStateName").alias("dest_state_name"),

    # --- timestamps (derived) ---
    F.col("crs_dep_ts"),
    F.col("dep_ts"),
    F.col("wheels_off_ts"),
    F.col("wheels_on_ts"),
    F.col("crs_arr_ts"),
    F.col("arr_ts"),

    # --- raw hhmm retained for traceability ---
    F.col("CRSDepTime").cast("int").alias("crs_dep_time_hhmm"),
    F.col("DepTime").cast("int").alias("dep_time_hhmm"),
    F.col("CRSArrTime").cast("int").alias("crs_arr_time_hhmm"),
    F.col("ArrTime").cast("int").alias("arr_time_hhmm"),
    F.col("DepTimeBlk").alias("dep_time_block"),
    F.col("ArrTimeBlk").alias("arr_time_block"),

    # --- delay measures ---
    F.col("DepDelay").alias("dep_delay"),
    F.col("DepDelayMinutes").alias("dep_delay_minutes"),
    F.col("DepDel15").alias("dep_del15"),
    F.col("ArrDelay").alias("arr_delay"),
    F.col("ArrDelayMinutes").alias("arr_delay_minutes"),
    F.col("ArrDel15").alias("arr_del15"),
    F.col("TaxiOut").alias("taxi_out"),
    F.col("TaxiIn").alias("taxi_in"),

    # --- duration and distance ---
    F.col("CRSElapsedTime").alias("crs_elapsed_time"),
    F.col("ActualElapsedTime").alias("actual_elapsed_time"),
    F.col("AirTime").alias("air_time"),
    F.col("Distance").alias("distance"),

    # --- status ---
    F.col("Cancelled").alias("cancelled"),
    F.col("CancellationCode").alias("cancellation_code"),
    F.col("Diverted").alias("diverted"),
    F.col("DivAirportLandings").alias("div_airport_landings"),

    # --- delay causes (null unless ArrDel15 = 1 - not coalesced) ---
    F.col("CarrierDelay").alias("carrier_delay"),
    F.col("WeatherDelay").alias("weather_delay"),
    F.col("NASDelay").alias("nas_delay"),
    F.col("SecurityDelay").alias("security_delay"),
    F.col("LateAircraftDelay").alias("late_aircraft_delay"),

    # --- audit ---
    F.col("_ingest_timestamp"),
    F.col("_source_file"),
]

silver = silver.select(*SILVER_COLUMNS)

print("silver columns:", len(silver.columns))
print()
for c in silver.columns:
    print(" ", c)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BUSINESS_KEY = ["flight_date", "carrier_code", "flight_number",
                "origin_airport_id", "dest_airport_id", "crs_dep_time_hhmm"]

total  = silver.count()
unique = silver.select(*BUSINESS_KEY).distinct().count()

print("rows           :", total)
print("distinct keys  :", unique)
print("duplicates     :", total - unique)

if total != unique:
    print()
    print("--- sample duplicate keys ---")
    dupes = (silver.groupBy(*BUSINESS_KEY)
                   .count()
                   .filter(F.col("count") > 1)
                   .orderBy(F.desc("count")))
    dupes.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

def ensure_watermark_table():
    """Create the watermark table on first run. Safe to call repeatedly."""
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        schema = """
            year INT,
            month INT,
            source_file STRING,
            row_count BIGINT,
            status STRING,
            loaded_at TIMESTAMP
        """
        spark.sql(f"CREATE TABLE {WATERMARK_TABLE} ({schema}) USING DELTA")
        print(f"created {WATERMARK_TABLE}")
    else:
        print(f"{WATERMARK_TABLE} exists")

ensure_watermark_table()
spark.table(WATERMARK_TABLE).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BUSINESS_KEY = ["flight_date", "carrier_code", "flight_number",
                "origin_airport_id", "dest_airport_id", "crs_dep_time_hhmm"]


def merge_silver(df):
    """
    Upsert a period into silver on the business key.
    Creates the table on first run; merges thereafter.
    Re-running the same period updates in place rather than duplicating.
    """
    if not spark.catalog.tableExists(SILVER_TABLE):
        df.write.format("delta").saveAsTable(SILVER_TABLE)
        return {"action": "created", "rows": df.count()}

    target = DeltaTable.forName(spark, SILVER_TABLE)
    condition = " AND ".join([f"t.{c} <=> s.{c}" for c in BUSINESS_KEY])

    (target.alias("t")
           .merge(df.alias("s"), condition)
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())

    return {"action": "merged", "rows": df.count()}


result = merge_silver(silver)
print(result)

s = spark.table(SILVER_TABLE)
print("silver total rows:", s.count())
s.groupBy("_source_file").count().orderBy("_source_file").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def record_watermark(year, month, source_file, row_count, status="success"):
    """Upsert the load record for a period."""
    row = spark.createDataFrame(
        [(year, month, source_file, row_count, status)],
        "year INT, month INT, source_file STRING, row_count BIGINT, status STRING"
    ).withColumn("loaded_at", F.current_timestamp())

    target = DeltaTable.forName(spark, WATERMARK_TABLE)
    (target.alias("t")
           .merge(row.alias("s"), "t.year = s.year AND t.month = s.month")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())


record_watermark(process_year, process_month, source_file, result["rows"])

spark.table(WATERMARK_TABLE).orderBy("year", "month").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
