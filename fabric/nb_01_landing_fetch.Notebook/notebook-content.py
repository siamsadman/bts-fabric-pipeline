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

# # Landing: fetch BTS monthly file
# 
# Downloads one monthly zip from TranStats, extracts the single CSV to
# `Files/landing/csv/`, and deletes the zip. Also refreshes the reference
# lookup tables used by `dim_carrier`.
# 
# ## Notes
# 
# Fabric notebooks have outbound internet access, so files are fetched
# server-side at datacentre bandwidth rather than uploaded.
# 
# The source URL does **not** zero-pad the month; landed filenames **are**
# zero-padded so they sort correctly.
# 
# Zips are written to session-local `/tmp` and deleted after extraction.
# Spark cannot read zip, and OneLake is not a useful home for them.
# 
# TranStats obfuscates its query-string parameters, so the lookup URLs below
# are captured constants rather than constructed. Do not attempt to build them.
# 
# An already-landed month returns `skipped`, so the notebook is safe to re-run.

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

import os, time, zipfile, shutil, requests, urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LANDING   = "/lakehouse/default/Files/landing/csv"
REFERENCE = "/lakehouse/default/Files/landing/reference"
TMP       = "/tmp/bts"

URL = ("https://transtats.bts.gov/PREZIP/"
       "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{y}_{m}.zip")

LOOKUPS = {
    "L_UNIQUE_CARRIERS": "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef",
    "L_AIRLINE_ID":      "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVeYVaR_VQ",
}

for d in (LANDING, REFERENCE, TMP):
    os.makedirs(d, exist_ok=True)

print("processing:", f"{process_year}-{process_month:02d}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_month(year, month, overwrite=False):
    """Download and extract one monthly file. Skips if already landed."""
    target = f"{LANDING}/{year}_{month:02d}.csv"

    if os.path.exists(target) and not overwrite:
        return {"period": f"{year}-{month:02d}", "status": "skipped",
                "mb": round(os.path.getsize(target) / 1024 / 1024, 1)}

    url      = URL.format(y=year, m=month)      # month deliberately not padded
    zip_path = f"{TMP}/{year}_{month}.zip"
    t0       = time.time()

    with requests.get(url, verify=False, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"expected exactly 1 CSV, found {names}")
        with z.open(names[0]) as src, open(target, "wb") as dst:
            shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)

    os.remove(zip_path)

    return {"period": f"{year}-{month:02d}", "status": "landed",
            "mb":   round(os.path.getsize(target) / 1024 / 1024, 1),
            "secs": round(time.time() - t0, 1)}


def fetch_lookup(name):
    """Download a BTS lookup table. Always refreshed - small, and can gain rows."""
    target = f"{REFERENCE}/{name}.csv"

    r = requests.get(LOOKUPS[name], verify=False, timeout=120)
    r.raise_for_status()

    if r.content.lstrip()[:4] != b"Code":
        raise ValueError(f"{name}: expected CSV, got {r.content[:60]!r}")

    with open(target, "wb") as f:
        f.write(r.content)

    return {"table": name, "status": "landed",
            "kb": round(os.path.getsize(target) / 1024, 1)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(fetch_month(process_year, process_month))

for name in LOOKUPS:
    print(fetch_lookup(name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = sorted(os.listdir(LANDING))
for f in files:
    print(f"  {f:<16} {round(os.path.getsize(f'{LANDING}/{f}') / 1024 / 1024, 1):>8} MB")

print()
print("months landed:", len(files))
print("total MB     :", round(sum(os.path.getsize(f"{LANDING}/{f}") for f in files) / 1024 / 1024, 1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
