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

import os, time, zipfile, shutil, requests, urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LANDING = "/lakehouse/default/Files/landing/csv"
TMP     = "/tmp/bts"
URL     = ("https://transtats.bts.gov/PREZIP/"
           "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{y}_{m}.zip")

os.makedirs(LANDING, exist_ok=True)
os.makedirs(TMP, exist_ok=True)

def fetch_month(year, month, overwrite=False):
    target = f"{LANDING}/{year}_{month:02d}.csv"

    if os.path.exists(target) and not overwrite:
        return {"period": f"{year}-{month:02d}", "status": "skipped",
                "mb": round(os.path.getsize(target) / 1024 / 1024, 1)}

    url      = URL.format(y=year, m=month)          # month deliberately not padded
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

print(fetch_month(2020, 1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target = f"{LANDING}/2020_01.csv"

with open(target, "r", encoding="utf-8", errors="replace") as f:
    header = f.readline().rstrip("\n")
    row1   = f.readline().rstrip("\n")

cols = header.split(",")
print("column count :", len(cols))
print("first 12     :", cols[:12])
print("last 5       :", cols[-5:])
print()
print("row 1 ends   :", row1[-40:])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for y, m in [(2020, 4), (2023, 6)]:
    print(fetch_month(y, m))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for f in sorted(os.listdir(LANDING)):
    p = f"{LANDING}/{f}"
    print(f"{f:<16} {round(os.path.getsize(p) / 1024 / 1024, 1):>8} MB")

print()
print("total MB:", round(sum(os.path.getsize(f"{LANDING}/{f}") for f in os.listdir(LANDING)) / 1024 / 1024, 1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LOOKUPS = {
    "L_UNIQUE_CARRIERS": "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef",
    "L_AIRLINE_ID":      "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVeYVaR_VQ",
}

for name, url in LOOKUPS.items():
    r = requests.get(url, verify=False, timeout=60, allow_redirects=True)
    head = r.content[:120].decode("utf-8", errors="replace").replace("\n", " | ")
    print(f"{name}")
    print("  status      :", r.status_code)
    print("  content-type:", r.headers.get("Content-Type"))
    print("  size (KB)   :", round(len(r.content) / 1024, 1))
    print("  first bytes :", head)
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

REFERENCE = "/lakehouse/default/Files/landing/reference"
os.makedirs(REFERENCE, exist_ok=True)

# TranStats obfuscates its query-string parameters, so these URLs are captured
# constants rather than constructed. Sourced from the "Get Lookup Table" links
# on the Reporting Carrier On-Time Performance field-selection page.
LOOKUPS = {
    "L_UNIQUE_CARRIERS": "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef",
    "L_AIRLINE_ID":      "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVeYVaR_VQ",
}


def fetch_lookup(name, overwrite=True):
    """Download a BTS lookup table into the reference landing folder."""
    target = f"{REFERENCE}/{name}.csv"

    if os.path.exists(target) and not overwrite:
        return {"table": name, "status": "skipped"}

    r = requests.get(LOOKUPS[name], verify=False, timeout=120)
    r.raise_for_status()

    if not r.content.lstrip()[:4] == b"Code":
        raise ValueError(f"{name}: expected CSV, got {r.content[:60]!r}")

    with open(target, "wb") as f:
        f.write(r.content)

    return {"table": name, "status": "landed",
            "kb": round(os.path.getsize(target) / 1024, 1)}


for name in LOOKUPS:
    print(fetch_lookup(name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
