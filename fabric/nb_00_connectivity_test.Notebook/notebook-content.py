# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

import requests

url = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2026_5.zip"

r = requests.get(url, verify=False, stream=True, timeout=60, allow_redirects=True)

print("status      :", r.status_code)
print("final url   :", r.url)
print("content-type:", r.headers.get("Content-Type"))
print("length (MB) :", round(int(r.headers.get("Content-Length", 0)) / 1024 / 1024, 2))

chunk = next(r.iter_content(chunk_size=4))
print("magic bytes :", chunk)
r.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
