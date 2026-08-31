# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Connectivity test
# 
# One-off check, run before the ingestion approach was decided: can a Fabric
# notebook reach transtats.bts.gov directly?
# 
# If yes, files are fetched server-side at datacentre bandwidth and ingestion
# becomes a genuine pipeline step. If no, the fallback was uploading ~1.9 GB
# of local zips through OneLake File Explorer — slow, and a manual step that
# would have to be repeated for every future month.
# 
# ## Result — 31 Aug 2026
# 
# Reachable. Status 200, `application/x-zip-compressed`, 30.25 MB, magic bytes
# `PK\x03\x04`. No redirect occurs on the PREZIP path.
# 
# The TranStats certificate chain fails validation in most HTTP clients, so
# `verify=False` is required.
# 
# **Why the magic-byte check matters:** TranStats returns HTTP 200 with an HTML
# page for URLs it does not recognise, so a status code alone proves nothing.
# Two separate failures during this build returned 200 with HTML bodies. Assert
# on content, not status.
# 
# This notebook is not part of the pipeline. It is kept as the record of a
# decision.


# CELL ********************

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
