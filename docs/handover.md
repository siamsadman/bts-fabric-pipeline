# Fabric Portfolio Project — Handover

**Status:** Landing, bronze, silver and gold built and validated against a three-month development slice. Orchestration pipeline, backfill, semantic model and README outstanding.
**Owner:** Siam Sadman
**Last updated:** 31 Aug 2026

---

## Purpose

A portfolio project that demonstrates Microsoft Fabric data engineering capability, not Power BI report building. The existing Olist series already proves dashboard and DAX skill; this project has to prove lakehouse architecture, Spark transformation, and pipeline orchestration — the claims DP-600 implies.

The audience is hiring managers and recruiters reviewing a GitHub repo. Most will not have Fabric access, so **the repo is the deliverable, not the workspace.**

---

## Constraints

- **Fabric trial: activated 30 Aug 2026.** Capacity `Trial-20260830T170531Z-yfzohItEVE233J_4YipPYQ`, SKU FTL4, region East Asia. Capacity ID `9FEE90A8-1279-40A7-B58A-54AFCF39EFD0`. Expires approximately **4 Nov 2026**. On expiry, reports stop rendering and items become inaccessible after a 7-day grace period. All evidence must live in the repo before then.
- **Capacity is F4 (4 CU) and cannot be resized** — checked 30 Aug 2026, portal states the size can't be changed for this trial capacity and offers only a sales contact.
  - *Observed 31 Aug:* performance is better than anticipated. Reading a 261 MB CSV with inference plus a full count took 11 seconds; writing 607k rows to Delta took 13 seconds; merging 577k rows took under a minute. No throttling encountered during development. The contingency of cutting the backfill to 2022 onward looks unnecessary, but remains available.
- **Effort budget: 2–3 weeks of evenings.** Not 60 days. Active job search stays the priority; this project supports it and must not displace it.
- Trial capacity is separate from Azure credit — lakehouse and notebook use does not consume the $200.

---

## Decisions locked

### Dataset: BTS Airline On-Time Performance

Chosen over Olist and AdventureWorks.

Rationale:
- **Volume justifies the architecture.** ~600k flights/month; five years ≈ 35M-row fact table. Olist's ~1.5M total rows would make a medallion lakehouse look like architecture theatre and invite the question "why did you need Spark for this?" *Confirmed: the three-month development slice alone is 1,497,990 rows — already larger than the entire Olist dataset.*
- **Genuine incremental load.** BTS publishes monthly, so pipeline and watermark logic process real new periods rather than a simulated drip-feed.
- **Silver layer does real work.** Raw CSV with coded columns and messy nulls, so cleaning and typing is substantive rather than passthrough.
- **Clean star schema** falls out naturally: flight fact plus carrier, airport, date, and delay-cause dimensions.
- **Domain fit.** Operational performance analytics — on-time rate, delay attribution, route reliability — sits adjacent to the TOLL logistics work already on the CV.

Source: TranStats (transtats.bts.gov). Reporting Carrier On-Time Performance runs 1987–present; Marketing Carrier from January 2018.

Rejected alternatives:
- **NYC TLC trip data** — larger and already parquet, but it is Microsoft's own demo dataset throughout the Fabric documentation. Reads as tutorial-following.
- **Dunnhumby Complete Journey** — genuinely rich retail data mapping to FMCG background, but a fixed snapshot with no incremental story.
- **Olist** — known domain and existing validated model, but too small, and reuses a dataset already published in the portfolio.
- **AdventureWorks** — too common, synthetic.

### Architecture: medallion lakehouse

| Layer | Content | Status |
|---|---|---|
| **Landing** | Monthly BTS CSVs in OneLake `Files/landing/csv/`, reference lookups in `Files/landing/reference/` | Built |
| **Bronze** | `bronze_flights` — Delta append with explicit schema, `_ingest_timestamp`, `_source_file`. No cleaning. | Built |
| **Silver** | `silver_flights` — cleaned, typed, 56 columns. `MERGE INTO` upsert against a watermark table. | Built |
| **Gold** | Star schema — two facts, five dimensions. | Built |
| **Orchestration** | Data Factory pipeline running the notebooks in sequence, parameterised by period, with watermark lookup, failure path, and schedule. | **Outstanding** |
| **Semantic model** | Direct Lake over gold. Handful of measures, one report page. Do not rebuild nine pages — report skill is already proven elsewhere. | **Outstanding** |

One lakehouse (`lh_bts`) for all layers, **not** three. Three would mean cross-lakehouse references in every notebook for no architectural gain at this scale. Layer separation is by table naming prefix.

**Lakehouse schemas deliberately not enabled.** Schema support is the newer of the two paths and has had friction with some pipeline activities and Spark write patterns — friction that would be debugged on evenings rather than building. Accepted cost: medallion structure reads as a naming convention rather than as folders. Revisitable; the tables are Delta either way.

### Notebook inventory

| Notebook | Purpose | Parameterised |
|---|---|---|
| `nb_00_connectivity_test` | Confirms Fabric can reach transtats.bts.gov | No |
| `nb_01_landing_fetch` | Downloads and extracts monthly files; fetches reference lookups | By period |
| `nb_02_bronze_ingest` | Explicit-schema read into `bronze_flights` | By period |
| `nb_03_silver_transform` | Timestamp derivation, column selection, merge, watermark | By period |
| `nb_04_gold_build` | Dimensions (full rebuild) and facts (incremental merge) | By period, facts only |

Each notebook has a **parameter cell** (`process_year`, `process_month`) toggled via the cell's `...` menu. Defaults are set for interactive development; the pipeline overrides them at runtime.

### Working practices

- **Git integration to GitHub, connected before any items were created.** Repo `github.com/siamsadman/bts-fabric-pipeline`, branch `main`, Fabric folder `/fabric`. Fine-grained PAT, Contents read/write, 90-day expiry (issued 31 Aug, so **expires ~29 Nov 2026** — after trial expiry, no renewal needed).
- **Tenant setting gotcha:** *Users can sync workspace items with GitHub repositories* is **disabled by default** and lives in the Admin portal, not workspace settings. With it off, GitHub simply does not appear in the workspace Git provider list — it looks like a missing feature rather than a permission.
- **Fabric Git sync carries item definitions only, not table contents.** `lh_bts.Lakehouse` in the repo is a `.platform` identity file; creating tables produces no commit. Consequence: the shape of the model is **not** discoverable from the repo alone. Screenshots and the README are therefore load-bearing evidence, not decoration.
- **Screenshot as you build, not at the end.** See the evidence checklist below.
- **README must include an architecture diagram.** The repo has to be self-explanatory to a reviewer who never opens Fabric.

---

## Ingestion — decided and confirmed 31 Aug 2026

**Server-side fetch, not upload.** Fabric notebooks have outbound internet access and reach transtats.bts.gov directly. Confirmed by `nb_00_connectivity_test`: status 200, `application/x-zip-compressed`, 30.25 MB, magic bytes `PK\x03\x04`.

This avoids a 1.92 GB upload over a slow connection, runs at datacentre bandwidth, and makes ingestion a genuine automated pipeline step. The OneLake File Explorer fallback is **retired** — local zips are provenance backup only.

**Download URL pattern** — note the month is *not* zero-padded:

```
https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YYYY}_{M}.zip
```

The certificate chain trips up most HTTP clients — use `verify=False`. Contrary to the earlier note, **no redirect occurs**; `-L` / `allow_redirects` is not needed for the PREZIP path.

**Landing behaviour:** the notebook downloads to session-local `/tmp`, extracts the single CSV to `Files/landing/csv/{YYYY}_{MM}.csv`, and deletes the zip. Zips are not retained in OneLake — Spark cannot read zip anyway, and the CRC-verified local archive is the provenance copy. Filenames are **zero-padded on landing** even though the source URL is not, so they sort correctly.

An already-landed month returns `skipped` rather than re-downloading, so the notebook is safe to re-run.

**Observed:** roughly 53 seconds per month including extraction; ~250 MB CSV from a ~27 MB zip. Full backfill projects to roughly **68 minutes and 20 GB** of landed CSV.

### Reference lookups — the TranStats obfuscation gotcha

`DimCarrier` needs airline names, which come from the BTS lookup tables. **TranStats obfuscates its query-string parameters with a substitution cipher.** The documented-looking URL `Download_Lookup.asp?Lookup=L_AIRLINE_ID` returns the site homepage with a 200 status; the `.aspx` variant returns 404.

The working URLs, captured from the "Get Lookup Table" links on the field-selection page:

```
https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef   # L_UNIQUE_CARRIERS
https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVeYVaR_VQ        # L_AIRLINE_ID
```

`Y11x72` decodes to `Lookup`, but the scheme is not plain ROT13 — digits substitute for some letters and case is preserved irregularly. **Do not attempt to construct these URLs.** They are captured constants, and the README should say so.

**Validation lesson:** two separate failures tonight returned HTTP 200 with HTML. Status codes alone prove nothing; assert on content. The zip fetch checks magic bytes; the lookup fetch checks the response starts with the `Code` header.

---

## Gold layer — star schema

### Grain

**One row per scheduled flight leg as reported, including cancelled and diverted flights.**

Cancelled and diverted rows are retained deliberately. Filtering them out is the common mistake with this dataset — it silently destroys cancellation-rate analysis and biases every delay average.

*Confirmed in the data:* April 2020 has a **41.5% cancellation rate** (130,078 of 313,382 rows). Filtering cancellations would discard two fifths of that month.

### Key strategy

Use **natural keys**, not generated surrogates.

- `airport_id` — the DOT numeric airport ID
- `carrier_code` — the reporting airline code
- `date_key` — integer `yyyymmdd`, generated in silver

Generated surrogate keys in a lakehouse are fragile: `monotonically_increasing_id()` is not stable across reruns, and a proper key-assignment step adds pipeline machinery this project does not need.

**Do not key on the three-letter IATA airport code.** BTS reuses those codes across different airports over time. A check in `nb_04` counts distinct `airport_id` against distinct `airport_code` — currently 367 vs 367, no collisions, but the three-month slice is too narrow to exercise decades-scale reuse. Keep the check; it becomes meaningful after backfill.

### Tables as built

| Table | Rows (3-month slice) | Load pattern |
|---|---|---|
| `dim_date` | 2,953 | Generated 2019-12-01 to 2027-12-31, full rebuild |
| `dim_airport` | 367 | Full rebuild from silver |
| `dim_carrier` | 17 | Full rebuild, joined to reference lookup |
| `dim_cancellation_code` | 5 | Static |
| `dim_delay_cause` | 5 | Static |
| `fact_flight` | 1,497,990 | Incremental merge by period |
| `fact_delay_attribution` | 390,324 | Incremental merge by period |

**Dimensions rebuild in full on every run; facts merge incrementally.** Dimensions are small and a new airport or carrier can appear in any period. The period parameter therefore applies to the facts only — worth a markdown note, since a reviewer will otherwise wonder why half the cells ignore it.

**`fact_flight`** — `cancellation_code` is coalesced to `'N'` ("Not cancelled") so the key column has no nulls and every fact row joins. This is the **only** coalesce in the model; measure nulls are preserved.

Both `dep_delay` and `dep_delay_minutes` are kept. The first goes negative for early departures; the second floors at zero. Averaging the wrong one gives a different answer — worth a README sentence.

**`fact_delay_attribution`** — built with Spark's `stack()` to unpivot the five parallel BTS cause columns. Grain is flight plus cause, so `delay_cause_key` is part of the business key. Rows with null or zero delay minutes are dropped; nulls are **not** coalesced to zero, which would fabricate on-time flights with zero-minute delays.

*Reconciliation, measured:* attribution covers **91.3%** of arrival delay minutes for January 2020 (5,283,250 of 5,785,006). The gap is flights delayed 1–14 minutes, for which BTS reports no cause. The unattributed share moves with severity — 11.7% in April 2020 when delays were mild, 5.2% in June 2023 when they were long. **This is correct behaviour and should be stated as a measured figure, not apologised for.**

Every flight with `arr_del15 = 1` appears in attribution (82,285 = 82,285 for January), which is the check that the filter and unpivot agree on population.

**`dim_airport`** — conformed, role-playing for origin and destination. Built by unioning the origin and destination projections of silver and deduplicating with `row_number()` over `airport_id` ordered by most recent flight date.

In the semantic model, create two relationships to `fact_flight` — active on origin, inactive on destination — and expose destination-side measures via `USERELATIONSHIP`.

`city_market_id` is retained and pays off: it groups **six** New York-area airports (HPN, ISP, JFK, LGA, SWF, EWR), five for Los Angeles, three each for Washington and the Bay Area.

*Optional stretch:* `AirportSeqID` is retained in silver and is a ready-made Type 2 SCD if time allows.

**`dim_carrier`** — joined to `L_AIRLINE_ID` on **DOT ID, not carrier code**. BTS defines a unique airline by its DOT certificate regardless of code, name, or holding company, and reuses codes across carriers over time (their own example: `PA`, `PA(1)`, `PA(2)`). `L_AIRLINE_ID` descriptions embed the code after a colon — `"Southwest Airlines Co.: WN"` — so split on `:` and keep the name.

Joined with a `left` join so a carrier missing from the lookup survives with a null name rather than vanishing and orphaning facts. Currently 17 carriers, zero missing names.

**`dim_cancellation_code`** — five rows: `N` not cancelled, `A` carrier, `B` weather, `C` national air system, `D` security.

**`dim_delay_cause`** — five rows with a `sort_order` column so charts order correctly.

*Optional:* `DimTimeBlock` from `dep_time_block` / `arr_time_block`, retained in silver.

### Referential integrity

`nb_04` ends with a `left_anti` join check per relationship. All five currently report zero orphans. **Alias both sides of the join and qualify every column reference** — unqualified names raise `AMBIGUOUS_REFERENCE` once both sides share a column name.

---

## Silver layer

### Columns

109 source fields reduced to **56**, using an explicit **keep-list rather than a drop-list**. An inclusion list means a new column in a future BTS vintage is ignored by default rather than flowing through uninvited, and it reads better than 69 exclusions.

Names are normalised to `snake_case` in silver. BTS PascalCase stays in bronze as the source contract — this also handles the vintage casing variance in one place.

Retained beyond the strict gold requirement: `origin_airport_seq_id` / `dest_airport_seq_id` (SCD stretch), `dep_time_block` / `arr_time_block` (time-block dimension), `carrier_iata_code`, `carrier_dot_id`, `origin_city_market_id` / `dest_city_market_id`, and the raw `hhmm` integers alongside the derived timestamps. Silver being slightly wider than gold needs is cheap insurance against reprocessing.

### The phantom column

The BTS CSV ends each row with a trailing comma, producing a 110th unnamed field. Spark names it `_c109`.

**Declare it and drop it by name.** Reading with a 109-field schema works, but Spark then flags every row as containing discarded data — a "corrupted records" diagnostic on 607,346 rows that is alarming, wrong, and trains you to ignore warnings. Declaring `_phantom` as a 110th `StringType` field and calling `.drop("_phantom")` silences it and makes the intent visible. It is also faster: 13 seconds against 24.

### Bronze schema strategy

**Explicit schema, not inference.** Inference reads the file twice and produces *inconsistent types between months* — a column that is entirely null in one month infers as `string` and as `int` in another, and the append fails with a schema mismatch partway through the backfill.

The schema was generated once from an inferred read of January 2020, then hardened:
- All numeric measures forced to `double`, so a month where a column is entirely null does not break the append.
- All `Div*` numerics forced to `double` — these are the emptiest columns and where inference is least trustworthy.

Verified against April 2020 (the worst month for nulls) before writing anything.

### Timestamp derivation

This is the substance of the silver notebook.

**Departure side** (`crs_dep_ts`, `dep_ts`, `wheels_off_ts`): built from `FlightDate` plus the `hhmm` integer. Split arithmetically — `hours = t // 100`, `minutes = t % 100` — which handles the missing leading zero without depending on string length. `2400` becomes `00:00` **on the following day**; normalising to `0000` without advancing the date silently moves the flight 24 hours earlier.

**Arrival side** (`crs_arr_ts`, `arr_ts`, `wheels_on_ts`): **date from arithmetic, clock from BTS.**

The original plan — add elapsed minutes to the departure timestamp — was tested and produced a **48% mismatch** against the reported `ArrTime`. Every mismatch was a whole number of hours and tracked direction of travel. Cause: `ActualElapsedTime` is gate-to-gate in *local clock time at each end*, so pure arithmetic yields origin-local time while BTS reports destination-local.

Options considered:
- **A.** Keep arithmetic. Correct durations, wrong wall clock.
- **B.** Date from arithmetic, clock from `ArrTime`. **Chosen.**
- **C.** Airport-to-timezone lookup with proper DST handling. **Deferred** — requires a third data source (OpenFlights or similar) keyed on IATA codes, which is exactly the unstable join the handover warns against, plus DST-aware conversion. Roughly two evenings for accuracy no measure in the model consumes: every metric here is a duration or a delay, both differences within the same frame.

**Known limitation to state in the README:** arrival timestamps are destination-local wall clock with a date derived from origin-local arithmetic. A small residual exists where the arithmetic date lands on the other side of midnight from the destination-local date. Timezone-correct conversion is a documented stretch item, not an oversight.

`2400` handling **differs between the two functions** and this is deliberate: on the departure side the base date has not moved, so a day is added; on the arrival side the arithmetic has already crossed midnight, so it is not. Applying the departure rule to arrivals produced 532 rows landing two days out. The docstrings say so explicitly, to stop the inconsistency being "fixed" back into existence.

### Validation gates

`validate_timestamps()` runs two **independent** checks and raises on failure:

1. Derived clock matches the BTS reported time of day.
2. Arrival date is the flight date or the day after — never earlier, never two days later.

The 2400 bug passed check 1 at 100% and was caught only by check 2. **One passing check is not validation.**

The `raise` matters: when the pipeline runs unattended, a bad month must fail the run rather than quietly write wrong data.

### Data quality items — status

1. **Times are `hhmm` without leading zeros.** Handled by arithmetic split.
2. **`2400` for midnight.** Handled, with the departure/arrival distinction above.
3. **No arrival date supplied.** Handled by the hybrid approach.
4. **Cancelled flights null across actual-time fields.** Preserved. Nulls propagate through the arithmetic without special-casing.
5. **Delay causes null unless `ArrDel15 = 1`.** Not coalesced. Measured at under 3% populated for April 2020.
6. **Header casing varies by vintage.** Normalised to snake_case in silver.

**New finding, not in the original list:** a cancelled flight can have a non-null `DepTime`. The aircraft pushed back from the gate and the flight was then cancelled; `ArrTime` and `ActualElapsedTime` remain null. Any logic assuming `Cancelled = 1` implies a null departure is wrong. The arithmetic handles it correctly without a special case, because elapsed time is null.

---

## Incremental load design

**Business key:**

`flight_date` + `carrier_code` + `flight_number` + `origin_airport_id` + `dest_airport_id` + `crs_dep_time_hhmm`

Note the actual silver column name is `flight_number` (from `Flight_Number_Reporting_Airline`), not `Flight_Number`.

**Validated unique across 1,497,990 rows** spanning January 2020, April 2020 and June 2023 — zero duplicates. Three months is not 77, so the check stays in the load path as a permanent gate rather than a one-off.

The gold facts use `date_key` in place of `flight_date`; `fact_delay_attribution` adds `delay_cause_key`.

**Merge condition uses `<=>`, not `=`.** Null-safe equality. Standard `=` returns null when either side is null, so a row with a null key column would never match itself and would insert a duplicate on **every** run.

**Watermark table** `load_watermark`: `(year, month, source_file, row_count, status, loaded_at)`. Itself upserted on `(year, month)`, so re-running a period updates its record rather than adding a second one — "what is the last successful period" stays a trivial query.

**Idempotency proven**, not asserted: re-running June 2023 through the full silver notebook left `silver_flights` at exactly 1,497,990 rows, all three file counts unchanged, and the watermark at three rows. Screenshot before and after.

### Backfill

**Range: January 2020 through May 2026.** 77 months. All 77 zips validated locally — correct count, none undersized, all structurally valid, **full CRC deep check passed**. Actual total **1.92 GB**, not the 2.3 GB previously estimated; the difference is the 2020 collapse months, which are markedly smaller (April 2020 is 12.28 MB against a 27.12 MB median).

Validation script: `scripts/Verify-BtsZips.ps1`. Run `-DeepCheck` for CRC verification.

**Hold 2026_6 back from the backfill.** Loading it afterwards is the live demonstration that the incremental pipeline works. Screenshot the watermark table before and after. The July file should publish around September 2026 — inside the trial window — giving a second genuine incremental run against a month that did not exist when the pipeline was built.

**Development slice: 2020-01, 2020-04, 2023-06.** Deliberately non-consecutive, one per failure mode:
- **2020-01** — normal month, full schedule, 607,346 rows. Baseline and the honest performance measurement.
- **2020-04** — the stress case. 313,382 rows, 41.5% cancelled. Breaks loudly rather than subtly.
- **2023-06** — recovered month three years later, so schema drift between vintages would surface. Also heavy delay-cause population, the opposite of April.

**Known gap:** the slice cannot test sequential watermark logic, since the months are not consecutive. Cover this with two adjacent months when the pipeline is built.

---

## Evidence checklist

Fabric Git sync does not capture table contents, so these screenshots are the only proof once capacity expires. Store in `docs/`.

Captured or to capture:
- [ ] Workspace Git integration panel showing repo, branch `main`, folder `/fabric`
- [ ] Workspace header with Source control button
- [ ] Lakehouse explorer showing bronze, silver and gold tables
- [ ] `nb_02` `_source_file` summary — audit columns working
- [ ] `nb_03` validation gate output — both timestamp checks passing
- [ ] `nb_04` full verification block — table counts, period breakdown, five zero-orphan integrity checks
- [ ] Watermark table before and after the incremental run of 2026_6
- [ ] Pipeline run history in the Monitoring hub
- [ ] Direct Lake semantic model and the report page

---

## Remaining work

1. **Data Factory pipeline.** Chain the four notebooks, parameterised by period, with watermark lookup, failure path, and schedule. Test sequential loading with two adjacent months.
2. **Full backfill.** 74 remaining months. Projects to ~68 minutes of fetch plus transform time. Run as a background pipeline execution.
3. **Load 2026_6** as the live incremental demonstration.
4. **Semantic model.** Direct Lake over gold. Two relationships to `dim_airport` (active origin, inactive destination) with `USERELATIONSHIP` measures.
5. **Report scope — still open.** One page, no more. Candidate: carrier on-time performance with delay-cause breakdown and route-level drill, with the 2020 collapse and recovery as the narrative spine.
6. **README** with architecture diagram, the decisions above, and the measured figures.
7. **Consolidate `nb_01` and `nb_02`** if time allows — both are currently clean but were built incrementally.

---

## Project instructions

> Technical build project. I am a BI developer with 12 years of experience: strong SQL Server, T-SQL, DAX, Power Query and Power BI. Microsoft Fabric, PySpark and lakehouse architecture are newer ground for me, so pitch explanations accordingly — no need to explain SQL or dimensional modelling fundamentals, do explain Spark and Fabric specifics. This project is a portfolio piece built on evenings alongside a job search, so favour working solutions over exhaustive ones. See the handover document for scope and decisions.

Do not add the CV or job-search context yet. Add it later only if you want README wording and the announcement post drafted to match CV language.

---

## Planned second pass

After this build completes, repeat the notebook layer at very small scope — one month, possibly one carrier, in a scratch workspace, no Git sync — working through each transform slowly rather than following instructions. Workspace setup, Git integration, lakehouse structure and dimensional modelling do not need repeating; the gap is Spark and notebook mechanics.

Keep a running list of questions in `docs/` as they arise during this build. That list is the syllabus for the second pass, and it is far easier to write down now than to reconstruct weeks later.

---

## Deferred

- **Timezone-correct arrival timestamps** (option C above). Needs an airport-to-timezone reference source and DST-aware conversion.
- **Type 2 SCD on `AirportSeqID`.** `origin_airport_seq_id` and `dest_airport_seq_id` are already retained in silver.
- **`DimTimeBlock`** from the retained time-block columns.
- **Eventstream + Eventhouse** for flight-status events, forcing KQL practice as DP-700 preparation. Only after the core pipeline runs reliably. DP-700 itself is deferred until re-employed.

---

## Repo housekeeping (separate from this project)

Existing Olist READMEs reference "9+ years" and PL-300 only. Update to 12+ years and add DP-600.
