# BTS Airline On-Time Performance - Microsoft Fabric Lakehouse

A medallion-architecture data engineering project in Microsoft Fabric, built over
US Bureau of Transportation Statistics flight data.

**Scope:** January 2020 - May 2026, 77 monthly files, roughly 35 million flight records.

Demonstrates lakehouse architecture, PySpark transformation, incremental loading with
watermark-driven MERGE, and Data Factory pipeline orchestration.

## Status

Build in progress. Source data validated; Fabric workspace provisioned on trial capacity.

## Repository layout

- `scripts/` - local utility scripts (source data validation)
- `fabric/` - Fabric workspace items, synced via Git integration
- `docs/` - architecture diagram and build evidence

## Source data

US Department of Transportation, Bureau of Transportation Statistics -
Reporting Carrier On-Time Performance, obtained from TranStats.
Raw archives are not committed to this repository.
