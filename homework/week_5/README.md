# Homework Week 5: Data Pipelines with Bruin

## Setup

- Built an ELT pipeline using [Bruin](https://github.com/bruin-data/bruin/tree/main/templates/zoomcamp) to ingest NYC TLC taxi data (yellow + green, 2019-01 to 2025-11) into BigQuery.
- Three-layer architecture: ingestion (Python + seed CSV) → staging (SQL) → reports (SQL).
- 315M+ rows loaded into `ingestion.trips`, deduplicated in staging, aggregated in reports.

## Question 1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

**Answer:** .bruin.yml and pipeline/ with pipeline.yml and assets/

Per the docs: `.bruin.yml` in the root, `pipeline.yml` in the `pipeline/` directory, and `assets/` folder next to it.

## Question 2. Materialization Strategies

Which incremental strategy deletes and re-inserts data for a specific time period?

**Answer:** time_interval - incremental based on a time column

It deletes rows in the date range then re-inserts. `append` doesn't delete, `replace` rebuilds everything, `view` stores nothing.

## Question 3. Pipeline Variables

How do you override the `taxi_types` array variable to only process yellow taxis?

**Answer:** bruin run --var 'taxi_types=["yellow"]'

The `--var` flag takes `key=value` format. Since `taxi_types` is an array, the value must be a JSON array. The docs example shows: `--var taxi_types='["yellow"]'`.

## Question 4. Running with Dependencies

How to run `ingestion/trips.py` plus all downstream assets?

**Answer:** bruin run ingestion/trips.py --downstream

The `--downstream` flag runs the asset and everything that depends on it. The other options (`--all`, `--recursive`, `--select`) don't exist in Bruin.

## Question 5. Quality Checks

Which quality check ensures `pickup_datetime` never has NULL values?

**Answer:** not_null: true

`unique` checks uniqueness, `positive`/`non_negative` check values, `accepted_values` checks against a list.

## Question 6. Lineage and Dependencies

Which command visualizes the dependency graph?

**Answer:** bruin lineage

Shows upstream and downstream dependencies for assets. `bruin lineage ./pipeline/assets/ingestion/trips.py`

## Question 7. First-Time Run

What flag to use on a new DuckDB database to create tables from scratch?

**Answer:** --full-refresh

Truncates and rebuilds tables. The docs recommend: "Use `--full-refresh` for initial runs on new databases." The other flags (`--create`, `--init`, `--truncate`) don't exist.
