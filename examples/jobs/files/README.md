# Orders Harmonization (`kind: DuckDBJob`)

An end-to-end, multi-source DuckDB job: read local Parquet, CSV and JSON files, join and normalize them in SQL, then
merge into a DuckDB warehouse table and export one Parquet file per country.

## What it shows
- **Typed inputs**: `spec.config.min_amount_usd` is a `number` with a default; `--set min_amount_usd=200` is
  converted and checked like any source input (unknown names are rejected).
- **File reads** through a `files` connection whose `roots` are relative to the job's folder (`data/`).
- **SQL steps**: a materialized temp table (`harmonized_orders`) reused by two consumers, and a view filtered with
  `{{ config.min_amount_usd }}`.
- **Outputs**: a `merge` on `primary_key` into the `fact_orders` table, and a `split_by` export named by the
  `file_name` template (`orders_{country}_{today}.parquet`, `{today}` being the batch date, so a replay overwrites
  the same files).

## Commands
```bash
streamwright validate examples/jobs/files/orders.yaml --graph   # no secrets needed
streamwright plan examples/jobs/files/orders.yaml
streamwright run examples/jobs/files/orders.yaml
streamwright run examples/jobs/files/orders.yaml \
  --overlay examples/jobs/files/orders.local.yaml --set min_amount_usd=200
streamwright duckdb state list examples/jobs/files/orders.yaml  # needs runtime.state (see ../README.md)
```

The warehouse (`/tmp/orders_analytics.duckdb`) and the export folder (`/tmp/orders_lake`) are set in `orders.yaml`;
change them with an overlay. Without `runtime.state` a run keeps no bookkeeping between runs; add
`runtime: {state: {connection: analytics_warehouse}}` to keep batches, receipts and leases in the warehouse's
`streamwright` schema.
