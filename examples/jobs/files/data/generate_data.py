#!/usr/bin/env python
import json
import os

import duckdb

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

def generate():
    con = duckdb.connect()

    # 1. customers.csv
    customers_csv = os.path.join(DATA_DIR, "customers.csv")
    con.execute("""
        COPY (
            SELECT 1 AS customer_id, 'Acme Corp' AS customer_name, 'enterprise' AS tier, 'US' AS country
            UNION ALL
            SELECT 2, 'Globex Inc', 'standard', 'CA'
            UNION ALL
            SELECT 3, 'Soylent Corp', 'premium', 'GB'
            UNION ALL
            SELECT 4, 'Initech', 'standard', 'US'
        ) TO '%s' (FORMAT CSV, HEADER)
    """ % customers_csv)

    # 2. orders.parquet
    orders_parquet = os.path.join(DATA_DIR, "orders.parquet")
    con.execute("""
        COPY (
            SELECT 1001 AS order_id, 1 AS customer_id, '2026-10-01'::DATE AS order_date, 500.0 AS amount,
                   'USD' AS currency
            UNION ALL
            SELECT 1002, 2, '2026-10-02'::DATE, 250.0, 'CAD'
            UNION ALL
            SELECT 1003, 3, '2026-10-03'::DATE, 150.0, 'GBP'
            UNION ALL
            SELECT 1004, 1, '2026-10-04'::DATE, 1200.0, 'USD'
            UNION ALL
            SELECT 1005, 4, '2026-10-05'::DATE, 45.0, 'USD'
        ) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)
    """ % orders_parquet)

    # 3. fx_rates.json
    fx_json = os.path.join(DATA_DIR, "fx_rates.json")
    rates = [
        {"currency": "USD", "rate_to_usd": 1.0},
        {"currency": "CAD", "rate_to_usd": 0.74},
        {"currency": "GBP", "rate_to_usd": 1.30}
    ]
    with open(fx_json, "w", encoding="utf-8") as f:
        json.dump(rates, f, indent=2)

    con.close()
    print("Sample datasets generated in %s" % DATA_DIR)

if __name__ == "__main__":
    generate()
