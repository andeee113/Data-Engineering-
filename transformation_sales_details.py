# transformation_sales_details.py
from __future__ import annotations

import pandas as pd
from db_connection import get_connection

DB_NAME = "DWH"
SRC_SCHEMA = "ingestion"
TGT_SCHEMA = "transformation"
SRC_TABLE = "sales_details"
TGT_TABLE = "sales_details"


def ensure_schema(schema: str) -> None:
    sql = f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
    EXEC('CREATE SCHEMA [{schema}]');
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


def drop_table_if_exists(schema: str, table: str) -> None:
    sql = f"""
    IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL
        DROP TABLE [{schema}].[{table}];
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


def create_table(schema: str, table: str) -> None:
    sql = f"""
    CREATE TABLE [{schema}].[{table}] (
        sls_ord_num   NVARCHAR(50)   NULL,
        sls_prd_key   NVARCHAR(100)  NULL,
        sls_cust_id   INT            NULL,
        sls_order_dt  DATE           NULL,
        sls_ship_dt   DATE           NULL,
        sls_due_dt    DATE           NULL,
        sls_sales     DECIMAL(18,2)  NULL,
        sls_quantity  INT            NULL,
        sls_price     DECIMAL(18,2)  NULL
    );
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


def load_ingestion() -> pd.DataFrame:
    query = f"SELECT * FROM {SRC_SCHEMA}.{SRC_TABLE};"
    with get_connection(DB_NAME) as conn:
        return pd.read_sql(query, conn)


def _int_to_date(series: pd.Series) -> pd.Series:
    """Convert YYYYMMDD integer to date. Invalid/0/null -> NaT."""
    s = pd.to_numeric(series, errors="coerce").astype("Int64")
    s = s.where(s > 0, pd.NA)
    return pd.to_datetime(s.astype("string"), format="%Y%m%d", errors="coerce")


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) Remove rows without order number
    df = df[df["sls_ord_num"].notna() & (df["sls_ord_num"].astype(str).str.strip() != "")].copy()

    # 2) Remove duplicate rows
    df = df.drop_duplicates()

    # ── DATE FORMATTING ──────────────────────────────────────────────
    # 3) Convert integer dates (YYYYMMDD) to proper dates; 0 / null -> NaT
    df["sls_order_dt"] = _int_to_date(df["sls_order_dt"])
    df["sls_ship_dt"] = _int_to_date(df["sls_ship_dt"])
    df["sls_due_dt"] = _int_to_date(df["sls_due_dt"])

    # 4) Fix missing order dates:
    #    - For multi-item orders (same sls_ord_num), copy the order_dt from
    #      siblings that DO have a valid date.
    #    - For single-item orders still missing, fall back to ship_dt.
    #    (This handles the rows where order_dt was stored as 0.)
    df["sls_order_dt"] = df.groupby("sls_ord_num")["sls_order_dt"].transform(
        lambda g: g.fillna(g.dropna().iloc[0]) if g.notna().any() else g
    )
    # Single-item orders (or groups where ALL were NaT): fall back to ship_dt
    still_missing = df["sls_order_dt"].isna()
    if still_missing.sum() > 0:
        print(f"Falling back to ship_dt for {still_missing.sum()} rows with no order_dt")
        df.loc[still_missing, "sls_order_dt"] = df.loc[still_missing, "sls_ship_dt"]

    # 5) Date validation warnings
    bad_ship = df["sls_ship_dt"].notna() & df["sls_order_dt"].notna() & (df["sls_ship_dt"] < df["sls_order_dt"])
    bad_due = df["sls_due_dt"].notna() & df["sls_order_dt"].notna() & (df["sls_due_dt"] < df["sls_order_dt"])
    if bad_ship.sum() > 0:
        print(f"Warning: {bad_ship.sum()} rows have ship_dt before order_dt")
    if bad_due.sum() > 0:
        print(f"Warning: {bad_due.sum()} rows have due_dt before order_dt")

    # ── PRICE / SALES / QUANTITY LOGIC ───────────────────────────────
    # 6) Ensure numeric & take absolute values (negative prices/sales are data errors)
    df["sls_quantity"] = pd.to_numeric(df["sls_quantity"], errors="coerce").astype("Int64")
    df["sls_sales"] = pd.to_numeric(df["sls_sales"], errors="coerce")
    df["sls_price"] = pd.to_numeric(df["sls_price"], errors="coerce")

    # If price is negative, take abs (data entry error, not a real negative price)
    neg_price = df["sls_price"].notna() & (df["sls_price"] < 0)
    if neg_price.sum() > 0:
        print(f"Fixing {neg_price.sum()} negative sls_price values (abs)")
        df.loc[neg_price, "sls_price"] = df.loc[neg_price, "sls_price"].abs()

    # If sales is negative, take abs
    neg_sales = df["sls_sales"].notna() & (df["sls_sales"] < 0)
    if neg_sales.sum() > 0:
        print(f"Fixing {neg_sales.sum()} negative sls_sales values (abs)")
        df.loc[neg_sales, "sls_sales"] = df.loc[neg_sales, "sls_sales"].abs()

    # 7) If sales is null/0 but qty and price are valid -> recalculate: sales = qty * price
    needs_recalc = (df["sls_sales"].isna() | (df["sls_sales"] == 0)) & df["sls_quantity"].notna() & df["sls_price"].notna()
    if needs_recalc.sum() > 0:
        print(f"Recalculating sls_sales for {needs_recalc.sum()} rows (qty * price)")
        df.loc[needs_recalc, "sls_sales"] = (
            df.loc[needs_recalc, "sls_quantity"].astype(float) * df.loc[needs_recalc, "sls_price"]
        )

    # 8) If price is null/0 but sales and qty are valid -> derive: price = sales / qty
    needs_price = (df["sls_price"].isna() | (df["sls_price"] == 0)) & df["sls_sales"].notna() & (df["sls_quantity"].notna() & (df["sls_quantity"] > 0))
    if needs_price.sum() > 0:
        print(f"Deriving sls_price for {needs_price.sum()} rows (sales / qty)")
        df.loc[needs_price, "sls_price"] = (
            df.loc[needs_price, "sls_sales"] / df.loc[needs_price, "sls_quantity"].astype(float)
        )

    # 9) If sales != qty * price after fixes, trust qty & price -> overwrite sales
    check = df["sls_quantity"].notna() & (df["sls_price"].notna()) & (df["sls_price"] > 0)
    expected_sales = df["sls_quantity"].astype(float) * df["sls_price"]
    mismatch = check & df["sls_sales"].notna() & ((df["sls_sales"] - expected_sales).abs() > 0.01)
    if mismatch.sum() > 0:
        print(f"Overwriting {mismatch.sum()} mismatched sls_sales (qty * price)")
        df.loc[mismatch, "sls_sales"] = expected_sales.loc[mismatch]

    # 10) Format dates as strings for storage
    for col in ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    return df


def insert_df(schema: str, table: str, df: pd.DataFrame) -> None:
    cols = [
        "sls_ord_num", "sls_prd_key", "sls_cust_id",
        "sls_order_dt", "sls_ship_dt", "sls_due_dt",
        "sls_sales", "sls_quantity", "sls_price"
    ]
    df = df[cols].copy()

    insert_sql = f"""
    INSERT INTO [{schema}].[{table}]
    ([sls_ord_num],[sls_prd_key],[sls_cust_id],
     [sls_order_dt],[sls_ship_dt],[sls_due_dt],
     [sls_sales],[sls_quantity],[sls_price])
    VALUES (?,?,?,?,?,?,?,?,?)
    """

    df2 = df.astype(object).where(pd.notna(df), None)
    rows = [tuple(r) for r in df2.itertuples(index=False, name=None)]

    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)

    print(f"Inserted {len(df):,} rows into {schema}.{table}")


def main() -> None:
    df = load_ingestion()
    print(f"Loaded {SRC_SCHEMA}.{SRC_TABLE}: {df.shape}")

    df2 = transform(df)
    print(f"After transform: {df2.shape}")

    ensure_schema(TGT_SCHEMA)
    drop_table_if_exists(TGT_SCHEMA, TGT_TABLE)
    create_table(TGT_SCHEMA, TGT_TABLE)
    insert_df(TGT_SCHEMA, TGT_TABLE, df2)

    print("Done.")


if __name__ == "__main__":
    main()
