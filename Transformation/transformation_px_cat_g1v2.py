# transformation_px_cat_g1v2.py
from __future__ import annotations

import pandas as pd
from db_connection import get_connection

DB_NAME = "DWH"
SRC_SCHEMA = "ingestion"
TGT_SCHEMA = "transformation"
SRC_TABLE = "px_cat_g1v2"
TGT_TABLE = "px_cat_g1v2"


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
        ID          NVARCHAR(100)  NULL,
        CAT         NVARCHAR(100)  NULL,
        SUBCAT      NVARCHAR(100)  NULL,
        MAINTENANCE NVARCHAR(100)  NULL
    );
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


def load_ingestion() -> pd.DataFrame:
    query = f"SELECT * FROM {SRC_SCHEMA}.{SRC_TABLE};"
    with get_connection(DB_NAME) as conn:
        return pd.read_sql(query, conn)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) Trim whitespace on all string columns
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string").str.strip()

    # 2) Replace empty strings with NA
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # 3) Remove rows without ID (primary identifier)
    df = df[df["ID"].notna()].copy()

    # 4) Validate: CAT and SUBCAT should be present
    missing_cat = df["CAT"].isna().sum()
    missing_subcat = df["SUBCAT"].isna().sum()
    if missing_cat > 0:
        print(f"Warning: {missing_cat} rows have null CAT")
    if missing_subcat > 0:
        print(f"Warning: {missing_subcat} rows have null SUBCAT")

    # 5) Validate MAINTENANCE values (should be Yes/No)
    valid_maint = {"Yes", "No"}
    invalid_maint = df["MAINTENANCE"].notna() & ~df["MAINTENANCE"].isin(valid_maint)
    if invalid_maint.sum() > 0:
        print(f"Warning: {invalid_maint.sum()} rows have invalid MAINTENANCE values")

    # 6) Remove duplicate IDs
    df = df.drop_duplicates(subset=["ID"], keep="first")

    return df


def insert_df(schema: str, table: str, df: pd.DataFrame) -> None:
    cols = ["ID", "CAT", "SUBCAT", "MAINTENANCE"]
    df = df[cols].copy()

    insert_sql = f"""
    INSERT INTO [{schema}].[{table}]
    ([ID],[CAT],[SUBCAT],[MAINTENANCE])
    VALUES (?,?,?,?)
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
