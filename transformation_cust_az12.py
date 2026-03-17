# transformation_cust_az12.py
from __future__ import annotations

import pandas as pd
from db_connection import get_connection

DB_NAME = "DWH"
SRC_SCHEMA = "ingestion"
TGT_SCHEMA = "transformation"
SRC_TABLE = "cust_az12"
TGT_TABLE = "cust_az12"


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
        CID   NVARCHAR(100)  NULL,
        BDATE DATE           NULL,
        GEN   NVARCHAR(10)   NULL
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

    # 1) Trim whitespace
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string").str.strip()

    # 2) Replace empty strings with NA
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # 3) Remove rows without CID
    df = df[df["CID"].notna()].copy()

    # 4) CID format: "NASAW000XXXXX" -> strip "NAS" prefix to get "AW000XXXXX"
    #    This aligns with cst_key in cust_info (e.g. AW00011000)
    df["CID"] = df["CID"].astype("string").str.replace("^NAS", "", regex=True)

    # 5) Validate and parse birth date
    df["BDATE"] = pd.to_datetime(df["BDATE"], errors="coerce")

    # 6) Check for future birth dates
    future_mask = df["BDATE"].notna() & (df["BDATE"].dt.date > pd.Timestamp.today().date())
    if future_mask.sum() > 0:
        print(f"Warning: {future_mask.sum()} rows have future BDATE")

    # 7) Normalize GEN: M/F -> Male/Female for consistency with cust_info (cst_gndr)
    gender_map = {"M": "Male", "F": "Female"}
    df["GEN"] = df["GEN"].replace(gender_map)

    # 8) Remove duplicates on CID
    df = df.drop_duplicates(subset=["CID"], keep="first")

    # 9) Format date for storage
    df["BDATE"] = df["BDATE"].dt.strftime("%Y-%m-%d")

    return df


def insert_df(schema: str, table: str, df: pd.DataFrame) -> None:
    cols = ["CID", "BDATE", "GEN"]
    df = df[cols].copy()

    insert_sql = f"""
    INSERT INTO [{schema}].[{table}]
    ([CID],[BDATE],[GEN])
    VALUES (?,?,?)
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
