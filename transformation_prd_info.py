# transformation_prd_info.py
from __future__ import annotations

import pandas as pd
from db_connection import get_connection

DB_NAME = "DWH"
SRC_SCHEMA = "ingestion"
TGT_SCHEMA = "transformation"
SRC_TABLE = "prd_info"
TGT_TABLE = "prd_info"


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
    # Keep everything close to your needs: store end date as text so "NAT" can be stored
    sql = f"""
    CREATE TABLE [{schema}].[{table}] (
        prd_id       INT            NULL,
        prd_key      NVARCHAR(100)  NULL,
        prd_cat5     NVARCHAR(5)    NULL,
        prd_key_rest NVARCHAR(200)  NULL,
        prd_nm       NVARCHAR(255)  NULL,
        prd_cost     DECIMAL(18,2)  NULL,
        prd_line     NVARCHAR(50)   NULL,
        prd_start_dt NVARCHAR(50)   NULL,
        prd_end_dt   NVARCHAR(50)   NULL
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

    # ---- keep your previous required transforms ----
    # prd_key split + '-' -> '_' in prd_cat5
    df["prd_key"] = df["prd_key"].astype("string")
    df["prd_cat5"] = df["prd_key"].str.slice(0, 5).str.replace("-", "_", regex=False)
    df["prd_key_rest"] = df["prd_key"].str.slice(5).str.lstrip("-")

    # prd_cost NULL -> 0
    df["prd_cost"] = pd.to_numeric(df["prd_cost"], errors="coerce").fillna(0)

    # prd_line mapping + NULL -> Not Available
    line_map = {"M": "Mountain", "S": "Sport", "T": "Touring", "R": "Road",
                "m": "Mountain", "s": "Sport", "t": "Touring", "r": "Road"}
    df["prd_line"] = df["prd_line"].astype("string").str.strip()
    df["prd_line"] = df["prd_line"].map(line_map).fillna("Not Available")

    # ---- YOUR REQUIRED DATE LOGIC (integrated exactly) ----
    df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors="coerce")
    df["prd_end_dt"] = pd.to_datetime(df["prd_end_dt"], errors="coerce")

    df = df.sort_values(by=["prd_key", "prd_id"]).copy()

    df["prd_end_dt"] = (
        df.groupby("prd_key")["prd_start_dt"].shift(-1) - pd.Timedelta(days=1)
    )

    df = df.sort_values(by=["prd_id"]).copy()

    df["prd_end_dt"] = df["prd_end_dt"].dt.strftime("%Y-%m-%d")
    df["prd_end_dt"] = df["prd_end_dt"].fillna("NAT")

    # store start date as yyyy-mm-dd too (consistent)
    df["prd_start_dt"] = df["prd_start_dt"].dt.strftime("%Y-%m-%d")
    df["prd_start_dt"] = df["prd_start_dt"].fillna("NAT")

    return df


def insert_df(schema: str, table: str, df: pd.DataFrame) -> None:
    cols = [
        "prd_id", "prd_key", "prd_cat5", "prd_key_rest",
        "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"
    ]
    df = df[cols].copy()

    insert_sql = f"""
    INSERT INTO [{schema}].[{table}]
    ([prd_id],[prd_key],[prd_cat5],[prd_key_rest],[prd_nm],
     [prd_cost],[prd_line],[prd_start_dt],[prd_end_dt])
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
    df2 = transform(df)

    ensure_schema(TGT_SCHEMA)
    drop_table_if_exists(TGT_SCHEMA, TGT_TABLE)
    create_table(TGT_SCHEMA, TGT_TABLE)
    insert_df(TGT_SCHEMA, TGT_TABLE, df2)

    print("Done.")


if __name__ == "__main__":
    main()