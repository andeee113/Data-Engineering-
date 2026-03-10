from __future__ import annotations

import pandas as pd
from db_connection import get_connection


INGESTION_SCHEMA = "ingestion"
TARGET_SCHEMA = "transformation"
DB_NAME = "DWH"


def load_table(schema: str, table: str) -> pd.DataFrame:
    query = f"SELECT * FROM {schema}.{table};"
    with get_connection(DB_NAME) as conn:
        return pd.read_sql(query, conn)


def ensure_schema(schema: str) -> None:
    sql = f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
    EXEC('CREATE SCHEMA [{schema}]');
    """
    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.execute(sql)


def drop_table_if_exists(schema: str, table: str) -> None:
    sql = f"""
    IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL
        DROP TABLE [{schema}].[{table}];
    """
    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.execute(sql)


def create_table_all_text(schema: str, table: str, columns: list[str]) -> None:
    # Create a table where all columns are NVARCHAR(MAX), similar to your PostgreSQL "all TEXT"
    cols_sql = ",\n        ".join([f"[{c}] NVARCHAR(MAX) NULL" for c in columns])
    sql = f"""
    CREATE TABLE [{schema}].[{table}] (
        {cols_sql}
    );
    """
    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.execute(sql)


def insert_dataframe(schema: str, table: str, df: pd.DataFrame) -> None:
    # Convert everything to strings, and keep "NA" exactly like your example
    df_to_save = df.copy()

    # Replace NaN/NaT with "NA"
    df_to_save = df_to_save.fillna("NA")

    # Ensure python-native objects (avoid numpy.int64 issues)
    df_to_save = df_to_save.astype(object)

    cols = list(df_to_save.columns)
    col_list = ", ".join([f"[{c}]" for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{schema}].[{table}] ({col_list}) VALUES ({placeholders})"

    rows = [tuple(row) for row in df_to_save.itertuples(index=False, name=None)]

    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)

    print(f"Inserted {len(df_to_save):,} rows into {schema}.{table}")


def clean_cust_info(cust_info: pd.DataFrame) -> pd.DataFrame:
    cust_info_clean = cust_info.copy()

    # 1) Replace empty strings / whitespace-only with NA
    cust_info_clean = cust_info_clean.replace(r"^\s*$", pd.NA, regex=True)

    # 2) Trim spaces
    for col in ["cst_firstname", "cst_lastname", "cst_key"]:
        if col in cust_info_clean.columns:
            cust_info_clean[col] = cust_info_clean[col].astype("string").str.strip()

    # 3-4) Keep only rows where ID and key are present
    cust_info_clean = cust_info_clean[
        cust_info_clean["cst_id"].notna() & cust_info_clean["cst_key"].notna()
    ].copy()

    # 5-7) Validate cst_key == "AW000" + cst_id
    # Make cst_id safe as string (handles Int64 dtype)
    expected_key = "AW000" + cust_info_clean["cst_id"].astype("Int64").astype("string")
    cust_info_clean = cust_info_clean[cust_info_clean["cst_key"] == expected_key].copy()

    # 8) Remove duplicate customer IDs
    cust_info_clean = cust_info_clean.drop_duplicates(subset=["cst_id"], keep="first")

    # 9) Replace marital status & gender codes
    marital_map = {"M": "Married", "S": "Single"}
    gender_map = {"M": "Male", "F": "Female"}

    if "cst_marital_status" in cust_info_clean.columns:
        cust_info_clean["cst_marital_status"] = cust_info_clean["cst_marital_status"].replace(marital_map)
    if "cst_gndr" in cust_info_clean.columns:
        cust_info_clean["cst_gndr"] = cust_info_clean["cst_gndr"].replace(gender_map)

    # 10-11) Convert create date, find future dates (we just print count like a data quality check)
    if "cst_create_date" in cust_info_clean.columns:
        converted = pd.to_datetime(cust_info_clean["cst_create_date"], errors="coerce")
        future_mask = converted.notna() & (converted.dt.date > pd.Timestamp.today().date())
        future_count = int(future_mask.sum())
        if future_count > 0:
            print(f"Warning: {future_count} rows have future cst_create_date")

    return cust_info_clean


def main() -> None:
    print("Loading ingestion.cust_info ...")
    cust_info = load_table(INGESTION_SCHEMA, "cust_info")
    print(f"Loaded cust_info: {cust_info.shape}")

    print("Cleaning cust_info ...")
    cust_info_clean = clean_cust_info(cust_info)
    print(f"After cleaning: {cust_info_clean.shape}")

    # Save like your script: "NA" visible everywhere
    cust_info_to_save = cust_info_clean.fillna("NA")

    print("Ensuring schema transformation ...")
    ensure_schema(TARGET_SCHEMA)

    print("Recreating transformation.cust_info ...")
    drop_table_if_exists(TARGET_SCHEMA, "cust_info")
    create_table_all_text(TARGET_SCHEMA, "cust_info", list(cust_info_to_save.columns))

    print("Inserting cleaned data ...")
    insert_dataframe(TARGET_SCHEMA, "cust_info", cust_info_to_save)

    print("Done.")


if __name__ == "__main__":
    main()