from pathlib import Path
import pandas as pd
from db_connection import get_connection

def _clean_str_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype("string").str.strip()
            df[c] = df[c].replace({"": None, "NULL": None, "null": None, "None": None})
    return df

def _to_date(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

def _to_datetime(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

def _to_int(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

def _to_decimal(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

def load_dataframe_to_table(df: pd.DataFrame, full_table_name: str, truncate: bool = True) -> None:
    df = _clean_str_cols(df)
    cols = list(df.columns)

    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join([f"[{c}]" for c in cols])
    insert_sql = f"INSERT INTO {full_table_name} ({col_list}) VALUES ({placeholders})"

    df = df.astype(object).where(pd.notna(df), None)
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with get_connection("DWH") as conn:
        cur = conn.cursor()
        cur.fast_executemany = True

        if truncate:
            cur.execute(f"TRUNCATE TABLE {full_table_name};")

        cur.executemany(insert_sql, rows)

    print(f"Loaded {len(df):,} rows into {full_table_name}")

def ingest_cust_info(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    _to_int(df, "cst_id")
    _to_datetime(df, "cst_create_date")
    load_dataframe_to_table(df, "ingestion.cust_info", truncate=True)

def ingest_prd_info(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    _to_int(df, "prd_id")
    _to_decimal(df, "prd_cost")
    _to_date(df, "prd_start_dt")
    _to_date(df, "prd_end_dt")
    load_dataframe_to_table(df, "ingestion.prd_info", truncate=True)

def ingest_sales_details(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    _to_int(df, "sls_cust_id")
    _to_int(df, "sls_order_dt")
    _to_int(df, "sls_ship_dt")
    _to_int(df, "sls_due_dt")
    _to_int(df, "sls_quantity")
    _to_decimal(df, "sls_sales")
    _to_decimal(df, "sls_price")
    load_dataframe_to_table(df, "ingestion.sales_details", truncate=True)

def ingest_cust_az12(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    load_dataframe_to_table(df, "ingestion.cust_az12", truncate=True)

def ingest_loc_a101(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    load_dataframe_to_table(df, "ingestion.loc_a101", truncate=True)

def ingest_px_cat_g1v2(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    load_dataframe_to_table(df, "ingestion.px_cat_g1v2", truncate=True)

if __name__ == "__main__":
    DATA_DIR = Path(r"C:\Users\andre\Desktop\Data Engineering\datasets")

    ingest_cust_info(DATA_DIR / "cust_info.csv")
    ingest_prd_info(DATA_DIR / "prd_info.csv")
    ingest_sales_details(DATA_DIR / "sales_details.csv")
    ingest_cust_az12(DATA_DIR / "CUST_AZ12.csv")
    ingest_loc_a101(DATA_DIR / "LOC_A101.csv")
    ingest_px_cat_g1v2(DATA_DIR / "PX_CAT_G1V2.csv")