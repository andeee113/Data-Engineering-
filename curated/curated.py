# curated.py
from __future__ import annotations

import pandas as pd
from db_connection import get_connection

DB_NAME = "DWH"
SRC_SCHEMA = "transformation"
TGT_SCHEMA = "curated"


# ── helpers ──────────────────────────────────────────────────────────────

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
        conn.cursor().execute(sql)


def drop_table_if_exists(schema: str, table: str) -> None:
    sql = f"""
    IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL
        DROP TABLE [{schema}].[{table}];
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


# ── dim_customer ─────────────────────────────────────────────────────────

def build_dim_customer() -> pd.DataFrame:
    print("Loading transformation tables for dim_customer ...")
    cust_info = load_table(SRC_SCHEMA, "cust_info")
    cust_az12 = load_table(SRC_SCHEMA, "cust_az12")
    loc_a101 = load_table(SRC_SCHEMA, "loc_a101")

    print(f"  cust_info:  {cust_info.shape}")
    print(f"  cust_az12:  {cust_az12.shape}")
    print(f"  loc_a101:   {loc_a101.shape}")

    # 1) cust_info LEFT JOIN cust_az12 on cst_key = CID
    merged = cust_info.merge(cust_az12, how="left", left_on="cst_key", right_on="CID")

    # 2) LEFT JOIN loc_a101 on cst_key = CID
    merged = merged.merge(loc_a101, how="left", left_on="cst_key", right_on="CID",
                          suffixes=("", "_loc"))

    # 3) Rename to clean dimension columns
    dim = merged.rename(columns={
        "cst_id":             "customer_id",
        "cst_key":            "customer_key",
        "cst_firstname":      "first_name",
        "cst_lastname":       "last_name",
        "cst_marital_status": "marital_status",
        "cst_gndr":           "gender",
        "cst_create_date":    "create_date",
        "BDATE":              "birth_date",
        "GEN":                "gender_erp",
        "CNTRY":              "country",
    })

    # 4) Drop redundant join columns
    cols_to_drop = [c for c in dim.columns if c in ("CID", "CID_loc")]
    dim = dim.drop(columns=cols_to_drop)

    # 5) Add surrogate key (auto-increment starting at 1)
    dim.insert(0, "customer_sk", range(1, len(dim) + 1))

    print(f"  dim_customer built: {dim.shape}")
    return dim


def create_dim_customer_table() -> None:
    sql = f"""
    CREATE TABLE [{TGT_SCHEMA}].[dim_customer] (
        customer_sk      INT            NOT NULL,
        customer_id      INT            NULL,
        customer_key     NVARCHAR(50)   NULL,
        first_name       NVARCHAR(100)  NULL,
        last_name        NVARCHAR(100)  NULL,
        marital_status   NVARCHAR(20)   NULL,
        gender           NVARCHAR(10)   NULL,
        create_date      NVARCHAR(50)   NULL,
        birth_date       NVARCHAR(50)   NULL,
        gender_erp       NVARCHAR(10)   NULL,
        country          NVARCHAR(100)  NULL,
        CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
    );
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


def insert_dim_customer(df: pd.DataFrame) -> None:
    cols = [
        "customer_sk", "customer_id", "customer_key", "first_name", "last_name",
        "marital_status", "gender", "create_date", "birth_date", "gender_erp", "country",
    ]
    df = df[cols].copy()
    col_list = ", ".join([f"[{c}]" for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{TGT_SCHEMA}].[dim_customer] ({col_list}) VALUES ({placeholders})"

    df2 = df.astype(object).where(pd.notna(df), None)
    rows = [tuple(r) for r in df2.itertuples(index=False, name=None)]

    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)

    print(f"  Inserted {len(df):,} rows into {TGT_SCHEMA}.dim_customer")


# ── dim_product ──────────────────────────────────────────────────────────

def build_dim_product() -> pd.DataFrame:
    print("Loading transformation tables for dim_product ...")
    prd_info = load_table(SRC_SCHEMA, "prd_info")
    px_cat = load_table(SRC_SCHEMA, "px_cat_g1v2")

    print(f"  prd_info:    {prd_info.shape}")
    print(f"  px_cat_g1v2: {px_cat.shape}")

    # 1) prd_info LEFT JOIN px_cat_g1v2 on prd_cat5 = ID
    merged = prd_info.merge(px_cat, how="left", left_on="prd_cat5", right_on="ID")

    # 2) Rename to clean dimension columns
    dim = merged.rename(columns={
        "prd_id":       "product_id",
        "prd_key":      "product_key",
        "prd_nm":       "product_name",
        "prd_cost":     "product_cost",
        "prd_line":     "product_line",
        "prd_start_dt": "start_date",
        "prd_end_dt":   "end_date",
        "CAT":          "category",
        "SUBCAT":       "subcategory",
        "MAINTENANCE":  "maintenance",
    })

    # 3) Drop redundant join columns (keep prd_key_rest for fact FK lookup, drop later)
    cols_to_drop = [c for c in dim.columns if c in ("prd_cat5", "ID")]
    dim = dim.drop(columns=cols_to_drop)

    # 4) Add surrogate key
    dim.insert(0, "product_sk", range(1, len(dim) + 1))

    print(f"  dim_product built: {dim.shape}")
    return dim


def create_dim_product_table() -> None:
    sql = f"""
    CREATE TABLE [{TGT_SCHEMA}].[dim_product] (
        product_sk     INT            NOT NULL,
        product_id     INT            NULL,
        product_key    NVARCHAR(100)  NULL,
        product_name   NVARCHAR(255)  NULL,
        product_cost   DECIMAL(18,2)  NULL,
        product_line   NVARCHAR(50)   NULL,
        start_date     NVARCHAR(50)   NULL,
        end_date       NVARCHAR(50)   NULL,
        category       NVARCHAR(100)  NULL,
        subcategory    NVARCHAR(100)  NULL,
        maintenance    NVARCHAR(100)  NULL,
        CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
    );
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


def insert_dim_product(df: pd.DataFrame) -> None:
    # prd_key_rest is kept in the DataFrame for fact FK lookup but NOT stored in DB
    cols = [
        "product_sk", "product_id", "product_key", "product_name", "product_cost",
        "product_line", "start_date", "end_date", "category", "subcategory", "maintenance",
    ]
    df = df[cols].copy()
    col_list = ", ".join([f"[{c}]" for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{TGT_SCHEMA}].[dim_product] ({col_list}) VALUES ({placeholders})"

    df2 = df.astype(object).where(pd.notna(df), None)
    rows = [tuple(r) for r in df2.itertuples(index=False, name=None)]

    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)

    print(f"  Inserted {len(df):,} rows into {TGT_SCHEMA}.dim_product")


# ── fact_sales ───────────────────────────────────────────────────────────

def build_fact_sales(dim_customer: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:
    print("Loading transformation.sales_details for fact_sales ...")
    sales = load_table(SRC_SCHEMA, "sales_details")
    print(f"  sales_details: {sales.shape}")

    # 1) Build FK lookup: sls_cust_id -> customer_sk
    #    sales_details.sls_cust_id matches dim_customer.customer_id
    cust_lookup = dim_customer[["customer_sk", "customer_id"]].copy()
    cust_lookup["customer_id"] = pd.to_numeric(cust_lookup["customer_id"], errors="coerce")
    sales["sls_cust_id"] = pd.to_numeric(sales["sls_cust_id"], errors="coerce")

    fact = sales.merge(cust_lookup, how="left", left_on="sls_cust_id", right_on="customer_id")

    # 2) Build FK lookup: sls_prd_key -> product_sk
    #    sales_details.sls_prd_key matches dim_product.prd_key_rest (the product code without category prefix)
    prod_lookup = dim_product[["product_sk", "prd_key_rest"]].copy()

    fact = fact.merge(prod_lookup, how="left", left_on="sls_prd_key", right_on="prd_key_rest")

    # 3) Warn on unmatched FKs
    null_cust = int(fact["customer_sk"].isna().sum())
    null_prod = int(fact["product_sk"].isna().sum())
    if null_cust > 0:
        print(f"  Warning: {null_cust} sales rows have no matching customer")
    if null_prod > 0:
        print(f"  Warning: {null_prod} sales rows have no matching product")

    # 4) Rename and select final columns
    fact = fact.rename(columns={
        "customer_sk":  "customer_key",
        "product_sk":   "product_key",
        "sls_ord_num":  "order_number",
        "sls_order_dt": "order_date",
        "sls_ship_dt":  "ship_date",
        "sls_due_dt":   "due_date",
        "sls_sales":    "sales_amount",
        "sls_quantity":  "quantity",
        "sls_price":    "unit_price",
    })

    fact = fact[[
        "order_number", "product_key", "customer_key",
        "order_date", "ship_date", "due_date",
        "sales_amount", "quantity", "unit_price",
    ]]

    print(f"  fact_sales built: {fact.shape}")
    return fact


def create_fact_sales_table() -> None:
    sql = f"""
    CREATE TABLE [{TGT_SCHEMA}].[fact_sales] (
        order_number   NVARCHAR(50)   NULL,
        product_key    INT            NULL,
        customer_key   INT            NULL,
        order_date     DATE           NULL,
        ship_date      DATE           NULL,
        due_date       DATE           NULL,
        sales_amount   DECIMAL(18,2)  NULL,
        quantity       INT            NULL,
        unit_price     DECIMAL(18,2)  NULL
    );
    """
    with get_connection(DB_NAME) as conn:
        conn.cursor().execute(sql)


def insert_fact_sales(df: pd.DataFrame) -> None:
    cols = [
        "order_number", "product_key", "customer_key",
        "order_date", "ship_date", "due_date",
        "sales_amount", "quantity", "unit_price",
    ]
    df = df[cols].copy()
    col_list = ", ".join([f"[{c}]" for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{TGT_SCHEMA}].[fact_sales] ({col_list}) VALUES ({placeholders})"

    df2 = df.astype(object).where(pd.notna(df), None)
    rows = [tuple(r) for r in df2.itertuples(index=False, name=None)]

    with get_connection(DB_NAME) as conn:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)

    print(f"  Inserted {len(df):,} rows into {TGT_SCHEMA}.fact_sales")


# ── main ─────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("CURATED LAYER")
    print("=" * 60)

    ensure_schema(TGT_SCHEMA)

    # ── dim_customer ──
    print("\n--- dim_customer ---")
    dim_cust = build_dim_customer()
    drop_table_if_exists(TGT_SCHEMA, "dim_customer")
    create_dim_customer_table()
    insert_dim_customer(dim_cust)

    # ── dim_product ──
    print("\n--- dim_product ---")
    dim_prod = build_dim_product()
    drop_table_if_exists(TGT_SCHEMA, "dim_product")
    create_dim_product_table()
    insert_dim_product(dim_prod)

    # ── fact_sales (needs both dimensions for FK lookups) ──
    print("\n--- fact_sales ---")
    fact = build_fact_sales(dim_cust, dim_prod)
    drop_table_if_exists(TGT_SCHEMA, "fact_sales")
    create_fact_sales_table()
    insert_fact_sales(fact)

    print("\n" + "=" * 60)
    print("CURATED LAYER COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
