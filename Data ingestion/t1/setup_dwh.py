from db_connection import get_connection

def create_database_if_not_exists(db_name: str) -> None:
    with get_connection("master") as conn:
        cur = conn.cursor()
        cur.execute(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE [{db_name}];")
        print(f"Database '{db_name}' created (or already exists).")

def create_schema_if_not_exists(db_name: str, schema_name: str) -> None:
    with get_connection(db_name) as conn:
        cur = conn.cursor()
        cur.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}')
            EXEC('CREATE SCHEMA [{schema_name}]');
        """)
        print(f"Schema '{schema_name}' created (or already exists) in '{db_name}'.")


# ---------- TABLES (INGESTION) ----------

def recreate_table_ingestion_cust_info() -> None:
    with get_connection("DWH") as conn:
        cur = conn.cursor()
        cur.execute("""
        IF OBJECT_ID('ingestion.cust_info', 'U') IS NOT NULL
            DROP TABLE ingestion.cust_info;

        CREATE TABLE ingestion.cust_info (
            cst_id             INT NULL,
            cst_key            NVARCHAR(100) NULL,
            cst_firstname      NVARCHAR(100) NULL,
            cst_lastname       NVARCHAR(100) NULL,
            cst_marital_status NVARCHAR(10)  NULL,
            cst_gndr           NVARCHAR(10)  NULL,
            cst_create_date    DATETIME2     NULL
        );
        """)
        print("Table ingestion.cust_info recreated.")

def recreate_table_ingestion_prd_info() -> None:
    with get_connection("DWH") as conn:
        cur = conn.cursor()
        cur.execute("""
        IF OBJECT_ID('ingestion.prd_info', 'U') IS NOT NULL
            DROP TABLE ingestion.prd_info;

        CREATE TABLE ingestion.prd_info (
            prd_id       INT           NOT NULL,
            prd_key      NVARCHAR(100) NULL,
            prd_nm       NVARCHAR(200) NULL,
            prd_cost     DECIMAL(18,2) NULL,
            prd_line     NVARCHAR(50)  NULL,
            prd_start_dt DATE          NULL,
            prd_end_dt   DATE          NULL,
            CONSTRAINT PK_ingestion_prd_info PRIMARY KEY (prd_id)
        );
        """)
        print("Table ingestion.prd_info recreated.")

def recreate_table_ingestion_sales_details() -> None:
    with get_connection("DWH") as conn:
        cur = conn.cursor()
        cur.execute("""
        IF OBJECT_ID('ingestion.sales_details', 'U') IS NOT NULL
            DROP TABLE ingestion.sales_details;

        CREATE TABLE ingestion.sales_details (
            sls_ord_num   NVARCHAR(50)  NULL,
            sls_prd_key   NVARCHAR(100) NULL,
            sls_cust_id   INT           NULL,
            sls_order_dt  INT           NULL,   -- în CSV e YYYYMMDD ca int
            sls_ship_dt   INT           NULL,
            sls_due_dt    INT           NULL,
            sls_sales     DECIMAL(18,2) NULL,
            sls_quantity  INT           NULL,
            sls_price     DECIMAL(18,2) NULL
        );
        """)
        print("Table ingestion.sales_details recreated.")

def recreate_table_ingestion_cust_az12() -> None:
    with get_connection("DWH") as conn:
        cur = conn.cursor()
        cur.execute("""
        IF OBJECT_ID('ingestion.cust_az12', 'U') IS NOT NULL
            DROP TABLE ingestion.cust_az12;

        CREATE TABLE ingestion.cust_az12 (
            CID   NVARCHAR(100) NULL,
            BDATE NVARCHAR(50)  NULL,
            GEN   NVARCHAR(10)  NULL
        );
        """)
        print("Table ingestion.cust_az12 recreated.")

def recreate_table_ingestion_loc_a101() -> None:
    with get_connection("DWH") as conn:
        cur = conn.cursor()
        cur.execute("""
        IF OBJECT_ID('ingestion.loc_a101', 'U') IS NOT NULL
            DROP TABLE ingestion.loc_a101;

        CREATE TABLE ingestion.loc_a101 (
            CID   NVARCHAR(100) NULL,
            CNTRY NVARCHAR(100) NULL
        );
        """)
        print("Table ingestion.loc_a101 recreated.")

def recreate_table_ingestion_px_cat_g1v2() -> None:
    with get_connection("DWH") as conn:
        cur = conn.cursor()
        cur.execute("""
        IF OBJECT_ID('ingestion.px_cat_g1v2', 'U') IS NOT NULL
            DROP TABLE ingestion.px_cat_g1v2;

        CREATE TABLE ingestion.px_cat_g1v2 (
            ID          NVARCHAR(100) NULL,
            CAT         NVARCHAR(100) NULL,
            SUBCAT      NVARCHAR(100) NULL,
            MAINTENANCE NVARCHAR(100) NULL
        );
        """)
        print("Table ingestion.px_cat_g1v2 recreated.")


if __name__ == "__main__":
    create_database_if_not_exists("DWH")
    create_schema_if_not_exists("DWH", "ingestion")

    recreate_table_ingestion_cust_info()
    recreate_table_ingestion_prd_info()
    recreate_table_ingestion_sales_details()
    recreate_table_ingestion_cust_az12()
    recreate_table_ingestion_loc_a101()
    recreate_table_ingestion_px_cat_g1v2()