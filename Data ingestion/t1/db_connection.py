import pyodbc

def get_connection(database: str = "master") -> pyodbc.Connection:
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    conn.autocommit = True  # needed for CREATE DATABASE
    return conn
import pyodbc

def get_connection(database: str = "master") -> pyodbc.Connection:
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    conn.autocommit = True
    return conn