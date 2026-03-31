"""
Cabinet Manufacturing - SSMS Extract Script
============================================
Pulls two tables from your SQL Server database and saves them as CSVs.
These CSVs are then loaded into Snowflake for the cost anomaly project.

Tables extracted:
  1. SAL_SALES_ORDER  --> sales_orders.csv
  2. BOM table        --> bom_data.csv  (update the table name below)

Requirements:
  pip install pyodbc pandas

How to run:
  python extract_to_csv.py

Author: Jonathan Garcia
"""

import pyodbc
import pandas as pd
from datetime import datetime
import os

# ─────────────────────────────────────────────
# CONFIGURATION  ← update these before running
# ─────────────────────────────────────────────

DB_CONFIG = {
    "server":   "your_server_name",      # e.g. "DESKTOP-ABC123" or "192.168.1.10"
    "database": "your_data_base_name",    # e.g. "NationsCabinetry"
    "driver":   "driver",  # check yours with: pyodbc.drivers()

    # Choose ONE auth method and comment out the other:

    # Option A - Windows Authentication (most common for on-prem SSMS)
    #"trusted_connection": "yes",

    # Option B - SQL Server login (uncomment if you use a username/password)
     "username": "username",
     "password": "password",
}

# Folder where CSVs will be saved
OUTPUT_DIR = "./exports"

# BOM table name - update this to match your actual table name in SSMS
BOM_TABLE = "your_bom_table_name"   # e.g. "SAL_SALES_ORDER_LINE_BOM"

# How many months of history to pull (keeps file sizes manageable)
MONTHS_BACK = 24

# ─────────────────────────────────────────────
# QUERIES
# ─────────────────────────────────────────────

SALES_ORDER_QUERY = f"""
    SELECT
        SALES_ORDER_ID,
        SALES_ORDER_NUM,
        ORDER_DATE,
        SALES_ORDER_TYPE_ID,
        CUSTOMER_ID,
        BILL_TO_COMPANY_NAME,
        SALESPERSON_ID,
        DIVISION_ID,
        ANTICIPATED_SHIP_DATE,
        SAL_ORDER_STATUS_ID,
        COST,
        AMOUNT,
        PROFIT,
        PROFIT_PERCENT,
        TOTAL_PIECES,
        PROJECT_NAME,
        CUSTOMER_PO_NUM,
        CREATED_DATE
    FROM SAL_SALES_ORDER
    WHERE
        ORDER_DATE >= DATEADD(MONTH, -{MONTHS_BACK}, GETDATE())
        AND SALES_ORDER_TYPE_ID = 1        -- standard cabinet orders only
        AND AMOUNT > 0                      -- exclude zero-dollar orders
        AND COST > 0                        -- exclude no-cost orders
        AND CUSTOMER_ID != 857             -- exclude TEST CUSTOMER
        AND SAL_ORDER_STATUS_ID != 6       -- exclude draft/open orders
    ORDER BY ORDER_DATE DESC
"""

BOM_QUERY = f"""
    SELECT
        SALES_ORDER_ID,
        LINE_ITEM_SEQ_NUM,
        BOM_SEQ_NUM,
        ITEM_ID,
        ITEM_NAME,
        RAW_MATERIAL_ITEM_ID,
        RAW_MATERIAL_NAME,
        RAW_MATERIAL_QUANTITY,
        RAW_MATERIAL_GROUP,
        UNIT_COST,
        BASE_QUANTITY,
        HIERARCHICAL_LEVEL,
        PARENT_ITEM_ID,
        PARENT_BOM_SEQ_NUM,
        SCRAP_PERCENT,
        ISSUE_UOM_ID,
        RAW_MATERIAL_CONSTRAINTS
    FROM {BOM_TABLE}
    WHERE
        SALES_ORDER_ID IN (
            SELECT SALES_ORDER_ID
            FROM SAL_SALES_ORDER
            WHERE
                ORDER_DATE >= DATEADD(MONTH, -{MONTHS_BACK}, GETDATE())
                AND SALES_ORDER_TYPE_ID = 1
                AND AMOUNT > 0
                AND COST > 0
                AND CUSTOMER_ID != 857
        )
        AND RAW_MATERIAL_ITEM_ID != -1     -- only rows with actual raw materials
        AND HIERARCHICAL_LEVEL = 7         -- deepest level = actual raw material consumption
    ORDER BY SALES_ORDER_ID, LINE_ITEM_SEQ_NUM, BOM_SEQ_NUM
"""

# ─────────────────────────────────────────────
# FUNCTIONS
# ─────────────────────────────────────────────

def build_connection_string(config):
    """Build pyodbc connection string from config dict."""
    if config.get("trusted_connection") == "yes":
        return (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"Trusted_Connection=yes;"
        )
    else:
        return (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']};"
        )


def run_query(conn, query, label):
    """Run a SQL query and return a DataFrame."""
    print(f"\n[{label}] Running query...")
    try:
        df = pd.read_sql(query, conn)
        print(f"[{label}] ✓ {len(df):,} rows fetched")
        return df
    except Exception as e:
        print(f"[{label}] ✗ Query failed: {e}")
        raise


def save_csv(df, filename, output_dir):
    """Save DataFrame to CSV with a timestamped filename."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"{timestamp}_{filename}")
    df.to_csv(filepath, index=False)
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  Saved → {filepath}  ({size_mb:.2f} MB)")
    return filepath


def print_summary(df, label):
    """Print a quick sanity check on the data."""
    print(f"\n── {label} Summary ──────────────────────")
    print(f"  Rows:    {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    if "ORDER_DATE" in df.columns:
        print(f"  Date range: {df['ORDER_DATE'].min()} → {df['ORDER_DATE'].max()}")
    if "COST" in df.columns and "AMOUNT" in df.columns:
        df["cost_pct"] = df["COST"] / df["AMOUNT"]
        over_target = (df["cost_pct"] > 0.40).sum()
        avg_pct = df["cost_pct"].mean() * 100
        print(f"  Avg material cost %: {avg_pct:.1f}%")
        print(f"  Orders over 40% target: {over_target:,}  ({over_target/len(df)*100:.1f}%)")
    print("─" * 44)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  Cabinet Manufacturing - SSMS Extract")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # Build connection
    conn_str = build_connection_string(DB_CONFIG)
    print(f"\nConnecting to: {DB_CONFIG['server']} / {DB_CONFIG['database']}")

    try:
        conn = pyodbc.connect(conn_str)
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("  - Run pyodbc.drivers() to see available drivers")
        print("  - Check server name in SSMS Object Explorer")
        print("  - Make sure SQL Server is accessible from this machine")
        return

    try:
        # --- Extract 1: Sales Orders ---
        sales_df = run_query(conn, SALES_ORDER_QUERY, "SALES_ORDERS")
        print_summary(sales_df, "Sales Orders")
        sales_path = save_csv(sales_df, "sales_orders.csv", OUTPUT_DIR)

        # --- Extract 2: BOM Data ---
        bom_df = run_query(conn, BOM_QUERY, "BOM_DATA")
        print_summary(bom_df, "BOM Data")
        bom_path = save_csv(bom_df, "bom_data.csv", OUTPUT_DIR)

        print("\n" + "=" * 50)
        print("  EXTRACT COMPLETE")
        print("=" * 50)
        print(f"  Sales orders: {sales_path}")
        print(f"  BOM data:     {bom_path}")
        print("\nNext step: upload these CSVs to Snowflake")
        print("=" * 50)

    finally:
        conn.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    main()