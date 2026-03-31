# Cabinet Manufacturing Cost Analytics Pipeline

## Overview
An end-to-end ELT pipeline built to analyze material cost efficiency across cabinet manufacturing orders. The pipeline extracts production data from an on-premise SQL Server database, loads it into Snowflake, and uses dbt to transform raw data into actionable cost analytics.

**Key Finding:** Analysis of 19,000+ orders over a 2-year period revealed that 41% of orders exceeded the company's 40% material cost target, with individual orders showing cost inflation of up to 10x caused by BOM configuration errors.

---

## Tech Stack
- **Python** — data extraction from SQL Server using pyodbc and pandas
- **SQL Server (T-SQL)** — source database containing sales orders and BOM data
- **Snowflake** — cloud data warehouse for storage and analytics
- **dbt** — data transformation and modeling

---

## Architecture

```
SQL Server (On-Premise)
        │
        │  Python (pyodbc)
        ▼
   CSV Export
        │
        │  Snowflake Upload
        ▼
  Snowflake Raw Schema
        │
        │  dbt Models
        ▼
  Cost Analysis Mart
```

---

## Project Structure

```
cabinet-cost-pipeline/
│
├── extract_to_csv.py          # Extracts sales orders and BOM data from SQL Server
│
├── sources.yml                # dbt source definitions pointing to raw Snowflake tables
│
├── stg_sales_orders.sql       # Staging model — cleans and calculates material cost %
├── stg_bom_data.sql           # Staging model — flattens BOM raw material rows
└── fct_cost_analysis.sql      # Mart model — joins orders with BOM cost for analysis
```

---

## Data Pipeline

### Step 1 — Extract
`extract_to_csv.py` connects to an on-premise SQL Server database and extracts:
- **Sales Orders** — 19,000+ orders including sale amount, material cost, customer, and salesperson
- **BOM Data** — 1.5M+ raw material rows including quantities, unit costs, and component details

### Step 2 — Load
Extracted CSVs are loaded into Snowflake under the `cabinet_manufacturing.raw` schema using Snowflake's built-in file upload.

### Step 3 — Transform (dbt)
Three dbt models transform the raw data:

| Model | Type | Description |
|-------|------|-------------|
| `stg_sales_orders` | Staging | Cleans orders, calculates material cost %, flags orders over 40% target |
| `stg_bom_data` | Staging | Filters to raw material rows, calculates extended cost per component |
| `fct_cost_analysis` | Mart | Joins orders with BOM totals, calculates cost variance |

---

## Key Findings

- **41%** of orders exceeded the company's 40% material cost target over a 2-year period
- **Top customers by cost exposure** — analysis by customer revealed significant over-target ordering patterns across major accounts
- **BOM configuration errors** — pipeline detected dimension parameter errors causing individual cabinet costs to inflate 10x, from ~$450 to $4,537, driven by incorrect height values (e.g. 9090" instead of 90") causing the system to calculate 891 linear feet of maple and 3,787 units of finish instead of standard quantities

---

## Setup Instructions

### Prerequisites
```
pip install pyodbc pandas dbt-snowflake
```

### Configuration
1. Open `extract_to_csv.py` and update the `DB_CONFIG` section with your SQL Server credentials
2. Update `BOM_TABLE` with your BOM table name
3. Run the extract script:
```
python extract_to_csv.py
```
4. Upload the generated CSVs to your Snowflake raw schema
5. Configure your dbt profile with your Snowflake credentials
6. Run dbt models:
```
dbt run
```

---

## Sample Analysis Queries

**Orders over 40% material cost target:**
```sql
SELECT *
FROM cabinet_manufacturing.raw.fct_cost_analysis
WHERE cost_target_status = 'Over Target'
AND TOTAL_PIECES > 0
ORDER BY material_cost_pct DESC;
```

**Cost exposure by customer:**
```sql
SELECT
    CUSTOMER_NAME,
    COUNT(*) as total_orders,
    ROUND(AVG(material_cost_pct) * 100, 1) as avg_cost_pct,
    SUM(CASE WHEN cost_target_status = 'Over Target' THEN 1 ELSE 0 END) as orders_over_target
FROM cabinet_manufacturing.raw.fct_cost_analysis
WHERE TOTAL_PIECES > 0
GROUP BY CUSTOMER_NAME
ORDER BY avg_cost_pct DESC;
```

---

## Author
**Jonathan Garcia**
San Antonio, TX
jonathan85665@gmail.com
