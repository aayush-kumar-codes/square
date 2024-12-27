
import pandas as pd
from sqlalchemy import create_engine

# Define MSSQL connection parameters
DATABASE_URI = "mssql+pyodbc://sa:XcO3bDsymu-A746-WA8rbkhX!2P_G@mssql-poc-u8681.vm.elestio.app:18698/Square?driver=ODBC+Driver+17+for+SQL+Server"

# Create database engine
engine = create_engine(DATABASE_URI)

query = """
SELECT
    l.name AS Location,
    (SELECT SUM(CAST(o.total_money_amount AS FLOAT)) / 100 
     FROM [Square].[square].[orders] o
     WHERE o.location_id = l.id AND o.created_at >= '2024-11-01T08:00:00Z' AND o.created_at < '2024-11-02T08:00:00Z' AND o.state = 'COMPLETED') AS [Gross Sales],
    (SELECT COUNT(*) 
     FROM [Square].[square].[orders_line_items] ol 
     WHERE ol.order_id IN (SELECT id FROM [Square].[square].[orders] WHERE location_id = l.id AND created_at >= '2024-11-01T08:00:00Z' AND created_at < '2024-11-02T08:00:00Z' AND state = 'COMPLETED')
    ) AS Items,
    (SELECT SUM(CAST(ol.total_service_charge_money_amount AS FLOAT)) / 100 
     FROM [Square].[square].[orders_line_items] ol 
     WHERE ol.order_id IN (SELECT id FROM [Square].[square].[orders] WHERE location_id = l.id AND created_at >= '2024-11-01T08:00:00Z' AND created_at < '2024-11-02T08:00:00Z' AND state = 'COMPLETED')
    ) AS [Service Charges],
    COALESCE(
        (SELECT SUM(CAST(o.refund_amount_money_amount AS FLOAT)) / 100 
         FROM [Square].[square].[orders] o
         WHERE o.location_id = l.id AND o.created_at >= '2024-11-01T08:00:00Z' AND o.created_at < '2024-11-02T08:00:00Z' AND o.state = 'COMPLETED'),
        0
    ) AS Refunds,
    (SELECT SUM(CAST(od.applied_money_amount AS FLOAT)) / 100 
     FROM [Square].[square].[orders_discounts] od 
     WHERE od.order_id IN (SELECT id FROM [Square].[square].[orders] WHERE location_id = l.id AND created_at >= '2024-11-01T08:00:00Z' AND created_at < '2024-11-02T08:00:00Z' AND state = 'COMPLETED')
    ) AS [Discounts & Comps],
    (COALESCE(
        (SELECT SUM(CAST(o.total_money_amount AS FLOAT)) / 100 
         FROM [Square].[square].[orders] o
         WHERE o.location_id = l.id AND o.created_at >= '2024-11-01T08:00:00Z' AND o.created_at < '2024-11-02T08:00:00Z' AND o.state = 'COMPLETED'),
        0
    ) - COALESCE(
        (SELECT SUM(CAST(o.refund_amount_money_amount AS FLOAT)) / 100 
         FROM [Square].[square].[orders] o
         WHERE o.location_id = l.id AND o.created_at >= '2024-11-01T08:00:00Z' AND o.created_at < '2024-11-02T08:00:00Z' AND o.state = 'COMPLETED'),
        0
    ) - COALESCE(
        (SELECT SUM(CAST(od.applied_money_amount AS FLOAT)) / 100 
     FROM [Square].[square].[orders_discounts] od 
     WHERE od.order_id IN (SELECT id FROM [Square].[square].[orders] WHERE location_id = l.id AND created_at >= '2024-11-01T08:00:00Z' AND created_at < '2024-11-02T08:00:00Z' AND state = 'COMPLETED')
    ),
        0
    )) AS [Net Sales],
    (SELECT SUM(CAST(o.total_tax_money_amount AS FLOAT)) / 100 
     FROM [Square].[square].[orders] o
     WHERE o.location_id = l.id AND o.created_at >= '2024-11-01T08:00:00Z' AND o.created_at < '2024-11-02T08:00:00Z' AND o.state = 'COMPLETED') AS [Tax],
     (SELECT SUM(CAST(o.total_tip_money_amount AS FLOAT)) / 100 
     FROM [Square].[square].[orders] o
     WHERE o.location_id = l.id AND o.created_at >= '2024-11-01T08:00:00Z' AND o.created_at < '2024-11-02T08:00:00Z' AND o.state = 'COMPLETED') AS [Tip],
    (SELECT SUM(CAST(o.processing_fee_money_amount AS FLOAT)) / 100 
     FROM [Square].[square].[orders] o
     WHERE o.location_id = l.id AND o.created_at >= '2024-11-01T08:00:00Z' AND o.created_at < '2024-11-02T08:00:00Z' AND o.state = 'COMPLETED') AS [Fees]

FROM 
    [Square].[square].[locations] l
WHERE 
    (SELECT COUNT(*) 
     FROM [Square].[square].[orders_line_items] ol 
     WHERE ol.order_id IN (SELECT id FROM [Square].[square].[orders] WHERE location_id = l.id)
    ) > 0
ORDER BY 
    [Gross Sales] DESC;
"""


# Execute query and load data into a Pandas DataFrame
with engine.connect() as connection:
    data = pd.read_sql_query(query, connection)

# Save DataFrame to an Excel file
output_file = "excel2.xlsx"
data.to_excel(output_file, index=False)

print(f"Excel file '{output_file}' has been created successfully.")
