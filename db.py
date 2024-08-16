from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

# Define MSSQL connection parameters
DATABASE_URI = "mssql+pyodbc://sa:XcO3bDsymu-A746-WA8rbkhX!2P_G@mssql-poc-u8681.vm.elestio.app:18698/Square?driver=ODBC+Driver+17+for+SQL+Server"

# Set up SQLAlchemy engine and session
engine = create_engine(DATABASE_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

metadata = MetaData()
customers_table = Table(
    'customers', metadata,
    autoload_with=engine,
    schema='Square'
)

customer_preferences_table = Table(
    'customer_preferences', metadata,
    autoload_with=engine,
    schema='Square'
)

customer_segments_table = Table(
    'customer_segments', metadata,
    autoload_with=engine,
    schema='Square'
)

customer_groups_table = Table(
    'customer_groups', metadata,
    autoload_with=engine,
    schema='Square'
)

customer_address_table = Table(
    "customer_address", metadata,
    autoload_with=engine,
    schema='Square'
)

segments_values_table = Table(
    "customer_segments_values", metadata,
    autoload_with=engine,
    schema='Square'
)

groups_values_table = Table(
    "customer_groups_values", metadata,
    autoload_with=engine,
    schema='Square'
)

catalogs_table = Table(
    "catalog", metadata,
    autoload_with=engine,
    schema='Square'
)

program_table = Table(
    "program", metadata,
    autoload_with=engine,
    schema='Square'
)

loyalty_accounts_table = Table(
    "loyalty_accounts", metadata,
    autoload_with=engine,
    schema='Square'
)

loyalty_accounts_mapping_table = Table(
    "loyalty_accounts_mapping", metadata,
    autoload_with=engine,
    schema='Square'
)

loyalty_events_table = Table(
    "loyalty_events", metadata,
    autoload_with=engine,
    schema='Square'
)

orders_table = Table(
    "orders", metadata,
    autoload_with=engine,
    schema='Square'
)