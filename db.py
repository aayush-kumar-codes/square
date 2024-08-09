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