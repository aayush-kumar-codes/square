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

catalog_category_table = Table(
    "catalog_category", metadata,
    autoload_with=engine,
    schema='Square'
)

catalog_discount_table = Table(
    "catalog_discount", metadata,
    autoload_with=engine,
    schema='Square'
)

catalog_item_table = Table(
    "catalog_item", metadata,
    autoload_with=engine,
    schema='Square'
)

catalog_item_modifiers_table = Table(
    "catalog_item_modifiers", metadata,
    autoload_with=engine,
    schema='Square'
)

catalog_item_option_table = Table(
    "catalog_item_option", metadata,
    autoload_with=engine,
    schema='Square'
)

catalog_item_option_values_table = Table(
    "catalog_item_option_values", metadata,
    autoload_with=engine,
    schema='Square'
)

catalog_item_variation_table = Table(
    "catalog_item_variation", metadata,
    autoload_with=engine,
    schema='Square'
)

loyalty_program_table = Table(
    "loyalty_program", metadata,
    autoload_with=engine,
    schema='Square'
)

loyalty_program_locations_table = Table(
    "loyalty_program_locations", metadata,
    autoload_with=engine,
    schema='Square'
)

loyalty_program_reward_tiers_table = Table(
    "loyalty_program_reward_tiers", metadata,
    autoload_with=engine,
    schema='Square'
)

loyalty_accounts_table = Table(
    "loyalty_accounts", metadata,
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

orders_discounts_table = Table(
    'orders_discounts', metadata,
    autoload_with=engine,
    schema='Square'
)

orders_line_items_table = Table(
    'orders_line_items', metadata,
    autoload_with=engine,
    schema='Square'
)

order_line_items_applied_discounts_table = Table(
    'orders_line_items_applied_discounts', metadata,
    autoload_with=engine,
    schema='Square'
)

order_line_items_modifiers_table = Table(
    'orders_line_items_modifiers', metadata,
    autoload_with=engine,
    schema='Square'
)
