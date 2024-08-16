import json
from sqlalchemy import insert, select

def save_customers_data(customers_data, customers_table, conn):
    for customer in customers_data:
        customer_id = customer.get('id')
        
        # Check if the customer already exists in the database
        existing_customer = conn.execute(
            select(customers_table.c.id).where(customers_table.c.id == customer_id)
        ).fetchone()
        
        if existing_customer:
            continue

        customer_data = {
            'id': customer.get('id'),
            'version': customer.get('version'),
            'created_at': customer.get('created_at'),
            'updated_at': customer.get('updated_at'),
            'given_name': customer.get('given_name'),
            'family_name': customer.get('family_name'),
            'company_name': customer.get('company_name'),
            'phone_number': customer.get('phone_number'),
            'email_address': customer.get('email_address'),
            'birthday': customer.get('birthday'),
            'note': customer.get('note'),
            'creation_source': customer.get('creation_source'),
            'reference_id': customer.get('reference_id')
        }
        
        stmt = insert(customers_table).values(customer_data)
        conn.execute(stmt)
        conn.commit()

def save_customer_preferences(customers_data, customer_preference_table, conn):
    for customer in customers_data:
        customer_id = customer.get("id")

        # Check if the customer preferences already exists in the database 
        existing_customer_preference = conn.execute(
            select(customer_preference_table.c.customer_id).where(customer_preference_table.c.customer_id == customer_id)
        ).fetchone()
        
        if existing_customer_preference:
            continue

        customer_preference_data ={
            "customer_id": customer.get("id"),
            "email_unsubscribe": customer.get("preferences")["email_unsubscribed"]
        }

        stmt = insert(customer_preference_table).values(customer_preference_data)
        conn.execute(stmt)
        conn.commit()

def save_customer_segments(customers_data, customer_segment_table, conn):
    for customer in customers_data:
        customer_id = customer.get("id")
        segments_ids = customer.get("segment_ids", [])

        for segment_id in segments_ids:
            # Check if the customer segment already exists in the database 
            existing_customer_segment = conn.execute(
                select(customer_segment_table.c.customer_id).where(customer_segment_table.c.customer_id == customer_id,customer_segment_table.c.segment_id == segment_id)
            ).fetchone()
            
            if existing_customer_segment:
                continue

            customer_segment_data ={
                "customer_id": customer_id,
                "segment_id": segment_id
            }

            stmt = insert(customer_segment_table).values(customer_segment_data)
            conn.execute(stmt)
            conn.commit()

def save_customer_groups(customers_data, customer_group_table, conn):
    for customer in customers_data:
        customer_id = customer.get("id")
        group_ids = customer.get("group_ids", [])

        for group_id in group_ids:
            # Check if the customer group already exists in the database 
            existing_customer_group = conn.execute(
                select(customer_group_table.c.customer_id).where(customer_group_table.c.customer_id == customer_id,customer_group_table.c.group_id == group_id)
            ).fetchone()
            
            if existing_customer_group:
                continue

            customer_group_data ={
                "customer_id": customer_id,
                "group_id": group_id
            }

            stmt = insert(customer_group_table).values(customer_group_data)
            conn.execute(stmt)
            conn.commit()

def save_customer_address(customers_data, customer_address_table, conn):
    for customer in customers_data:
        customer_id = customer.get('id')
        address = customer.get("address")
        if not address:
            continue

        existing_customer_address = conn.execute(
            select(customer_address_table.c.customer_id).where(customer_address_table.c.customer_id == customer_id)
        ).fetchone()
        if existing_customer_address:
            continue

        
        customer_address_data = {
            "customer_id": customer_id,
            "address_line_1": address.get("address_line_1"),
            "address_line_2": address.get("address_line_2"),
            "locality": address.get("locality"),
            "administrative_district_level_1": address.get("administrative_district_level_1"),
            "postal_code": address.get("postal_code")
        }

        stmt = insert(customer_address_table).values(customer_address_data)
        conn.execute(stmt)
        conn.commit()

def save_segments_data(segments_data, segments_values_table, conn):
    for segment in segments_data:
        segment_id = segment.get('id')
        
        # Check if the segment already exists in the database
        existing_segment = conn.execute(
            select(segments_values_table.c.id).where(segments_values_table.c.id == segment_id)
        ).fetchone()
        
        if existing_segment:
            continue

        segment_data = {
            'id': segment.get('id'),
            'created_at': segment.get('created_at'),
            'updated_at': segment.get('updated_at'),
            'name': segment.get('name'),
        }
        
        stmt = insert(segments_values_table).values(segment_data)
        conn.execute(stmt)
        conn.commit()

def save_groups_data(groups_data, groups_values_table, conn):
    for group in groups_data:
        group_id = group.get('id')
        
        # Check if the segment already exists in the database
        existing_group = conn.execute(
            select(groups_values_table.c.id).where(groups_values_table.c.id == group_id)
        ).fetchone()
        
        if existing_group:
            continue

        group_data = {
            'id': group.get('id'),
            'created_at': group.get('created_at'),
            'updated_at': group.get('updated_at'),
            'name': group.get('name'),
        }
        
        stmt = insert(groups_values_table).values(group_data)
        conn.execute(stmt)
        conn.commit()

def save_catalogs_data(catalogs_data, catalogs_table, conn):
    for catalog in catalogs_data:
        catalog_id = catalog.get('id')
        
        # Check if the catalog already exists in the database
        existing_catalog = conn.execute(
            select(catalogs_table.c.catalog_id).where(catalogs_table.c.catalog_id == catalog_id)
        ).fetchone()
        
        if existing_catalog:
            continue

        catalog_data = {
            'catalog_id': catalog_id,
            'type': catalog.get('type'),
            'created_at': catalog.get('created_at'),
            'updated_at': catalog.get('updated_at'),
            'version': catalog.get('version'),
            'is_deleted': catalog.get('is_deleted'),
            'present_at_all_locations': catalog.get('present_at_all_locations')
        }
        
        stmt = insert(catalogs_table).values(catalog_data)
        conn.execute(stmt)
        conn.commit()

def save_program(program, program_table, conn):
    program_id = program.get("id")
    location_ids = program.get("location_ids", [])

    for location_id in location_ids:
        # Check if the program location id already exists in the database 
        existing_location_id = conn.execute(
            select(program_table.c.location_id).where(program_table.c.id == program_id, program_table.c.location_id == location_id)
        ).fetchone()
        
        if existing_location_id:
            continue

        program_data ={
            "id": program_id,
            "status": program.get("status"),
            "location_id": location_id,
            "created_at": program.get("created_at"),
            "updated_at": program.get("updated_at")
        }

        stmt = insert(program_table).values(program_data)
        conn.execute(stmt)
        conn.commit()

def save_loyalty_accounts(loyalty_accounts, loyalty_accounts_table, loyalty_accounts_mapping_table, conn):
    for loyalty_account in loyalty_accounts:
        customer_id = loyalty_account.get("customer_id")
        mapping = loyalty_account.get("mapping")

        # Check if the loyalty account already exists in the database 
        existing_loyalty_account = conn.execute(
            select(loyalty_accounts_table.c.customer_id).where(loyalty_accounts_table.c.customer_id == customer_id)
        ).fetchone()
        
        if existing_loyalty_account:
            continue

        loyalty_account_data = {
            "id": loyalty_account.get("id"),
            "program_id": loyalty_account.get("program_id"),
            "balance": loyalty_account.get("balance"),
            "lifetime_points": loyalty_account.get("lifetime_points"),
            "customer_id": loyalty_account.get("customer_id"),
            "enrolled_at": loyalty_account.get("enrolled_at"),
            "created_at": loyalty_account.get("created_at"),
            "updated_at": loyalty_account.get("updated_at")
        }

        loyalty_accounts_mapping_data = {
            "id": mapping.get("id"),
            "created_at": mapping.get("created_at"),
            "phone_number": mapping.get("phone_number")
        }

        stmt = insert(loyalty_accounts_table).values(loyalty_account_data)
        stmt2 = insert(loyalty_accounts_mapping_table).values(loyalty_accounts_mapping_data)
        conn.execute(stmt)
        conn.execute(stmt2)
        conn.commit()


def save_loyalty_events(loyalty_events, loyalty_events_table, conn):
    for loyalty_event in loyalty_events:
        event_id = loyalty_event.get("id")

        # Check if the loyalty event already exists in the database 
        existing_loyalty_event = conn.execute(
            select(loyalty_events_table.c.id).where(loyalty_events_table.c.id == event_id)
        ).fetchone()
        
        if existing_loyalty_event:
            continue

        loyalty_event_data = {
            "id": loyalty_event.get("id"),
            "type": loyalty_event.get("type"),
            "created_at": loyalty_event.get("created_at"),
            "loyalty_account_id": loyalty_event.get("loyalty_account_id"),
            "location_id": loyalty_event.get("location_id"),
            "source": loyalty_event.get("source"),
            "type_info": json.dumps(loyalty_event.get(loyalty_event.get("type").lower()))
        }

        stmt = insert(loyalty_events_table).values(loyalty_event_data)
        conn.execute(stmt)
        conn.commit()

def save_orders(orders, orders_table, conn):
    for order in orders:
        order_id = order.get("order_id")

        # Check if the order already exists in the database 
        existing_order = conn.execute(
            select(orders_table.c.order_id).where(orders_table.c.order_id == order_id)
        ).fetchone()
        
        if existing_order:
            continue

        order_data = {
            "order_id": order.get("order_id"),
            "version": order.get("version"),
            "location_id": order.get("location_id"),
            "source": order.get("source"),
            "created_at": order.get("created_at"),
            "updated_at": order.get("updated_at"),
        }

        stmt = insert(orders_table).values(order_data)
        conn.execute(stmt)
        conn.commit()