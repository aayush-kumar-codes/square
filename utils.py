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