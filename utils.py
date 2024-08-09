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

        # Check if the customer segments already exists in the database 
        existing_customer_segment = conn.execute(
            select(customer_segment_table.c.customer_id).where(customer_segment_table.c.customer_id == customer_id)
        ).fetchone()
        
        if existing_customer_segment:
            continue

        customer_segment_data ={
            "customer_id": customer.get("id"),
            "segment_id": customer.get("segment_ids")[0]
        }

        stmt = insert(customer_segment_table).values(customer_segment_data)
        conn.execute(stmt)
        conn.commit()

def save_customer_groups(customers_data, customer_group_table, conn):
    for customer in customers_data:
        customer_id = customer.get("id")

        # Check if the customer groups already exists in the database 
        existing_customer_group = conn.execute(
            select(customer_group_table.c.customer_id).where(customer_group_table.c.customer_id == customer_id)
        ).fetchone()
        
        if existing_customer_group:
            continue

        customer_group_data ={
            "customer_id": customer.get("id"),
            "group_id": customer.get("group_ids")[0] if customer.get("group_ids") else None
        }

        stmt = insert(customer_group_table).values(customer_group_data)
        conn.execute(stmt)
        conn.commit()
