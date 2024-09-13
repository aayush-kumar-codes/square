import json
from sqlalchemy import insert, select

from db import catalog_category_table, catalog_discount_table, catalog_item_table, catalog_item_modifiers_table, catalog_item_option_table, catalog_item_option_values_table, catalog_item_variation_table


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
    bulk_insert_data = []
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
        
        bulk_insert_data.append(catalog_data)
    
    if bulk_insert_data:
        conn.execute(catalogs_table.insert(), bulk_insert_data)
        conn.commit()
 
def save_catalog_type_data(catalog_type_data, type ,conn):
    bulk_insert_data = []
    if type == "category":
        for catalog in catalog_type_data:
            id = catalog.get('id')
            category_data = catalog.get('category_data')
            
            # Check if the catalog already exists in the database
            existing_catalog = conn.execute(
                select(catalog_discount_table.c.id).where(catalog_discount_table.c.id == id)
            ).fetchone()
            
            if existing_catalog:
                continue

            catalog_data = {
                'id': id,
                'type': catalog.get('type'),
                'updated_at': catalog.get('updated_at'),
                'created_at': catalog.get('created_at'),
                'version': catalog.get('version'),
                'is_deleted': catalog.get('is_deleted'),
                'present_at_all_locations': catalog.get('present_at_all_locations'),
                'category_data_name': category_data.get('name'),
                'category_data_ordinal': category_data.get('ordinal'),
                'category_data_abbreviation': category_data.get('abbreviation'),
                'category_data_category_type': category_data.get('category_type'),
                'is_top_level': catalog.get('is_top_level'),
                'online_visibility': catalog.get('online_visibility'),
            }
            
            bulk_insert_data.append(catalog_data)
        
        if bulk_insert_data:
            conn.execute(catalog_category_table.insert(), bulk_insert_data)
            conn.commit()

    elif type == "discount":
        for catalog in catalog_type_data:
            id = catalog.get('id')
            discount_data = catalog.get('discount_data')
            
            # Check if the catalog already exists in the database
            existing_catalog = conn.execute(
                select(catalog_discount_table.c.id).where(catalog_discount_table.c.id == id)
            ).fetchone()
            
            if existing_catalog:
                continue

            catalog_data = {
                'id': id,
                'type': catalog.get('type'),
                'updated_at': catalog.get('updated_at'),
                'created_at': catalog.get('created_at'),
                'version': catalog.get('version'),
                'is_deleted': catalog.get('is_deleted'),
                'present_at_all_locations': catalog.get('present_at_all_locations'),
                'discount_data_name': discount_data.get('name'),
                'discount_data_discount_type': discount_data.get('discount_type'),
                'discount_data_percentage': discount_data.get('percentage'),
                'discount_data_pin_required': discount_data.get('pin_required'),
                'discount_data_application_method': discount_data.get('application_method'),
                'discount_data_comp_ordinal': discount_data.get('comp_ordinal'),
                'discount_data_modify_tax_basis': discount_data.get('modify_tax_basis'),
            }
            
            bulk_insert_data.append(catalog_data)
        
        if bulk_insert_data:
            conn.execute(catalog_discount_table.insert(), bulk_insert_data)
            conn.commit()

    elif type == "item":
        for catalog in catalog_type_data:
            id = catalog.get('id')
            item_data = catalog.get('item_data')
            categories = catalog.get('categories')
            reporting_category = catalog.get('reporting_category')
            # Check if the catalog already exists in the database
            existing_catalog = conn.execute(
                select(catalog_item_table.c.id).where(catalog_item_table.c.id == id)
            ).fetchone()
            
            if existing_catalog:
                continue

            catalog_data = {
                'id': id,
                'type': catalog.get('type'),
                'updated_at': catalog.get('updated_at'),
                'created_at': catalog.get('created_at'),
                'version': catalog.get('version'),
                'is_deleted': catalog.get('is_deleted'),
                'present_at_all_locations': catalog.get('present_at_all_locations'),
                'product_type': catalog.get('product_type'),
                'item_data_name': item_data.get('name'),
                'categories_id': categories.get('id') if categories else None ,
                'categories_ordinal': categories.get('ordinal') if categories else None ,
                'reporting_category': reporting_category.get('id') if reporting_category else None ,
                'reporting_ordinal': reporting_category.get('ordinal') if reporting_category else None
            }
            
            bulk_insert_data.append(catalog_data)
        if bulk_insert_data:
            conn.execute(catalog_item_table.insert(), bulk_insert_data)
            conn.commit()

    elif type == "modifier":
        for catalog in catalog_type_data:
            id = catalog.get('id')
            modifier_data = catalog.get('modifier_data')
            modifier_data_price_money = modifier_data.get('price_money')
            # Check if the catalog already exists in the database
            existing_catalog = conn.execute(
                select(catalog_item_modifiers_table.c.id).where(catalog_item_modifiers_table.c.id == id)
            ).fetchone()
            
            if existing_catalog:
                continue

            catalog_data = {
                'id': id,
                'type': catalog.get('type'),
                'updated_at': catalog.get('updated_at'),
                'created_at': catalog.get('created_at'),
                'version': catalog.get('version'),
                'is_deleted': catalog.get('is_deleted'),
                'present_at_all_locations': catalog.get('present_at_all_locations'),
                'modifier_data_name': modifier_data.get('name'),
                'modifier_data_price_money_amount': modifier_data_price_money.get('amount'),
                'modifier_data_price_money_currency': modifier_data_price_money.get('currency'),
                'on_by_default': modifier_data.get('on_by_default'),
                'ordinal': modifier_data.get('ordinal'),
                'modifier_list_id': modifier_data.get('modifier_list_id')
            }
            
            bulk_insert_data.append(catalog_data)
        if bulk_insert_data:
            conn.execute(catalog_item_modifiers_table.insert(), bulk_insert_data)
            conn.commit()

    elif type == "item_option":
        for catalog in catalog_type_data:
            id = catalog.get('id')
            item_option_data = catalog.get('item_option_data')
            # Check if the catalog already exists in the database
            existing_catalog = conn.execute(
                select(catalog_item_option_table.c.id).where(catalog_item_option_table.c.id == id)
            ).fetchone()
            
            if existing_catalog:
                continue

            catalog_data = {
                'id': id,
                'type': catalog.get('type'),
                'updated_at': catalog.get('updated_at'),
                'created_at': catalog.get('created_at'),
                'version': catalog.get('version'),
                'is_deleted': catalog.get('is_deleted'),
                'present_at_all_locations': catalog.get('present_at_all_locations'),
                'item_option_data_name': item_option_data.get('name'),
                'item_option_data_display_name': item_option_data.get('display_name'),
                'item_option_data_show_colors': item_option_data.get('show_colors')
            }
            
            bulk_insert_data.append(catalog_data)
        if bulk_insert_data:
            conn.execute(catalog_item_option_table.insert(), bulk_insert_data)
            conn.commit()

    elif type == "item_option_val":
        for catalog in catalog_type_data:
            id = catalog.get('id')
            item_option_value_data = catalog.get('item_option_value_data')
            # Check if the catalog already exists in the database
            existing_catalog = conn.execute(
                select(catalog_item_option_values_table.c.id).where(catalog_item_option_values_table.c.id == id)
            ).fetchone()
            
            if existing_catalog:
                continue

            catalog_data = {
                'id': id,
                'type': catalog.get('type'),
                'version': catalog.get('version'),
                'item_option_value_data_item_option_id': item_option_value_data.get('item_option_id'),
                'item_option_value_data_name': item_option_value_data.get('name'),
                'item_option_value_data_ordinal': item_option_value_data.get('ordinal')
            }
            
            bulk_insert_data.append(catalog_data)
        if bulk_insert_data:
            conn.execute(catalog_item_option_values_table.insert(), bulk_insert_data)
            conn.commit()

    elif type == "item_variation":
        for catalog in catalog_type_data:
            id = catalog.get('id')
            item_variation_data = catalog.get('item_variation_data')
            price_money = item_variation_data.get('price_money')
            # Check if the catalog already exists in the database
            existing_catalog = conn.execute(
                select(catalog_item_variation_table.c.id).where(catalog_item_variation_table.c.id == id)
            ).fetchone()
            
            if existing_catalog:
                continue

            catalog_data = {
                'id': id,
                'type': catalog.get('type'),
                'updated_at': catalog.get('updated_at'),
                'created_at': catalog.get('created_at'),
                'version': catalog.get('version'),
                'is_deleted': catalog.get('is_deleted'),
                'present_at_all_locations': catalog.get('present_at_all_locations'),
                'item_variation_data_item_id': item_variation_data.get('item_id'),
                'item_variation_data_name': item_variation_data.get('name'),
                'item_variation_data_sku': item_variation_data.get('sku'),
                'item_variation_data_ordinal': item_variation_data.get('ordinal'),
                'item_variation_data_pricing_type': item_variation_data.get('pricing_type'),
                'item_variation_data_price_money_amount': price_money.get('amount') if price_money else None,
                'item_variation_data_amount_currency': price_money.get('currency') if price_money else None ,
            }
            
            bulk_insert_data.append(catalog_data)
        if bulk_insert_data:
            conn.execute(catalog_item_variation_table.insert(), bulk_insert_data)
            conn.commit()

def save_program(program, program_table, program_locations_table, program_reward_tiers_table, conn):
    program_id = program.get("id")
    reward_tiers = program.get("reward_tiers")
    location_ids = program.get("location_ids", [])
    terminology = program.get("terminology")
    accrual_rules = program.get("accrual_rules")[0]
    spend_data = accrual_rules.get("spend_data")
    amount_money = spend_data.get("amount_money")
    
    existing_program = conn.execute(
            select(program_table.c.id).where(program_table.c.id == program_id)
        ).fetchone()
        
    if not existing_program:
        program_data = {
            "id": program_id,
            "status": program.get("status"),
            "terminology_one": terminology.get("one"),
            "terminology_other": terminology.get("other"),
            "created_at": program.get("created_at"),
            "updated_at": program.get("updated_at"),
            "accrual_rules_accrual_type": accrual_rules.get("accrual_type"),
            "accrual_rules_points": accrual_rules.get("points"),
            "accrual_rules_spend_data": json.dumps(spend_data),
            "accrual_rules_amount_money_amount": amount_money.get("amount"),
            "accrual_rules_amount_money_currency": amount_money.get("currency"),
            "tax_mode": spend_data.get("tax_mode")
        }
        stmt = insert(program_table).values(program_data)
        conn.execute(stmt)
        conn.commit()

    for reward_tier in reward_tiers: 
        id = reward_tier.get("id")
        definition = reward_tier.get("definition") 
        pricing_rule_reference = reward_tier.get("pricing_rule_reference")
        
        existing_reward_tier = conn.execute(
                select(program_reward_tiers_table.c.id).where(program_reward_tiers_table.c.id == id)
            ).fetchone()
            
        if existing_reward_tier:
            continue
        
        reward_tier_data = {
            "id": reward_tier.get("id"),
            "program_id": program_id,
            "points": reward_tier.get("points"),
            "name": reward_tier.get("name"),
            "definition_scope": definition.get("scope"),
            "definition_discount_type": definition.get("discount_type"),
            "definition_fixed_discount_money_amount": amount_money.get("amount"),
            "definition_fixed_discount_money_currency": amount_money.get("currency"),
            "created_at": reward_tier.get("created_at"),
            "pricing_rule_reference_object_id": pricing_rule_reference.get("object_id"),
            "pricing_rule_reference_catalog_version": pricing_rule_reference.get("catalog_version")
            }
        stmt = insert(program_reward_tiers_table).values(reward_tier_data)
        conn.execute(stmt)
        conn.commit()

    for location_id in location_ids:
        # Check if the program location id already exists in the database 
        existing_location_id = conn.execute(
            select(program_locations_table.c.location_id).where(program_locations_table.c.program_id == program_id, program_locations_table.c.location_id == location_id)
        ).fetchone()
        
        if existing_location_id:
            continue

        program_location_data ={
            "program_id": program_id,
            "location_id": location_id
        }

        stmt = insert(program_locations_table).values(program_location_data)
        conn.execute(stmt)
        conn.commit()

def save_loyalty_accounts(loyalty_accounts, loyalty_accounts_table, conn):
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
            "updated_at": loyalty_account.get("updated_at"),
            "mapping_customer_id": mapping.get("id"),
            "mapping_created_at": mapping.get("created_at"),
            "mapping_phone_number": mapping.get("phone_number")
        }

        stmt = insert(loyalty_accounts_table).values(loyalty_account_data)
        conn.execute(stmt)
        conn.commit()


def save_loyalty_events(loyalty_events, loyalty_events_table, conn):
    for loyalty_event in loyalty_events:
        event_id = loyalty_event.get("id")
        create_reward = loyalty_event.get("create_reward")

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
            "create_reward_loyalty_program_id": create_reward.get("loyalty_program_id"),
            "loyalty_program_id_points": create_reward.get("points"),
            "loyalty_account_id": loyalty_event.get("loyalty_account_id"),
            "location_id": loyalty_event.get("location_id"),
            "source": loyalty_event.get("source"),
        }
        stmt = insert(loyalty_events_table).values(loyalty_event_data)
        conn.execute(stmt)
        conn.commit()

def save_orders(orders, orders_table, conn):
    for order in orders:
        
        tender = order.get('tenders', [{}])[0]  
        customer_id = tender.get('customer_id')
        tender_type = tender.get('type')

        order_id = order.get("id")

        refunds = order.get("refunds", [])
        refund_data = {}
        if refunds:
            refund = refunds[0]  
            refund_data = {
                "refund_id": refund.get("id"),
                "refund_location_id": refund.get("location_id"),
                "refund_reason": refund.get("reason"),
                "refund_amount_money_amount": refund.get("amount_money", {}).get("amount"),
                "refund_amount_money_currency": refund.get("amount_money", {}).get("currency"),
                "refund_status": refund.get("status"),
            }

        # Handle returns data to get source order ID
        returns = order.get("returns", [])
        refund_source_order_id = None
        if returns:
            refund_source_order_id = returns[0].get("source_order_id")
       

        
        # Check if the order already exists in the database 
        existing_order = conn.execute(
            #select(orders_table.c.order_id).where(orders_table.c.order_id == order_id)
            select(orders_table.c.id).where(orders_table.c.id == order_id)
        ).fetchone()

        if existing_order:
            continue

        order_data = {
        "id": order.get("id"),
        "customer_id": customer_id,
        "location_id": order.get("location_id"),
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at"),
        "closed_at": order.get("closed_at"),
        "state": order.get("state"),
        "total_tax_money_amount": order.get("total_tax_money", {}).get("amount"),
        "total_tax_money_currency": order.get("total_tax_money", {}).get("currency"),
        "total_discount_money_amount": order.get("total_discount_money", {}).get("amount"),
        "total_discount_money_currency": order.get("total_discount_money", {}).get("currency"),
        "total_tip_money_amount": order.get("total_tip_money", {}).get("amount"),
        "total_tip_money_currency": order.get("total_tip_money", {}).get("currency"),
        "total_money_amount": order.get("total_money", {}).get("amount"),
        "total_money_currency": order.get("total_money", {}).get("currency"),
        
        # Refund fields
        "refund_source_order_id": refund_source_order_id,
        "refund_id": refund_data.get("refund_id"),
        "refund_location_id": refund_data.get("refund_location_id"),
        "refund_reason": refund_data.get("refund_reason"),
        "refund_amount_money_amount": refund_data.get("refund_amount_money_amount"),
        "refund_amount_money_currency": refund_data.get("refund_amount_money_currency"),
        "refund_status": refund_data.get("refund_status"),
        "type": tender_type,
    }
        
        stmt = insert(orders_table).values(order_data)
        conn.execute(stmt)
        conn.commit()

        
def orders_discounts_data(orders, orders_discounts_table, conn):
     for order in orders:
        
        discounts = order.get('discounts', [])

        for discount in discounts:
            
            discount_data = {
                "order_id": order.get("id"),  
                "uid": discount.get("uid"),
                "catalog_object_id": discount.get("catalog_object_id"),
                "catalog_version": discount.get("catalog_version"),
                "name": discount.get("name"),
                "percentage": discount.get("percentage"),
                "applied_money_amount": discount.get("applied_money", {}).get("amount"),
                "applied_money_currency": discount.get("applied_money", {}).get("currency")
            }

            # Check if the discount already exists in the database
            existing_discount = conn.execute(
                select(orders_discounts_table.c.id).where(orders_discounts_table.c.uid == discount_data['uid'])
            ).fetchone()

            if existing_discount:
                continue

            # Insert the discount data into the database
            stmt = insert(orders_discounts_table).values(discount_data)
            conn.execute(stmt)
            conn.commit()


def save_orders_line_items_data(orders, orders_line_items_table, conn):
    for order in orders:
        # Extract line items from the order
        line_items = order.get('line_items', [])


        # Create a mapping of return line item UIDs
        return_line_item_uids = {}
        returns = order.get('returns', [])
        for return_obj in returns:
            return_line_items = return_obj.get('return_line_items', [])
            for return_line_item in return_line_items:
                uid = return_line_item.get('uid')
                return_line_item_uids[uid] = uid

        for item in line_items:
            # Extract base price money
            base_price_money_amount = item.get('base_price_money', {}).get('amount')
            base_price_money_currency = item.get('base_price_money', {}).get('currency')

            # Extract total service charge money
            total_service_charge_money_amount = item.get('total_service_charge_money', {}).get('amount')
            total_service_charge_money_currency = item.get('total_service_charge_money', {}).get('currency')

            # Extract applied taxes
            applied_taxes = item.get('applied_taxes', [])
            applied_taxes_uid = ','.join([tax.get('uid') for tax in applied_taxes])
            applied_taxes_tax_uid = ','.join([tax.get('tax_uid') for tax in applied_taxes])
            applied_taxes_applied_money_amount = ','.join([str(tax.get('applied_money', {}).get('amount')) for tax in applied_taxes])
            applied_taxes_applied_money_currency = ','.join([tax.get('applied_money', {}).get('currency') for tax in applied_taxes])

            # Extract taxes
            taxes = order.get('taxes', [])
            for tax in taxes:
                tax_uid = tax.get('uid')
                tax_catalog_object_id = tax.get('catalog_object_id')
                tax_catalog_version = tax.get('catalog_version')
                tax_name = tax.get('name')
                tax_percentage = tax.get('percentage')
                tax_type = tax.get('type')
                tax_applied_money_amount = tax.get('applied_money', {}).get('amount')
                tax_amount_currency = tax.get('applied_money', {}).get('currency')
                tax_scope = tax.get('scope')

                # Process each line item
                line_item_uid = item.get("uid", '')
                line_item_data = {
                    "order_id": order.get("id"), 
                    "line_item_uid": item.get("uid", ''),
                    "catalog_object_id": item.get("catalog_object_id"),
                    "catalog_version": item.get("catalog_version"),
                    "quantity": item.get("quantity"),
                    "name": item.get("name"),
                    "variation_name": item.get("variation_name"),
                    "base_price_money_amount": base_price_money_amount,
                    "base_price_money_currency": base_price_money_currency,
                    "applied_taxes_uid": applied_taxes_uid,
                    "applied_taxes_tax_uid": applied_taxes_tax_uid,
                    "applied_taxes_applied_money_amount": applied_taxes_applied_money_amount,
                    "applied_taxes_applied_money_currency": applied_taxes_applied_money_currency,
                    "total_service_charge_money_amount": total_service_charge_money_amount,
                    "total_service_charge_money_currency": total_service_charge_money_currency,
                    "taxes_uid": tax_uid,
                    "taxes_catalog_object_id": tax_catalog_object_id,
                    "taxes_catalog_version": tax_catalog_version,
                    "taxes_name": tax_name,
                    "taxes_percentage": tax_percentage,
                    "taxes_type": tax_type,
                    "taxes_applied_money_amount": tax_applied_money_amount,
                    "taxes_amount_currency": tax_amount_currency,
                    "taxes_scope": tax_scope,
                    "return_line_items_source_line_item_uid": return_line_item_uids.get(line_item_uid, '') 
                }
                
                
                try:
                    # Corrected select statement
                    stmt = select(orders_line_items_table.c.line_item_uid).where(
                        orders_line_items_table.c.line_item_uid == line_item_data['line_item_uid']
                    )
                    existing_line_item = conn.execute(stmt).fetchone()

                    if existing_line_item:
                        continue

                    insert_stmt = insert(orders_line_items_table).values(line_item_data)
                    conn.execute(insert_stmt)
                    conn.commit()
                except Exception as e:
                    print(f"Error processing line item with UID {line_item_data['line_item_uid']}: {e}")



def save_orders_line_items_applied_discounts_data(orders, order_line_items_applied_discounts_table, conn):
    for order in orders:
        line_items = order.get('line_items', [])
        discounts = {d['uid']: d for d in order.get('discounts', [])} 

        for item in line_items:
            # Extract applied discounts
            applied_discounts = item.get('applied_discounts', [])
            for applied_discount in applied_discounts:
                discount_uid = applied_discount.get('discount_uid')
                discount_details = discounts.get(discount_uid, {})

                discount_data = {
                    "line_item_uid": item.get("uid", ''),
                    "uid": applied_discount.get('uid', ''),
                    "catalog_object_id": discount_details.get('catalog_object_id', ''),
                    "catalog_version": discount_details.get('catalog_version', ''),
                    "name": discount_details.get('name', ''),
                    "percentage": discount_details.get('percentage', ''),
                    "applied_money_amount": applied_discount.get('applied_money', {}).get('amount', 0),
                    "applied_money_currency": applied_discount.get('applied_money', {}).get('currency', '')
                }

                try:
                    # Check if the discount already exists in the database
                    stmt = select(order_line_items_applied_discounts_table.c.uid).where(
                        (order_line_items_applied_discounts_table.c.uid == discount_data['uid']) &
                        (order_line_items_applied_discounts_table.c.line_item_uid == discount_data['line_item_uid'])
                    )
                    existing_discount = conn.execute(stmt).fetchone()

                    if existing_discount:
                        continue

                    # Insert the discount data into the database
                    insert_stmt = insert(order_line_items_applied_discounts_table).values(discount_data)
                    conn.execute(insert_stmt)
                    conn.commit()
    
                except Exception as e:
                    print(f"Error processing discount with UID {discount_data['uid']} for line item UID {discount_data['line_item_uid']}: {e}")


def save_orders_line_items_modifiers_data(orders, order_line_items_modifiers_table, conn):
    for order in orders:
        line_items = order.get('line_items', [])

        for item in line_items:
            # Extract modifiers
            modifiers = item.get('modifiers', [])
            for modifier in modifiers:
                modifier_data = {
                    "order_id": order.get("id", ''),
                    "modifier_uid": modifier.get('uid', ''),
                    "name": modifier.get('name', ''),
                    "catalog_object_id": modifier.get('catalog_object_id', ''),
                    "catalog_version": modifier.get('catalog_version', ''),
                    "quantity": modifier.get('quantity', ''),
                    "base_price_money_amount": modifier.get('base_price_money', {}).get('amount', 0),
                    "base_price_money_currency": modifier.get('base_price_money', {}).get('currency', ''),
                    "total_price_money_amount": modifier.get('total_price_money', {}).get('amount', 0),
                    "total_price_money_currency": modifier.get('total_price_money', {}).get('currency', '')
                }

                
                try:
                    # Check if the modifier already exists in the database
                    stmt = select(order_line_items_modifiers_table.c.modifier_uid).where(
                        (order_line_items_modifiers_table.c.modifier_uid == modifier_data['modifier_uid']) &
                        (order_line_items_modifiers_table.c.order_id == modifier_data['order_id'])
                    )
                    existing_modifier = conn.execute(stmt).fetchone()

                    if existing_modifier:
                        continue

                    # Insert the modifier data into the database
                    insert_stmt = insert(order_line_items_modifiers_table).values(modifier_data)
                    conn.execute(insert_stmt)
                    conn.commit()
                    

                except Exception as e:
                    print(f"Error processing modifier with UID {modifier_data['modifier_uid']} for order ID {modifier_data['order_id']}: {e}")
