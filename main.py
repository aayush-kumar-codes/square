import os

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from db import (
    catalogs_table,
    customer_address_table,
    customer_groups_table,
    customer_preferences_table,
    customer_segments_table,
    customers_table,
    engine,
    groups_values_table,
    locations_table,
    loyalty_accounts_table,
    loyalty_events_table,
    loyalty_program_locations_table,
    loyalty_program_reward_tiers_table,
    loyalty_program_table,
    orders_discounts_table,
    order_line_items_applied_discounts_table,
    order_line_items_modifiers_table,
    orders_line_items_table,
    orders_table,
    segments_values_table,
)
from utils import (
    orders_discounts_data,
    save_catalog_type_data,
    save_catalogs_data,
    save_customer_address,
    save_customer_groups,
    save_customer_preferences,
    save_customer_segments,
    save_customers_data,
    save_groups_data,
    save_locations_data,
    save_loyalty_accounts,
    save_loyalty_events,
    save_orders,
    save_orders_line_items_applied_discounts_data,
    save_orders_line_items_data,
    save_orders_line_items_modifiers_data,
    save_program,
    save_segments_data,
)


load_dotenv()

# Define your Square API credentials
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
SQUARE_BASE_URL = os.getenv("SQUARE_BASE_URL")

headers = {
    'Authorization': f'Bearer {SQUARE_ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

# Initialize FastAPI
app = FastAPI()


# GET Customers from Square API
@app.get("/customers")
def get_square_customers():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/customers', headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
        
        customers_data = response.json().get('customers', [])
        
        with engine.connect() as conn:
            save_customers_data(customers_data, customers_table, conn)
            print("Customer Data Saved Successfully")
            save_customer_preferences(customers_data, customer_preferences_table, conn)
            print("Customer Preferences Saved Successfully")
            save_customer_segments(customers_data, customer_segments_table, conn)
            print("Customer Segments Saved Successfully")
            save_customer_groups(customers_data, customer_groups_table, conn)
            print("Customer Groups Saved Successfully")  
            save_customer_address(customers_data, customer_address_table, conn)   
            print("Customer Address Saved Successfully")       
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

#GET Segments from Square API
@app.get("/segments")
def get_square_segments():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/customers/segments', headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
        
        segments_data = response.json().get('segments', [])
        
        with engine.connect() as conn:
            save_segments_data(segments_data, segments_values_table, conn)
            print("Segments Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

#GET Segments from Square API
@app.get("/groups")
def get_square_groups():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/customers/groups', headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
        
        groups_data = response.json().get('groups', [])
        
        with engine.connect() as conn:
            save_groups_data(groups_data, groups_values_table, conn)
            print("Groups Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

@app.post("/catalogs")
async def get_square_catalogs(request: Request):
    body = await request.json()
    types = body.get("types")
    if types not in ['item','category','tax','discount','modifier','item_option','item_option_val','item_variation']:
        return {"error": "Unknown type","accepted_type": ['item','category','tax','discount','modifier','item_option','item_option_val','item_variation']}
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/catalog/list?types={types}', headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        catalogs_data = response.json().get('objects', [])
        
        with engine.connect() as conn:
            save_catalogs_data(catalogs_data, catalogs_table, conn)
            print("Catalogs Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

@app.post("/catalog-type")
async def get_square_catalog_type(request: Request):
    body = await request.json()
    types = body.get("types")
    if types not in ['category','discount','item','modifier','item_option','item_option_val','item_variation']:
        return {"error": "Unknown type", "accepted_type": ['category','discount','item','modifier','item_option','item_option_val','item_variation'] }

    try:
        response = requests.get(f'{SQUARE_BASE_URL}/catalog/list?types={types}', headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        catalog_type_data = response.json().get('objects', [])
        
        with engine.connect() as conn:
            save_catalog_type_data(catalog_type_data, type, conn)
            print("Catalog Type Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

@app.get("/loyalty-program")
def get_square_loyalty_program():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/loyalty/programs/main', headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        program = response.json().get('program', {})
        
        with engine.connect() as conn:
            save_program(program, loyalty_program_table, loyalty_program_locations_table, loyalty_program_reward_tiers_table, conn)
            print("Program Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

@app.post("/loyalty-accounts")
async def get_square_loyalty_accounts(request: Request):
    try:
        body = await request.json()
        response = requests.post(f'{SQUARE_BASE_URL}/loyalty/accounts/search', headers=headers, json=body)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        loyalty_accounts = response.json().get("loyalty_accounts", [])
        
        with engine.connect() as conn:
            save_loyalty_accounts(loyalty_accounts, loyalty_accounts_table, conn)
            print("Loyalty Accounts Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

@app.post("/loyalty-events")
async def get_square_loyalty_events(request: Request):
    try:
        body = await request.json()
        response = requests.post(f'{SQUARE_BASE_URL}/loyalty/events/search', headers=headers, json=body)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        loyalty_events = response.json().get("events", [])
        loyalty_events = [item for item in loyalty_events if 'create_reward' in item]
        
        with engine.connect() as conn:
            save_loyalty_events(loyalty_events, loyalty_events_table, conn)
            print("Loyalty Events Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

@app.get("/orders")
async def get_square_orders():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/locations', headers=headers)
        data = response.json()
        location_ids = [location['id'] for location in data['locations'][:10]]
        
        start_date = ""
        end_date = ""
        body = {
            "location_ids": [
                location_ids
            ],
            "limit": 1000,
            "return_entries": False,
            "query": {
                "sort": {
                    "sort_field": "UPDATED_AT",
                    "sort_order": "ASC"
                    },
                "filter": {
                    "date_time_filter": {
                        "created_at": {
                            "start_at": start_date,
                            "end_at": end_date
                        }
                    }
                }
            }
        }
        
        response = requests.post(f'{SQUARE_BASE_URL}/orders/search', headers=headers, json=body)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        orders = response.json().get("orders", [])
        print(len(orders))
        
        with engine.connect() as conn:
            
            save_orders(orders, orders_table, conn)
            print("Orders Data Saved Successfully")     

            orders_discounts_data(orders, orders_discounts_table, conn)
            print("Orders Discounts Data Saved Successfully")

            save_orders_line_items_data(orders, orders_line_items_table, conn)
            print("Orders Line Items Data Saved Successfully")

            save_orders_line_items_applied_discounts_data(orders, order_line_items_applied_discounts_table, conn)
            print("Orders Line Items Applied Discounts Data Saved Successfully")

            save_orders_line_items_modifiers_data(orders, order_line_items_modifiers_table, conn)
            print("Orders Line Items Modifiers Data Saved Successfully")

                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

@app.get("/locations")
async def get_locations():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/locations', headers=headers)
        if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=response.json())
        locations_data=response.json().get("locations")
        with engine.connect() as conn:
                save_locations_data(locations_data, locations_table, conn)
                print("Locations Data Saved Successfully")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

