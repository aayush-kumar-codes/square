import os

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from db import catalogs_table, customer_address_table, customer_groups_table, customers_table, customer_preferences_table, customer_segments_table, DATABASE_URI, engine,loyalty_accounts_table, loyalty_accounts_mapping_table, loyalty_events_table, groups_values_table,orders_table, program_table, segments_values_table, SessionLocal
from utils import save_catalogs_data, save_customer_address, save_customers_data, save_customer_groups, save_customer_preferences, save_customer_segments,save_groups_data, save_loyalty_accounts, save_loyalty_events,save_orders, save_program, save_segments_data


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

@app.get("/catalogs")
def get_square_catalogs():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/catalog/list?types=category%2Ctax', headers=headers)
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

@app.get("/program")
def get_square_program():
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/loyalty/programs/main', headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        program = response.json().get('program', {})
        
        with engine.connect() as conn:
            save_program(program, program_table, conn)
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
            save_loyalty_accounts(loyalty_accounts, loyalty_accounts_table, loyalty_accounts_mapping_table, conn)
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

@app.post("/orders")
async def get_square_orders(request: Request):
    try:
        response = requests.get(f'{SQUARE_BASE_URL}/locations', headers=headers)
        data = response.json()
        location_ids = [location['id'] for location in data['locations'][:10]]
        body = await request.json()
        body["location_ids"] = location_ids
        response = requests.post(f'{SQUARE_BASE_URL}/orders/search', headers=headers, json=body)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
    
        orders = response.json().get("order_entries", [])
        
        with engine.connect() as conn:
            save_orders(orders, orders_table, conn)
            print("Orders Data Saved Successfully")      
                
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Square API.")
    
    return {"status": "success"}

# Start the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009)
