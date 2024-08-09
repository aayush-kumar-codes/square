import os

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from db import customer_groups_table, customers_table, customer_preferences_table, customer_segments_table, DATABASE_URI, engine, SessionLocal
from utils import save_customers_data, save_customer_groups, save_customer_preferences, save_customer_segments


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
        print("api hit")
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
