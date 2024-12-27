import json
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

from db import (
    engine,
    order_line_items_applied_discounts_table,
    order_line_items_modifiers_table,
    orders_discounts_table,
    orders_line_items_table,
    orders_table,
)
from utils import (
    orders_discounts_data,
    save_orders,
    save_orders_line_items_applied_discounts_data,
    save_orders_line_items_data,
    save_orders_line_items_modifiers_data,      
)


load_dotenv()

# Define your Square API credentials
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
SQUARE_BASE_URL = os.getenv("SQUARE_BASE_URL")

headers = {
    'Authorization': f'Bearer {SQUARE_ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

def get_square_orders():
    try:
        # List of location IDs
        location_ids = [
            "FGB5RC5XZB40V",
            "9C5FK94Z9RV1Y",
            "855HZ46DVTZZA",
            "HF76N2SWCBA82",
            "LNS16N1DCZ3DZ",
            "L9X26DHNKNGAH",
            "LQDJVT1JESBZ4",
            "71687SDWY31KY",
            "BFKQ37TW6SC45",
            "CN7V15Z2TZAJP",
            "QEFEH2W0TQPXA",
            "L9HAW7DM8J21X",
            "XP2VE3WA94D96",
            "SZSASWT7P91HZ",
            "KD6PC1TQYFJ6B",
            "QX2MG1Q66NFW4",
            "1M6EB38YP225E",
            "73STF1DWCX822",
            "1W3VCTHAHRQ0G"
        ]
        
        total_orders_count = 0
        
        for location_id in location_ids:
            delta = timedelta(days=1)  # 1-day intervals

            for date in [datetime.strptime("2024-11-12T08:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), datetime.strptime("2024-11-13T08:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), datetime.strptime("2024-11-14T08:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), datetime.strptime("2024-11-15T08:00:00Z", "%Y-%m-%dT%H:%M:%SZ")]:
                start_date = date 
                end_date = start_date + delta
                body = {
                    "location_ids": [location_id],
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
                                    "start_at": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                    "end_at": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                                }
                            }
                        }
                    }
                }

                # Make the POST request to the Square API
                response = requests.post(f'{SQUARE_BASE_URL}/orders/search', headers=headers, json=body)

                if response.status_code != 200:
                    print(response.status_code, response.json())

                orders = response.json().get("orders", [])
                location_orders_count = len(orders)
                total_orders_count += location_orders_count
                
                print(f"Location: {location_id}, Date: {start_date} to {end_date}, Orders: {location_orders_count}")
                

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

        # Return a success message after processing all locations and dates
        print("Executed successfully", "orders:   ", total_orders_count)

    except Exception as e:
        print(f"Error occurred: {e}")
        
get_square_orders()