import json

import requests

BASE_URL = "http://localhost:8000"


def post_sales_data():
    """Example of posting sales data"""
    data = [
        {"year_week": 202001, "vegetable": "tomate", "sales": 100},
        {"year_week": 202002, "vegetable": "carotte", "sales": 150},
        {"year_week": 202003, "vegetable": "pomme de terre", "sales": 200},
    ]
    response = requests.post(f"{BASE_URL}/post_data", json=data)
    print("Post data response:", response.status_code)
    print(response.json())


def get_raw_sales():
    """Example of getting raw sales data"""
    response = requests.get(f"{BASE_URL}/get_raw_sales")
    print("\nRaw sales data:")
    print(json.dumps(response.json(), indent=2))


def get_monthly_sales(remove_outliers=False):
    """Example of getting monthly sales data"""
    params = {"remove_outliers": str(remove_outliers).lower()}
    response = requests.get(f"{BASE_URL}/get_monthly_sales", params=params)
    print(f"\nMonthly sales data (remove_outliers={remove_outliers}):")
    print(json.dumps(response.json(), indent=2))


if __name__ == "__main__":
    print("Running example API client...")
    post_sales_data()
    get_raw_sales()
    get_monthly_sales(remove_outliers=False)
    get_monthly_sales(remove_outliers=True)
