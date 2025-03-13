import os
import sys
import tempfile
from pathlib import Path
import pandas as pd
import pytest
import sqlite3

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.app_sql import create_app


@pytest.fixture
def app():
    temp_db_fd, temp_db_path = tempfile.mkstemp(suffix=".db")
    os.close(temp_db_fd)
    config = {"TESTING": True, "DATABASE_PATH": temp_db_path}
    app = create_app(config)
    yield app
    os.unlink(temp_db_path)


def test_init_database(app):
    """Test that /init_database endpoint correctly initializes the database"""
    with app.test_client() as client:
        # Insert some test data first
        client.post(
            "/post_sales/",
            json=[{"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100}],
        )

        # Initialize the database
        response = client.post("/init_database")
        assert response.status_code == 200

        # Check that database tables are empty
        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            bronze = pd.read_sql_query("SELECT * FROM bronze_sales", conn)
            silver = pd.read_sql_query("SELECT * FROM silver_sales", conn)
            gold = pd.read_sql_query("SELECT * FROM gold_sales", conn)

            assert len(bronze) == 0
            assert len(silver) == 0
            assert len(gold) == 0


def test_post_sales_idempotence(app):
    """Test that posting the same data twice doesn't duplicate entries in SQL DB"""
    with app.test_client() as client:
        # Initialize the database
        client.post("/init_database")

        # Post the same data twice
        response = client.post(
            "/post_sales/",
            json=[{"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100}],
        )
        assert response.status_code == 200

        response = client.post(
            "/post_sales/",
            json=[{"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100}],
        )
        assert response.status_code == 200

        # Verify there's only one entry in the database
        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            bronze = pd.read_sql_query("SELECT * FROM bronze_sales", conn)
            assert len(bronze) == 1


def test_invalid_data_sql(app):
    """Test that invalid data is rejected"""
    with app.test_client() as client:
        response = client.post(
            "/post_sales/",
            json=[{"date": "2020-01"}],  # Missing required fields
        )
        assert response.status_code == 400


def test_partial_invalid_data_sql(app):
    """Test that if any record is invalid, none are processed"""
    with app.test_client() as client:
        client.post("/init_database")

        data = [
            {"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100},
            {"date": "2020-02", "vegetable": "carrot", "kilo_sold": 150},
            {"date": "2020-03"},  # Invalid record
            {"date": "2020-04", "vegetable": "potato", "kilo_sold": 200},
        ]
        response = client.post("/post_sales/", json=data)
        assert response.status_code == 400

        # Verify no data was inserted
        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            bronze = pd.read_sql_query("SELECT * FROM bronze_sales", conn)
            assert len(bronze) == 0


def test_get_raw_sales_sql(app):
    """Test the /get_raw_sales/ endpoint with SQL storage"""
    with app.test_client() as client:
        client.post("/init_database")

        client.post(
            "/post_sales/",
            json=[
                {"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100},
                {"date": "2020-02", "vegetable": "carrot", "kilo_sold": 150},
            ],
        )

        response = client.get("/get_raw_sales/")
        assert response.status_code == 200

        data = response.get_json()
        assert len(data) == 2
        assert all(k in data[0] for k in ["date", "vegetable", "kilo_sold"])


def test_get_monthly_sales_sql(app):
    """Test the /get_monthly_sales/ endpoint with SQL storage"""
    with app.test_client() as client:
        client.post("/init_database")

        test_data = [
            {"date": "2020-01", "vegetable": "tomate", "kilo_sold": 100},
            {"date": "2020-02", "vegetable": "tomato", "kilo_sold": 150},
            {
                "date": "2020-03",
                "vegetable": "tomatoes",
                "kilo_sold": 1000,
            },  # potential outlier
            {"date": "2020-04", "vegetable": "carrot", "kilo_sold": 200},
        ]
        client.post("/post_sales/", json=test_data)

        # Get all monthly sales
        response = client.get("/get_monthly_sales/")
        assert response.status_code == 200

        data = response.get_json()
        assert len(data) > 0
        assert all(
            veg in ["tomato", "carrot"] for d in data for veg in [d["vegetable"]]
        )

        # Get only non-outlier data
        response = client.get("/get_monthly_sales/?remove_outliers=true")
        assert response.status_code == 200

        data = response.get_json()
        assert all(not d.get("is_outlier", False) for d in data)


def test_vegetable_name_standardization_sql(app):
    """Test vegetable name standardization with SQL storage"""
    with app.test_client() as client:
        client.post("/init_database")

        test_data = [
            {"date": "2020-01", "vegetable": "tomate", "kilo_sold": 100},
            {"date": "2020-01", "vegetable": "carotte", "kilo_sold": 150},
            {"date": "2020-01", "vegetable": "patata", "kilo_sold": 200},
            {"date": "2020-01", "vegetable": "pera", "kilo_sold": 250},
            {"date": "2020-01", "vegetable": "brussel sprout", "kilo_sold": 300},
        ]
        client.post("/post_sales/", json=test_data)

        response = client.get("/get_monthly_sales/")
        data = response.get_json()

        vegetables = {d["vegetable"] for d in data}
        assert vegetables.issubset(
            {"tomato", "carrot", "potato", "pear", "brussels sprout"}
        )


def test_extended_translations(app):
    """Test the expanded vegetable name translations that were added"""
    with app.test_client() as client:
        client.post("/init_database")

        test_data = [
            {
                "date": "2020-01",
                "vegetable": "tomatto",
                "kilo_sold": 100,
            },  # Misspelling
            {
                "date": "2020-01",
                "vegetable": "tomaot",
                "kilo_sold": 150,
            },  # Another misspelling
            {
                "date": "2020-01",
                "vegetable": "peer",
                "kilo_sold": 200,
            },  # English variation
            {
                "date": "2020-01",
                "vegetable": "brusselsprout",
                "kilo_sold": 250,
            },  # No space
        ]
        client.post("/post_sales/", json=test_data)

        response = client.get("/get_monthly_sales/")
        data = response.get_json()

        vegetables = {d["vegetable"] for d in data}
        assert vegetables.issubset({"tomato", "pear", "brussels sprout"})
