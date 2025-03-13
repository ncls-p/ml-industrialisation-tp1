import os
import sqlite3
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

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
    with app.test_client() as client:
        client.post(
            "/post_sales/",
            json=[{"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100}],
        )

        response = client.post("/init_database")
        assert response.status_code == 200

        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            bronze = pd.read_sql_query("SELECT * FROM bronze_sales", conn)
            silver = pd.read_sql_query("SELECT * FROM silver_sales", conn)
            gold = pd.read_sql_query("SELECT * FROM gold_sales", conn)

            assert len(bronze) == 0
            assert len(silver) == 0
            assert len(gold) == 0


def test_post_sales_idempotence(app):
    with app.test_client() as client:
        client.post("/init_database")

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

        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            bronze = pd.read_sql_query("SELECT * FROM bronze_sales", conn)
            assert len(bronze) == 1


def test_invalid_data_sql(app):
    with app.test_client() as client:
        response = client.post(
            "/post_sales/",
            json=[{"date": "2020-01"}],
        )
        assert response.status_code == 400


def test_partial_invalid_data_sql(app):
    with app.test_client() as client:
        client.post("/init_database")

        data = [
            {"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100},
            {"date": "2020-02", "vegetable": "carrot", "kilo_sold": 150},
            {"date": "2020-03"},
            {"date": "2020-04", "vegetable": "potato", "kilo_sold": 200},
        ]
        response = client.post("/post_sales/", json=data)
        assert response.status_code == 400

        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            bronze = pd.read_sql_query("SELECT * FROM bronze_sales", conn)
            assert len(bronze) == 0


def test_get_raw_sales_sql(app):
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
    with app.test_client() as client:
        client.post("/init_database")

        test_data = [
            {"date": "2020-01", "vegetable": "tomate", "kilo_sold": 100},
            {"date": "2020-02", "vegetable": "tomato", "kilo_sold": 150},
            {
                "date": "2020-03",
                "vegetable": "tomatoes",
                "kilo_sold": 1000,
            },
            {"date": "2020-04", "vegetable": "carrot", "kilo_sold": 200},
        ]
        client.post("/post_sales/", json=test_data)

        response = client.get("/get_monthly_sales/")
        assert response.status_code == 200

        data = response.get_json()
        assert len(data) > 0
        assert all(
            veg in ["tomato", "carrot"] for d in data for veg in [d["vegetable"]]
        )

        response = client.get("/get_monthly_sales/?remove_outliers=true")
        assert response.status_code == 200

        data = response.get_json()
        assert all(not d.get("is_outlier", False) for d in data)


def test_vegetable_name_standardization_sql(app):
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
    with app.test_client() as client:
        client.post("/init_database")

        test_data = [
            {
                "date": "2020-01",
                "vegetable": "tomatto",
                "kilo_sold": 100,
            },
            {
                "date": "2020-01",
                "vegetable": "tomaot",
                "kilo_sold": 150,
            },
            {
                "date": "2020-01",
                "vegetable": "peer",
                "kilo_sold": 200,
            },
            {
                "date": "2020-01",
                "vegetable": "brusselsprout",
                "kilo_sold": 250,
            },
        ]
        client.post("/post_sales/", json=test_data)

        response = client.get("/get_monthly_sales/")
        data = response.get_json()

        vegetables = {d["vegetable"] for d in data}
        assert vegetables.issubset({"tomato", "pear", "brussels sprout"})
