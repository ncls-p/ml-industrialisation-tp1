import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.app_csv import create_app


@pytest.fixture
def app():
    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    temp_csv.close()

    config = {"TESTING": True, "CSV_PATH": temp_csv.name}

    app = create_app(config)

    yield app

    os.remove(temp_csv.name)


def test_post_data(app):
    with app.test_client() as client:
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

        df = pd.read_csv(app.config["CSV_PATH"])
        assert len(df) == 1
        assert df.iloc[0].to_dict() == {
            "year_week": 202001,
            "vegetable": "tomato",
            "sales": 100,
        }


def test_invalid_data(app):
    with app.test_client() as client:
        response = client.post(
            "/post_sales/",
            json=[{"date": "2020-01"}],
        )
        assert response.status_code == 400


def test_partial_valid_data(app):
    with app.test_client() as client:
        data = [
            {"date": "2020-01", "vegetable": "tomato", "kilo_sold": 100},
            {"date": "2020-02", "vegetable": "carrot", "kilo_sold": 150},
            {"date": "2020-03"},
            {"date": "2020-04", "vegetable": "potato", "kilo_sold": 200},
        ]
        response = client.post("/post_sales/", json=data)
        assert response.status_code == 400


def test_get_raw_sales(app):
    with app.test_client() as client:
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


def test_get_monthly_sales(app):
    with app.test_client() as client:
        test_data = [
            {"date": "2020-01", "vegetable": "tomate", "kilo_sold": 100},
            {"date": "2020-02", "vegetable": "tomato", "kilo_sold": 150},
            {"date": "2020-03", "vegetable": "tomatoes", "kilo_sold": 1000},  # outlier
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


def test_vegetable_name_standardization(app):
    with app.test_client() as client:
        test_data = [
            {"date": "2020-01", "vegetable": "tomate", "kilo_sold": 100},
            {"date": "2020-01", "vegetable": "carotte", "kilo_sold": 150},
            {"date": "2020-01", "vegetable": "patata", "kilo_sold": 200},
        ]
        client.post("/post_sales/", json=test_data)

        response = client.get("/get_monthly_sales/")
        data = response.get_json()
        vegetables = {d["vegetable"] for d in data}
        assert vegetables.issubset({"tomato", "carrot", "potato"})
