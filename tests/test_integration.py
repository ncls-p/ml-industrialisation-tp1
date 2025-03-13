import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.app import create_app


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
            "/post_data",
            json=[{"year_week": 202001, "vegetable": "tomato", "sales": 100}],
        )
        assert response.status_code == 200

        response = client.post(
            "/post_data",
            json=[{"year_week": 202001, "vegetable": "tomato", "sales": 100}],
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
            "/post_data",
            json=[{"year_week": 202001}],
        )
        assert response.status_code == 400


def test_partial_valid_data(app):
    with app.test_client() as client:
        data = [
            {"year_week": 202001, "vegetable": "tomato", "sales": 100},
            {"year_week": 202002, "vegetable": "carrot", "sales": 150},
            {"year_week": 202003},
            {"year_week": 202004, "vegetable": "potato", "sales": 200},
        ]
        response = client.post("/post_data", json=data)
        assert response.status_code == 400


def test_get_raw_sales(app):
    with app.test_client() as client:
        client.post(
            "/post_data",
            json=[
                {"year_week": 202001, "vegetable": "tomato", "sales": 100},
                {"year_week": 202002, "vegetable": "carrot", "sales": 150},
            ],
        )

        response = client.get("/get_raw_sales")
        assert response.status_code == 200
        data = response.get_json()
        assert len(data) == 2
        assert all(k in data[0] for k in ["year_week", "vegetable", "sales"])


def test_get_monthly_sales(app):
    with app.test_client() as client:
        test_data = [
            {"year_week": 202001, "vegetable": "tomate", "sales": 100},
            {"year_week": 202002, "vegetable": "tomato", "sales": 150},
            {"year_week": 202003, "vegetable": "tomatoes", "sales": 1000},  # outlier
            {"year_week": 202004, "vegetable": "carrot", "sales": 200},
        ]
        client.post("/post_data", json=test_data)

        response = client.get("/get_monthly_sales")
        assert response.status_code == 200
        data = response.get_json()
        assert len(data) > 0
        assert all(
            veg in ["tomato", "carrot"] for d in data for veg in [d["vegetable"]]
        )

        response = client.get("/get_monthly_sales?remove_outliers=true")
        assert response.status_code == 200
        data = response.get_json()
        assert all(not d.get("is_outlier", False) for d in data)


def test_vegetable_name_standardization(app):
    with app.test_client() as client:
        test_data = [
            {"year_week": 202001, "vegetable": "tomate", "sales": 100},
            {"year_week": 202001, "vegetable": "carotte", "sales": 150},
            {"year_week": 202001, "vegetable": "patata", "sales": 200},
        ]
        client.post("/post_data", json=test_data)

        response = client.get("/get_monthly_sales")
        data = response.get_json()
        vegetables = {d["vegetable"] for d in data}
        assert vegetables.issubset({"tomato", "carrot", "potato"})
