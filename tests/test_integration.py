import os
import pandas as pd
import pytest
import tempfile

from app import create_app

@pytest.fixture
def app():
    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_csv.close()

    config = {"TESTING": True, "CSV_PATH": temp_csv.name}

    app = create_app(config)

    yield app

    os.remove(temp_csv.name)


def test_post_data(app):
    with app.test_client() as client:
        response = client.post(
            "post_data",
            json={"year_week": 202001, "vegetable": "tomato", "sales": 100},
        )

    assert response.status_code == 200

    df = pd.read_csv(app.config['CSV_PATH'])
    assert not df.empty
    assert df.iloc[-1].to_dict() == {"year_week": 202001, "vegetable": "tomato", "sales": 100}
