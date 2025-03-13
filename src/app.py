import os

import pandas as pd
from flask import Flask, jsonify, request

PATH_CSV = "data/raw/db.csv"


def standardize_vegetable_name(name: str) -> str:
    """Standardize vegetable names to English."""
    translations = {
        "tomate": "tomato",
        "tomatoes": "tomato",
        "carotte": "carrot",
        "zanahoria": "carrot",
        "pomme de terre": "potato",
        "patata": "potato",
        "oignon": "onion",
        "cebolla": "onion",
        "poivron": "pepper",
        "pimiento": "pepper",
    }
    name = name.lower().strip()
    return translations.get(name, name)


def compute_monthly_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Convert weekly sales to monthly sales."""
    if df.empty:
        return pd.DataFrame(columns=["year_month", "vegetable", "sales", "is_outlier"])

    df["date"] = pd.to_datetime(df["year_week"].astype(str) + "0", format="%Y%W%w")

    monthly = (
        df.groupby([df["date"].dt.strftime("%Y%m"), "vegetable"])["sales"]
        .sum()
        .reset_index()
    )
    monthly.columns = ["year_month", "vegetable", "sales"]

    return monthly


def tag_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Tag outliers based on mean + 5*std per vegetable."""
    if df.empty:
        return df

    df["is_outlier"] = False
    for vegetable in df["vegetable"].unique():
        mask = df["vegetable"] == vegetable
        mean = df.loc[mask, "sales"].mean()
        std = df.loc[mask, "sales"].std()
        df.loc[mask, "is_outlier"] = df.loc[mask, "sales"] > (mean + 5 * std)

    return df


def create_app(config=None):
    config = config or {}
    app = Flask(__name__)
    if "CSV_PATH" not in config:
        config["CSV_PATH"] = PATH_CSV
    app.config.update(config)

    @app.route("/post_data", methods=["POST"])
    def post_data():
        data = request.json
        if not isinstance(data, list):
            return jsonify({"error": "Data must be a list of records"}), 400

        required_columns = {"year_week", "vegetable", "sales"}
        for record in data:
            if not isinstance(record, dict) or not all(
                col in record for col in required_columns
            ):
                return jsonify(
                    {"error": "All records must contain required columns"}
                ), 400

        df_new = pd.DataFrame(data)

        if (
            os.path.isfile(app.config["CSV_PATH"])
            and os.path.getsize(app.config["CSV_PATH"]) > 0
        ):
            df = pd.read_csv(app.config["CSV_PATH"])
            df = pd.concat([df, df_new]).drop_duplicates(
                subset=["year_week", "vegetable"]
            )
        else:
            df = df_new

        df.to_csv(app.config["CSV_PATH"], index=False)
        return jsonify({"status": "success"}), 200

    @app.route("/get_raw_sales", methods=["GET"])
    def get_raw_sales():
        if not os.path.isfile(app.config["CSV_PATH"]):
            return jsonify([]), 200

        df = pd.read_csv(app.config["CSV_PATH"])
        return jsonify(df.to_dict(orient="records")), 200

    @app.route("/get_monthly_sales", methods=["GET"])
    def get_monthly_sales():
        remove_outliers = request.args.get("remove_outliers", "false").lower() == "true"

        if not os.path.isfile(app.config["CSV_PATH"]):
            return jsonify([]), 200

        df = pd.read_csv(app.config["CSV_PATH"])

        df["vegetable"] = df["vegetable"].apply(standardize_vegetable_name)

        monthly_df = compute_monthly_sales(df)

        monthly_df = tag_outliers(monthly_df)

        if remove_outliers:
            monthly_df = monthly_df[~monthly_df["is_outlier"]]

        return jsonify(monthly_df.to_dict(orient="records")), 200

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(port=8000)
