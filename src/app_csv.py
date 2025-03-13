import os
import pandas as pd
import numpy as np
from flask import Flask, jsonify, request
from typing import List, Dict
from datetime import datetime, timedelta

PATH_CSV = "data/raw/db.csv"


def standardize_vegetable_name(name: str) -> str:
    """Standardize vegetable names to English."""
    # Dictionary of translations and corrections
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
    """Convert weekly sales to monthly sales.
    For a week with n days in one month and (7-n) days in the next month,
    allocate n/7 of sales to first month and (7-n)/7 to second month.
    """
    if df.empty:
        return pd.DataFrame(columns=["year_month", "vegetable", "sales", "is_outlier"])

    # Create result dataframe
    result = []

    for _, row in df.iterrows():
        # Use 'date' field instead of year_week
        date_val = row["date"]
        vegetable = row["vegetable"]
        sales = row["kilo_sold"]

        # Parse year and week from date
        date_parts = date_val.split("-")
        year = int(date_parts[0])
        week = int(date_parts[1])
        year_week = year * 100 + week

        # Get the dates for the week
        # First day of the week (Monday)
        # %W format: Week number with the first Monday as the first day of week one
        start_date = datetime.strptime(f"{year}-{week}-1", "%Y-%W-%w")

        # Analyze which months the days of this week belong to
        month_days = {}

        for i in range(7):
            current_date = start_date + timedelta(days=i)
            month_key = current_date.strftime("%Y%m")
            month_days[month_key] = month_days.get(month_key, 0) + 1

        # Distribute sales proportionally to months
        for month, days in month_days.items():
            month_sales = sales * (days / 7.0)
            result.append(
                {"year_month": month, "vegetable": vegetable, "sales": month_sales}
            )

    # Convert to DataFrame and aggregate by month and vegetable
    monthly_df = pd.DataFrame(result)

    if not monthly_df.empty:
        monthly_df = (
            monthly_df.groupby(["year_month", "vegetable"])["sales"].sum().reset_index()
        )

    return monthly_df


def tag_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Tag outliers based on mean + 5*std per vegetable."""
    if df.empty:
        return df

    df["is_outlier"] = False
    for vegetable in df["vegetable"].unique():
        mask = df["vegetable"] == vegetable
        mean = df.loc[mask, "sales"].mean()
        std = df.loc[mask, "sales"].std()
        if not pd.isna(std):  # Avoid division by zero
            df.loc[mask, "is_outlier"] = df.loc[mask, "sales"] > (mean + 5 * std)

    return df


def create_app(config=None):
    config = config or {}
    app = Flask(__name__)
    if "CSV_PATH" not in config:
        config["CSV_PATH"] = PATH_CSV
    app.config.update(config)

    # Ensure data directory exists
    os.makedirs(os.path.dirname(app.config["CSV_PATH"]), exist_ok=True)

    @app.route("/post_sales/", methods=["POST"])
    def post_sales():
        data = request.json
        if not isinstance(data, list):
            return jsonify({"error": "Data must be a list of records"}), 400

        # Validate all records before processing any
        required_columns = {"date", "vegetable", "kilo_sold"}
        for record in data:
            if not isinstance(record, dict) or not all(
                col in record for col in required_columns
            ):
                return jsonify(
                    {"error": "All records must contain required columns"}
                ), 400

        # Convert from new format to internal format
        transformed_data = []
        for record in data:
            # Extract year and week from date string (e.g., "2020-01" for year 2020, week 1)
            date_parts = record["date"].split("-")
            year = int(date_parts[0])
            week = int(date_parts[1])
            year_week = year * 100 + week

            transformed_data.append(
                {
                    "year_week": year_week,
                    "vegetable": record["vegetable"],
                    "sales": record["kilo_sold"],
                }
            )

        # Read existing data if file exists
        if (
            os.path.isfile(app.config["CSV_PATH"])
            and os.path.getsize(app.config["CSV_PATH"]) > 0
        ):
            df = pd.read_csv(app.config["CSV_PATH"])
            # Ensure idempotency by removing duplicates
            df = pd.concat([df, pd.DataFrame(transformed_data)]).drop_duplicates(
                subset=["year_week", "vegetable"]
            )
        else:
            df = pd.DataFrame(transformed_data)

        df.to_csv(app.config["CSV_PATH"], index=False)
        return jsonify({"status": "success"}), 200

    @app.route("/get_raw_sales/", methods=["GET"])
    def get_raw_sales():
        if not os.path.isfile(app.config["CSV_PATH"]):
            return jsonify([]), 200

        df = pd.read_csv(app.config["CSV_PATH"])

        # Transform back to the external format
        result = []
        for _, row in df.iterrows():
            year_week = row["year_week"]
            year = year_week // 100
            week = year_week % 100
            date_str = f"{year}-{week:02d}"

            result.append(
                {
                    "date": date_str,
                    "vegetable": row["vegetable"],
                    "kilo_sold": row["sales"],
                }
            )

        return jsonify(result), 200

    @app.route("/get_monthly_sales/", methods=["GET"])
    def get_monthly_sales():
        remove_outliers = request.args.get("remove_outliers", "false").lower() == "true"

        if not os.path.isfile(app.config["CSV_PATH"]):
            return jsonify([]), 200

        # Read and process data
        df = pd.read_csv(app.config["CSV_PATH"])

        # Create a temporary dataframe with the date field
        temp_df = []
        for _, row in df.iterrows():
            year_week = row["year_week"]
            year = year_week // 100
            week = year_week % 100
            date_str = f"{year}-{week:02d}"

            temp_df.append(
                {
                    "date": date_str,
                    "vegetable": row["vegetable"],
                    "kilo_sold": row["sales"],
                }
            )

        temp_df = pd.DataFrame(temp_df)

        # Standardize vegetable names
        temp_df["vegetable"] = temp_df["vegetable"].apply(standardize_vegetable_name)

        # Convert to monthly sales
        monthly_df = compute_monthly_sales(temp_df)

        # Tag outliers
        monthly_df = tag_outliers(monthly_df)

        # Filter outliers if requested
        if remove_outliers:
            monthly_df = monthly_df[~monthly_df["is_outlier"]]

        # Transform to the right output format
        result = []
        for _, row in monthly_df.iterrows():
            year_month = row["year_month"]
            year = int(year_month[:4])
            month = int(year_month[4:])
            month_str = f"{year}-{month:02d}"

            result.append(
                {
                    "date": month_str,
                    "vegetable": row["vegetable"],
                    "kilo_sold": row["sales"],
                    "is_outlier": bool(row["is_outlier"]),
                }
            )

        return jsonify(result), 200

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(port=8000)
