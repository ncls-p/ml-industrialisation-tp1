import os
from datetime import datetime, timedelta

import pandas as pd
from flask import Flask, jsonify, request

PATH_CSV = "data/raw/db.csv"


def standardize_vegetable_name(name: str) -> str:
    """Standardize vegetable names to English."""
    translations = {
        "tomate": "tomato",
        "tomatoes": "tomato",
        "tomaot": "tomato",
        "tomatto": "tomato",
        "poire": "pear",
        "peer": "pear",
        "pera": "pear",
        "carotte": "carrot",
        "zanahoria": "carrot",
        "pomme de terre": "potato",
        "patata": "potato",
        "oignon": "onion",
        "cebolla": "onion",
        "poivron": "pepper",
        "pimiento": "pepper",
        "brusel sprout": "brussels sprout",
        "brussel sprout": "brussels sprout",
        "brussell sprout": "brussels sprout",
        "brusselsprout": "brussels sprout",
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
        year_week = row["year_week"]
        vegetable = row["vegetable"]
        sales = row["sales"]

        # Parse year and week
        year = year_week // 100
        week = year_week % 100

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

    # Create Bronze, Silver, and Gold CSV files
    bronze_path = os.path.join(
        os.path.dirname(app.config["CSV_PATH"]), "bronze_sales.csv"
    )
    silver_path = os.path.join(
        os.path.dirname(app.config["CSV_PATH"]), "silver_sales.csv"
    )
    gold_path = os.path.join(os.path.dirname(app.config["CSV_PATH"]), "gold_sales.csv")

    @app.route("/init_database", methods=["POST"])
    def init_database():
        """Initialize or reset the CSV files"""
        # Create empty dataframes with proper columns
        bronze_df = pd.DataFrame(columns=["year_week", "vegetable", "sales"])
        silver_df = pd.DataFrame(columns=["year_week", "vegetable", "sales"])
        gold_df = pd.DataFrame(
            columns=["year_month", "vegetable", "sales", "is_outlier"]
        )

        # Save empty dataframes
        bronze_df.to_csv(bronze_path, index=False)
        silver_df.to_csv(silver_path, index=False)
        gold_df.to_csv(gold_path, index=False)

        return jsonify({"status": "Database initialized"}), 200

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

        # Convert input format to internal format
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

        # Step 1: Update Bronze table (raw data)
        if os.path.isfile(bronze_path) and os.path.getsize(bronze_path) > 0:
            bronze_df = pd.read_csv(bronze_path)
            # Ensure idempotency by removing duplicates
            bronze_df = pd.concat(
                [bronze_df, pd.DataFrame(transformed_data)]
            ).drop_duplicates(subset=["year_week", "vegetable"])
        else:
            bronze_df = pd.DataFrame(transformed_data)
        bronze_df.to_csv(bronze_path, index=False)

        # Step 2: Update Silver table (standardized vegetable names)
        silver_data = []
        for record in transformed_data:
            silver_data.append(
                {
                    "year_week": record["year_week"],
                    "vegetable": standardize_vegetable_name(record["vegetable"]),
                    "sales": record["sales"],
                }
            )

        if os.path.isfile(silver_path) and os.path.getsize(silver_path) > 0:
            silver_df = pd.read_csv(silver_path)
            silver_df = pd.concat(
                [silver_df, pd.DataFrame(silver_data)]
            ).drop_duplicates(subset=["year_week", "vegetable"])
        else:
            silver_df = pd.DataFrame(silver_data)
        silver_df.to_csv(silver_path, index=False)

        # Step 3: Update Gold table (monthly aggregated with outlier detection)
        # Calculate monthly sales from all silver data
        monthly_df = compute_monthly_sales(silver_df)

        # Tag outliers
        monthly_df = tag_outliers(monthly_df)

        # Save to gold CSV
        monthly_df.to_csv(gold_path, index=False)

        return jsonify({"status": "success"}), 200

    @app.route("/get_raw_sales/", methods=["GET"])
    def get_raw_sales():
        if not os.path.isfile(bronze_path) or os.path.getsize(bronze_path) == 0:
            return jsonify([]), 200

        df = pd.read_csv(bronze_path)

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

        if not os.path.isfile(gold_path) or os.path.getsize(gold_path) == 0:
            return jsonify([]), 200

        # Read gold data
        gold_df = pd.read_csv(gold_path)

        # Filter outliers if requested
        if remove_outliers:
            gold_df = gold_df[~gold_df["is_outlier"]]

        # Transform to the correct output format
        result = []
        for _, row in gold_df.iterrows():
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
