import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
from flask import Flask, jsonify, request

DATABASE_PATH = "data/raw/sales.db"


def standardize_vegetable_name(name: str) -> str:
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


def create_tables(connection):
    cursor = connection.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bronze_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year_week INTEGER,
        vegetable TEXT,
        sales REAL,
        UNIQUE(year_week, vegetable)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS silver_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year_week INTEGER,
        vegetable TEXT,
        sales REAL,
        UNIQUE(year_week, vegetable)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gold_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT,
        vegetable TEXT,
        sales REAL,
        is_outlier INTEGER,
        UNIQUE(year_month, vegetable)
    )
    """)

    connection.commit()


def compute_monthly_sales(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["year_month", "vegetable", "sales", "is_outlier"])

    result = []

    for _, row in df.iterrows():
        year_week = row["year_week"]
        vegetable = row["vegetable"]
        sales = row["sales"]

        year = year_week // 100
        week = year_week % 100

        start_date = datetime.strptime(f"{year}-{week}-1", "%Y-%W-%w")

        month_days = {}

        for i in range(7):
            current_date = start_date + timedelta(days=i)
            month_key = current_date.strftime("%Y%m")
            month_days[month_key] = month_days.get(month_key, 0) + 1

        for month, days in month_days.items():
            month_sales = sales * (days / 7.0)
            result.append(
                {"year_month": month, "vegetable": vegetable, "sales": month_sales}
            )

    monthly_df = pd.DataFrame(result)

    if not monthly_df.empty:
        monthly_df = (
            monthly_df.groupby(["year_month", "vegetable"])["sales"].sum().reset_index()
        )

    return monthly_df


def tag_outliers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df["is_outlier"] = False
    for vegetable in df["vegetable"].unique():
        mask = df["vegetable"] == vegetable
        mean = df.loc[mask, "sales"].mean()
        std = df.loc[mask, "sales"].std()
        if not pd.isna(std):
            df.loc[mask, "is_outlier"] = df.loc[mask, "sales"] > (mean + 5 * std)

    return df


def create_app(config=None):
    config = config or {}
    app = Flask(__name__)

    if "DATABASE_PATH" not in config:
        config["DATABASE_PATH"] = DATABASE_PATH
    app.config.update(config)

    os.makedirs(os.path.dirname(app.config["DATABASE_PATH"]), exist_ok=True)

    with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
        create_tables(conn)

    @app.route("/init_database", methods=["POST"])
    def init_database():
        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS bronze_sales")
            cursor.execute("DROP TABLE IF EXISTS silver_sales")
            cursor.execute("DROP TABLE IF EXISTS gold_sales")
            create_tables(conn)

        return jsonify({"status": "Database initialized"}), 200

    @app.route("/post_sales/", methods=["POST"])
    def post_sales():
        data = request.json
        if not isinstance(data, list):
            return jsonify({"error": "Data must be a list of records"}), 400

        required_columns = {"date", "vegetable", "kilo_sold"}
        for record in data:
            if not isinstance(record, dict) or not all(
                col in record for col in required_columns
            ):
                return jsonify(
                    {"error": "All records must contain required columns"}
                ), 400

        transformed_data = []
        for record in data:
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

        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            cursor = conn.cursor()
            for record in transformed_data:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO bronze_sales (year_week, vegetable, sales)
                    VALUES (?, ?, ?)
                    """,
                    (record["year_week"], record["vegetable"], record["sales"]),
                )

            for record in transformed_data:
                std_vegetable = standardize_vegetable_name(record["vegetable"])
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO silver_sales (year_week, vegetable, sales)
                    VALUES (?, ?, ?)
                    """,
                    (record["year_week"], std_vegetable, record["sales"]),
                )

            silver_df = pd.read_sql_query("SELECT * FROM silver_sales", conn)

            if not silver_df.empty:
                monthly_df = compute_monthly_sales(silver_df)

                monthly_df = tag_outliers(monthly_df)

                for _, row in monthly_df.iterrows():
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO gold_sales (year_month, vegetable, sales, is_outlier)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            row["year_month"],
                            row["vegetable"],
                            row["sales"],
                            int(row["is_outlier"]),
                        ),
                    )

            conn.commit()

        return jsonify({"status": "success"}), 200

    @app.route("/get_raw_sales/", methods=["GET"])
    def get_raw_sales():
        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            df = pd.read_sql_query(
                "SELECT year_week, vegetable, sales FROM bronze_sales", conn
            )

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

        with sqlite3.connect(app.config["DATABASE_PATH"]) as conn:
            query = "SELECT year_month, vegetable, sales, is_outlier FROM gold_sales"

            if remove_outliers:
                query += " WHERE is_outlier = 0"

            df = pd.read_sql_query(query, conn)

            if not df.empty:
                df["is_outlier"] = df["is_outlier"].astype(bool)

        result = []
        for _, row in df.iterrows():
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
