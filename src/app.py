from flask import Flask, request, jsonify
import pandas as pd
from pathlib import Path
import os

PATH_CSV = "data/raw/db.csv"

def create_app(config=None):
    config = config or {}
    app = Flask(__name__)

    if "CSV_PATH" not in config:
        config["CSV_PATH"] = PATH_CSV

    app.config.update(config)

    @app.route('/post_data', methods=['POST'])
    def post_data():
        data = request.json

        if os.path.isfile(app.config['CSV_PATH']) and os.path.getsize(app.config['CSV_PATH']) > 0:
            df = pd.read_csv(app.config['CSV_PATH'])
            df = df.append(data, ignore_index=True)
        else:
            df = pd.DataFrame([data])

        df.to_csv(app.config['CSV_PATH'], index=False)

        return jsonify({"status": "success"}), 200

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(port=8000)
