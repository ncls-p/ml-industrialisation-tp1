import os

import pandas as pd
from flask import Flask, jsonify, request

PATH_CSV = "data/raw/db.csv"


def create_app(config=None):
    config = config or {}
    app = Flask(__name__)

    if "CSV_PATH" not in config:
        config["CSV_PATH"] = PATH_CSV

    app.config.update(config)

    @app.route("/post_data", methods=["POST"])
    def post_data():
        data = request.json
        df_new = pd.DataFrame(data)

        if (
            os.path.isfile(app.config["CSV_PATH"])
            and os.path.getsize(app.config["CSV_PATH"]) > 0
        ):
            df = pd.read_csv(app.config["CSV_PATH"])
            df = pd.concat([df, df_new])
        else:
            df = df_new

        df.to_csv(app.config["CSV_PATH"], index=False)

        return jsonify({"status": "success"}), 200

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(port=8000)
