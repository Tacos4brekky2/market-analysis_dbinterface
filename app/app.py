import pandas as pd
import json
from tools import message_maker, DBConnection
from flask import Flask, request, Response


class DatabaseAPI:
    def __init__(self, flask_app: Flask):
        self.app = flask_app
        self.register_routes()

    def register_routes(self):
        self.app.add_url_rule("/", "home", self.home, methods=["GET"])
        self.app.add_url_rule("/read", "read", self.read, methods=["POST"])
        self.app.add_url_rule("/write", "write", self.write, methods=["POST"])

    def home(self):
        return message_maker("StonksDB API home.", 200)

    def write(self) -> Response:
        json_data = request.json
        debug_values = {
            "Request data": False,
            "Database connection": False,
            "Cache connection": False,
            "Cached data": False,
            "Existing data": False,
            "Data stored": False,
        }
        try:
            if not json_data:
                return message_maker("Request data not provided.", 560, debug_values)
            debug_values["Request data"] = True
            db = DBConnection().connect("mongodb")
            if not db:
                return message_maker("Connection failed: MongoDB.", 560, debug_values)
            debug_values["Database connection"] = True
            cache = DBConnection().connect(dbms="redis")
            if not cache:
                return message_maker("Connection failed: Redis", 560, debug_values)
            debug_values["Cache connection"] = True
            db_name = json_data["cache_info"]["destination"]
            table_name = json_data["meta"]["table"]
            collection_name = json_data["meta"]["collection"]
            collection = db[db_name][collection_name]
            collection_list = [x for x in collection.find()]
            cached_data = json.loads(cache.get(f"{collection_name}:{table_name}"))
            if not cached_data:
                return message_maker("No data found in cache.", 560, debug_values)
            debug_values["Cached data"] = True
            existing_data = dict()
            for entry in collection_list:
                if entry["meta"]["table"] == table_name:
                    existing_data = entry
                    break
            if not existing_data:
                insert_result = collection.insert_one(json_data["data"])
                return message_maker("Database write success.", 200, debug_values)
            debug_values["Existing data"] = True
            df_insert_data = pd.DataFrame(**json_data["data"])
            df_existing_data = pd.DataFrame(**existing_data["data"])
            # Remove all data that is already in the database and insert.
            missing = self.missing_data(
                data=df_insert_data, existing_data=df_existing_data
            )
            if missing.empty:
                return message_maker(
                    "Database write failure: Data already up to date.",
                    560,
                    debug_values,
                )
            else:
                insert_data = {
                    "meta": json_data["meta"],
                    "data": missing.to_dict(orient="split", index=False),
                }
                insert_result = collection.insert_one(insert_data)
                debug_values["Data stored"] = True
                return message_maker("Database write success", 200, debug_values)
        except Exception as e:
            return message_maker(
                "Database write exception.", 560, {**debug_values, **{"exception": e}}
            )

    def read(self) -> Response:
        json_data = request.json
        debug_values = {
            "Request data": False,
            "Database connection": False,
            "Data found": False,
        }
        try:
            if not json_data:
                return message_maker(
                    "Read request data not provided.", 500, debug_values
                )
            debug_values["Request data"] = True
            db = DBConnection().connect("mongodb")
            if not db:
                return message_maker("Connection failed: MongoDB.", 560, debug_values)
            debug_values["Database connection"] = True
            collection = json_data["collection"]
            table = json_data["table"]
            filters = json_data["filters"]
            return_data = db[collection][table].find_one()
            if not return_data:
                return message_maker(
                    "Database read failed: data not found.",
                    560,
                    debug_values,
                )
            debug_values["Data found"] = True
            return message_maker(
                "Database read successful.", 200, debug_values, data=return_data
            )
        except Exception as e:
            return message_maker(
                "Database API read exception.",
                560,
                {**debug_values, **{"exception": e}},
            )

    def missing_data(
        self, data: pd.DataFrame, existing_data: pd.DataFrame
    ) -> pd.DataFrame:
        res = pd.DataFrame()
        if existing_data.empty:
            return data
        mask = data.apply(tuple, 1).isin(existing_data.apply(tuple, 1))
        res = data.mask(mask).dropna()
        return res


if __name__ == "__main__":
    app = Flask(__name__)
    port = 5000
    host = "localhost"
    interface = DatabaseAPI(flask_app=app)
    app.run(host=host, port=port, debug=False)
