from flask import Response, jsonify, make_response
from collections import defaultdict
import toml
import os


def message_maker(
    return_message: str,
    return_code: int,
    debug_values: dict = dict(),
    response: bool = True,
    data = None
    ) -> Response:
    message = {
        "message": return_message,
        "return code": return_code,
        "data": data
        } 
    response_message = {**message, **debug_values} if debug_values else message
    if not response:
        message = {
            "Return message": "",
            "Data": dict(),
            "Formatter class": False,
            "Formatter": False,
            "Status code": 500,
        }
        return jsonify(response_message)
    return make_response(jsonify(response_message), return_code)

def load_api_configs(config_dir: str) -> dict:
    try:
        api_names = [x for x in os.listdir(config_dir)]
        data = defaultdict()
        for api_name in api_names:
            api_path = os.path.join(config_dir, api_name)
            data[api_name] = defaultdict()
            category_files = [x for x in os.listdir(api_path)]
            for category_file in category_files:
                file_path = os.path.join(api_path, category_file)
                file = toml.load(file_path)
                category = category_file.strip(".toml")
                data[api_name][category] = file
    except Exception as e:
        print(e)
        return dict()
    return data
