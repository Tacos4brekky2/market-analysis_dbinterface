from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import redis
import os
import toml
from collections import defaultdict


def get_secret(secret_key: str, debug: bool = False) -> str:
    if os.getenv("STONKS_DB_DEV", "") == "true":
        debug = True
    # For dev environment use, set environment variables to {key}_DEV
    if debug:
        secret_key += "_DEV"
    # Check for secret in environment variables.
    env_secret = os.getenv(secret_key, "")
    if env_secret:
        return env_secret


def get_headers(
    api_name: str, secrets_path: str = "tools/secrets.toml", debug: bool = False
) -> dict:
    if os.getenv("STONKS_DB_DEV", "") == "true":
        debug = True
    headers = defaultdict()
    try:
        secrets_file = toml.load(secrets_path)
        headers_template = secrets_file[api_name]["headers"]
        for key, value in headers_template.items():
            print(key, value)
            match value[1]:
                case "secret":
                    secret_value = get_secret(secret_key=value[0], debug=debug)
                    if secret_value:
                        headers[key] = secret_value
                case "public":
                    headers[key] = value[0]
    except Exception as e:
        print(e)
    return headers


def get_credentials(
    service_name: str, credentials_config_path: str = "tools/secrets.toml"
):
    # Load the portion of the credentials config file that stores
    # connection parameters and their environment variables.
    template = dict()
    try:
        credentials_config = toml.load(credentials_config_path)
        if service_name not in credentials_config.keys():
            return dict()
        template = credentials_config[service_name]
        # Pulls values from environment variables and creates the return parameters.
        credentials = defaultdict()
        if template:
            for key, secret_key in template.items():
                secret = get_secret(secret_key=str(secret_key))
                if secret:
                    credentials[str(key)] = secret
                else:
                    credentials = dict()
                    break
        return credentials
    except Exception as e:
        print(e)
    return dict()


class DBConnection:
    def __init__(
        self, debug: bool = False
    ):
        # Creates a database connection based on input connection parameters.
        # If connection parameters are not provided, they will be read from 'secrets.toml'
        self.debug = debug
        self.dbms_list = {"mongodb": MongoClient, "redis": redis.Redis}

    def connect(self, dbms: str, connection_parameters: dict = dict()):
        try:
            if dbms not in self.dbms_list.keys():
                self.exception = f"Invalid DBMS name: {dbms}"
                return
            database_class = self.dbms_list[dbms]
            if not connection_parameters:
                connection_parameters = get_credentials(service_name=dbms)
            if connection_parameters:
                connection = database_class(**connection_parameters)
                if self.ping_database(dbms, connection):
                    return connection
        except Exception as e:
            print(e)
            return

    def ping_database(self, dbms: str, connection) -> bool:
        match dbms:
            case "mongodb":
                try:
                    connection.admin.command("ping")
                    return True
                except ConnectionFailure as e:
                    self.exception = e
                    return False
            case "redis":
                if connection.ping():
                    return True
                else:
                    self.exception = "Redis ping failed."
                    return False
        return False
