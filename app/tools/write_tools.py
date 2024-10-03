import os
import yaml
import json
import asyncio
from redis import asyncio as aioredis
import motor.motor_asyncio

async def get_storage_location(data: dict, meta: dict) -> dict:
    config = dict()
    res = {"db": str(), "col": str()}
    try:
        with open('toolcfg.yaml') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        pass

async def process_write():
    res = {
            "db_name": str(),
            "collection": str()
            }
    try:
    except Exception as e:
        pass
