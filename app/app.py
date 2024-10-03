import os
import time
import json
import asyncio
from redis import asyncio as aioredis
import motor.motor_asyncio


# Database IO service for https://github.com/Tacos4brekky2/market-analysis_server

INPUT_STREAMS = ["client-out", "updater-out"]
OUTPUT_STREAM = "client-in"
CONSUMER_GROUP = "market-analysis_server"
CONSUMER_NAME = "interface-consumer"

REDIS_PARAMS = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": os.getenv("REDIS_PORT", "6379"),
    "password": os.getenv("REDIS_PASSWORD", ""),
}
MONGO_PARAMS = {
    "host": os.getenv("MONGO_HOST", "localhost"),
    "port": os.getenv("MONGO_PORT", "27017"),
    "password": os.getenv("MONGO_PASSWORD", ""),
}


def deserialize_message(message):
    deserialized_message = dict()
    for key, value in message.items():
        try:
            deserialized_message[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            deserialized_message[key.decode("utf-8")] = value.decode("utf-8")
    return deserialized_message


async def create_redis_groups(redis, input_streams: list):
    for stream in input_streams:
        try:
            await redis.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
        except Exception as e:
            print(f"Error creating consumer group: {e}")


async def consume(redis, mongo, input_streams: list):
    await create_redis_groups(redis=redis, input_streams=input_streams)
    while True:
        messages = await redis.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=CONSUMER_NAME,
            streams={stream: ">" for stream in input_streams},
            count=10,
        )
        for stream, message_list in messages:
            for message_id, message in message_list:
                deserialized_message = deserialize_message(message)
                await handle_message(
                    redis=redis,
                    mongo=mongo,
                    stream=stream,
                    message=deserialized_message,
                    message_id=message_id,
                )


async def handle_message(redis, mongo, stream: str, message, message_id):
    try:
        message_type = message["type"]
        request_id = message["request_id"]
        del message["type"]
        del message["request_id"]
        match message_type:
            case "DATA_FETCHED":
                await write(data=message, mongo=mongo)
                await produce(
                    redis=redis,
                    message_type="DATA_STORED",
                    request_id=request_id,
                    payload={},
                    stream="client-in",
                )
            case "DATA_REQUESTED":
                print(f"Consumed message {message_id}")
                data = await read(params=message, mongo=mongo)
                if data:
                    await produce(
                        redis=redis,
                        request_id=request_id,
                        message_type="DATA_READ",
                        payload=data,
                        stream="client-in",
                    )
                    await redis.xack(stream, CONSUMER_GROUP, message_id)
                else:
                    await produce(
                        redis=redis,
                        request_id=request_id,
                        message_type="FETCH_REQUEST",
                        payload=message,
                        stream="updater-in",
                    )
    except json.JSONDecodeError:
        print("Failed to decode JSON message")
    except Exception as e:
        print(f"Interface error: {e}")


async def produce(redis, request_id, message_type: str, payload: dict, stream: str):
    if isinstance(payload, list):
        return
    else:
        message = {
            k: (json.dumps(v) if isinstance(v, dict) else str(v))
            for k, v in payload.items()
        }
        message["type"] = message_type
        message["request_id"] = request_id
        response = await redis.xadd(stream, fields=message)
        print(f"Produce response: {response}")


async def write(data: dict, mongo):
    print("Writing data")
    storage_data = dict()
    for key, value in data.items():
        if isinstance(key, bytes):
            storage_data[key.decode("utf-8")] = value
        else:
            storage_data[key] = value
    try:
        collection = mongo["stonksdev"]["main"]
        await collection.insert_one(storage_data)
    except Exception as e:
        print(f"Error processing write request: {e}")


async def read(params: dict, mongo) -> dict:
    print("Prcessing read request.")
    document = dict()
    sortkey = params["sortkey"]
    del params["sortkey"]
    try:
        collection = mongo["stonksdev"]["main"]
        cursor = collection.find({"params": params}).sort(sortkey)
        document = await cursor.to_list(length=100)
        document = document[0]
        del document["_id"]
    except Exception as e:
        print(f"Error processing read request: {e}")
        document = dict()
    finally:
        return document


async def main():
    redis = aioredis.from_url(
        f'redis://{REDIS_PARAMS["host"]}:{REDIS_PARAMS["port"]}',
        password=os.getenv("REDIS_PASSWORD", ""),
    )
    mongo = motor.motor_asyncio.AsyncIOMotorClient(
        f'mongodb://{MONGO_PARAMS["host"]}:{MONGO_PARAMS["port"]}'
    )
    try:
        while True:
            await consume(redis=redis, mongo=mongo, input_streams=INPUT_STREAMS)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        print("Interface shut down.")


if __name__ == "__main__":
    asyncio.run(main())
