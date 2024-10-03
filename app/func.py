import motor.motor_asyncio


async def store_data(db, data):
    collection = db["TEST"]
    await collection.insert_one(data)
    print("Data stored in MongoDB")





def format_data(data, api_name: str, category: str, endpoint: str):
    # Implement your data formatting logic here
    return data


def get_table_name(table_val: str, kw: dict) -> str:
    table_name = [kw[x].lower() for x in kw.keys() if x.lower() == table_val.lower()]
    if table_name:
        table_name = str(table_name[0])
        return table_name
    return str()



try:
    data = await response.json()
    print("Found response json.")
    return data
except Exception as e:
    print(e)
try:
    csv_text = await response.text()
    print("Found response csv.  Converting to json.")
    return parse_response_csv(csv_text)
except Exception as e:
    print(e)

