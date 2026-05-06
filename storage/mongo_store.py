import os
from pymongo import MongoClient


def save_to_mongo(record):
    mongo_uri = os.getenv("MONGO_URI")

    if not mongo_uri:
        print("MongoDB not configured. Skipping MongoDB save.")
        return

    client = MongoClient(mongo_uri)

    db = client["green_devops_monitor"]
    collection = db["pipeline_metrics"]

    collection.insert_one(record)

    client.close()

    print("Record saved to MongoDB.")