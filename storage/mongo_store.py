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


def update_stage_record(run_id, stage, updates):
    """Update the most recent stage record for a run with extra Jenkins timing metadata."""
    mongo_uri = os.getenv("MONGO_URI")

    if not mongo_uri:
        print("MongoDB not configured. Skipping MongoDB update.")
        return False

    client = MongoClient(mongo_uri)
    db = client["green_devops_monitor"]
    collection = db["pipeline_metrics"]

    existing = collection.find_one(
        {"run_id": run_id, "stage": stage},
        sort=[("end_timestamp", -1)],
    )
    if not existing:
        client.close()
        print("No MongoDB stage record found to update.")
        return False

    collection.update_one({"_id": existing["_id"]}, {"$set": updates})
    client.close()

    print("MongoDB stage record updated.")
    return True
