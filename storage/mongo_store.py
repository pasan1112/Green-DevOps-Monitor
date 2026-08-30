import os


def _get_mongo_client():
    try:
        from pymongo import MongoClient
    except ImportError:
        print("pymongo is not installed. Skipping MongoDB operations.")
        return None

    return MongoClient


def save_to_mongo(record):
    mongo_uri = "mongodb+srv://admin:admin1234@green-devops-monitor.xxflzzs.mongodb.net/"
    mongo_client_cls = _get_mongo_client()

    if not mongo_uri or mongo_client_cls is None:
        print("MongoDB not configured. Skipping MongoDB save.")
        return False

    client = None
    try:
        client = mongo_client_cls(mongo_uri, serverSelectionTimeoutMS=3000)
        db = client["green_devops_monitor"]
        collection = db["pipeline_metrics"]

        collection.insert_one(record)

        print("Record saved to MongoDB.")
        return True
    except Exception as exc:
        print(f"MongoDB save failed. Skipping MongoDB save: {exc}")
        return False
    finally:
        if client is not None:
            client.close()



def update_stage_record(run_id, stage, updates):
    """Update the most recent stage record for a run with extra Jenkins timing metadata."""
    mongo_uri = "mongodb+srv://admin:admin1234@green-devops-monitor.xxflzzs.mongodb.net/"
    mongo_client_cls = _get_mongo_client()

    if not mongo_uri or mongo_client_cls is None:
        print("MongoDB not configured. Skipping MongoDB update.")
        return False

    client = mongo_client_cls(mongo_uri)
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
