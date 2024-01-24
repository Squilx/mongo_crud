from config import connection_string
from pymongo import MongoClient

def get_critical_spares(site):
    # Connect to MongoDB
    client = MongoClient(connection_string)
    if site == "Middlesbrough":
        database = "stores_middlesbrough"
        col = "spares_middlesbrough"
    elif site == "Billingham":
        database = "stores_billingham"
        col = "spares_billingham"
    db = client[database]
    collection = db[col]

    # Define the projection (fields to return)
    projection = {
        "_id": 0,
        "sku": 1,
        "description": 1,
        "critical_spare_number": 1,
        "quantity": 1,
        "location": 1,
    }

    # Define a query filter to select documents with "critical_spare_number" field
    filter = {"critical_spare_number": {"$exists": True}}

    # Query the collection, apply projection, and filter documents
    results = collection.find(filter, projection)

    # Sort the results by "location" in ascending order
    results.sort("location", 1)  # 1 for ascending, -1 for descending

    return results
