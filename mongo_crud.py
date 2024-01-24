import datetime
import pymongo
import get_database
from copy import deepcopy
import csv
import threading
import json


def import_backup(site):
    collection = get_database.get_main_db(site)

    # Read the JSON file
    with open('middlesbrough_backup.json') as file:
        data = json.load(file)

    # Insert data into the collection
    result = collection.insert_many(data)

    print(f"Inserted {len(result.inserted_ids)} documents.")


def rename_field(site, old_field_name, new_field_name):
    # Assuming 'client' is your MongoDB client and 'db' is your database
    collection = get_database.get_main_db(site)

    # Update all documents in the collection to rename the specified field
    update_result = collection.update_many(
        {},  # Empty filter (to match all documents)
        {"$rename": {old_field_name: new_field_name}}
    )

    print(f"Field renamed from {old_field_name} to {new_field_name} successfully for"
          f" {update_result.modified_count} documents!")


def get_fields(site):
    # Assuming 'client' is your MongoDB client and 'db' is your database
    collection = get_database.get_main_db(site)

    # Aggregation pipeline to get all fields present in the collection
    pipeline = [
        {
            '$project': {
                'allFields': {
                    '$objectToArray': '$$ROOT'
                }
            }
        },
        {
            '$unwind': '$allFields'
        },
        {
            '$group': {
                '_id': None,
                'fields': {
                    '$addToSet': '$allFields.k'
                }
            }
        }
    ]

    # Perform aggregation to get the list of fields
    result = list(collection.aggregate(pipeline))
    field_count = 0
    # Extract the list of fields from the result
    if result:
        fields_list = result[0]['fields']
        fields_list.sort()
        print("List of fields in the collection:")
        for field in fields_list:
            print(field)
            field_count += 1
        print(f"You have {field_count} fields")
    else:
        print("No documents found in the collection.")


def get_location(site=None, location=None):
    if site and location:
        db = get_database.get_main_db(site.title())
        pattern = f"^{location}"
        projection = {"_id": 0, "sku": 1, "description": 1, "location": 1, "quantity": 1}
        query = {"location": {"$regex": pattern}}
        sort_key = "location"
        matching_documents = db.find(query, projection).sort(sort_key, pymongo.ASCENDING)
        # Define the path for the CSV file
        csv_file_path = f"All in location {location}.csv"
        documents = list(matching_documents)
        for document in documents:
            print(document)
        answer = input("Write to CSV? (Y/N)")
        if answer.capitalize() == "Y":
            # Write data to CSV file
            with open(csv_file_path, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                header = documents[0].keys() if documents else []
                csv_writer.writerow(header)
                for doc in documents:
                    csv_writer.writerow(doc.values())
        else:
            return None


def get_unknown_asset(selected_site, category=None):
    db = get_database.get_main_db(selected_site)

    # Query to find documents
    if category:
        query = {
            "category": category,
            "$or": [
                {"asset_compatibility": {"$exists": False}},
                {"asset_compatibility": {"$in": ["unknown"]}}
            ]
        }
    else:
        query = {
            "$or": [
                {"asset_compatibility": {"$exists": False}},
                {"asset_compatibility": {"$in": ["unknown"]}}
            ]
        }

    # Projection to exclude _id field from the query result
    projection = {
        "_id": 0
    }
    # Execute the query and store the results in a list
    results = list(db.find(query, projection))

    # Define the CSV file name
    csv_file = f'{selected_site} Unknown {category}.csv'

    # Write results to CSV file handling missing fields
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write header row
        header_written = False
        for result in results:
            # Convert asset_compatibility field to a comma-separated string
            asset_compatibility = ', '.join(result.get('asset_compatibility', []))

            # Write header row if not already written
            if not header_written:
                header = list(result.keys())
                header.append('asset_compatibility')  # Add the asset_compatibility field
                writer.writerow(header)
                header_written = True

            # Write data rows
            row = [result.get(field) for field in header[:-1]]  # Exclude asset_compatibility field
            row.append(asset_compatibility)
            writer.writerow(row)

    print(f"CSV data has been written to {csv_file}")


# Scan barcode function for scanning products and locations
def scan_barcode(selected_site, barcode):
    collection_name = get_database.get_main_db(selected_site)
    import re
    if re.findall("^[A-Z]|^[a-z]", barcode):
        result = collection_name.find_one({"location": barcode}, {"_id": 0})
    else:
        result = collection_name.find_one({"sku": barcode}, {"_id": 0})
    if result is None:
        return "No result! Please scan again"
    else:
        return result


# A pointless single use case function for removing none values from the object before adding to the database
def remove_none(dict_object):
    filtered = {k: v for k, v in dict_object.items() if v is not None}
    dict_object.clear()
    dict_object.update(filtered)
    return dict_object


# Full text search through aggregation pipeline (
def search(selected_site):
    collection_name = get_database.get_main_db(selected_site)
    result = collection_name.aggregate({})
    return result


def create(spare):
    collection_name = get_database.get_main_db(spare.site)
    # Auto increment sku
    if not spare.sku:
        new_sku = int
        temp_list = []
        for i in collection_name.find():
            temp_list.append(int(i["sku"]))
            new_sku = str(max(temp_list) + 1).zfill(6)
        spare.sku = new_sku
    # Convert the quantity to an integer value and add to DB or fail whole transaction
    try:
        spare.quantity = int(spare.quantity)
        # Transform comma separated values into a list to us as mongoDB array (unless empty)
        if spare.asset_compatibility is None:
            pass
        else:
            spare.asset_compatibility = spare.asset_compatibility.split(",")

        # Convert the object into a dictionary for mongoDB insert
        insert_object = deepcopy(vars(spare))
        insert_object.pop("site")
        # Remove all default None values (if any) before insert
        remove_none(insert_object)

        # Insert the newly created item
        insert_result = collection_name.insert_one(insert_object)
        # Check if the new entry was correctly inserted
        if insert_result.acknowledged:
            return f"Successfully added {spare.quantity} of {spare.sku} to {spare.site} database"
    except ValueError:
        return "Item NOT inserted! Quantity is non-integer type, please change and submit"


def get_critical_spares(site):
    db = get_database.get_main_db(site)
    query = {"critical": True, "critical_spare_number": {"$exists": True}, "quantity": {"$gt": 0}}
    projection = {
        "critical_spare_number": 1,
        "description": 1,
        "location": 1,
        "quantity": 1,
        "sku": 1,
        "_id": 0
    }
    sort_field = "location"
    results_cursor = db.find(query, projection).sort(sort_field)

    # Convert the results cursor into a list of dictionaries
    results_list = [document for document in results_cursor]
    return results_list


def find_blank_entries(site):
    db = get_database.get_main_db(site)
    # Aggregation pipeline to find documents with only _id field
    pipeline = [
        {
            "$project": {
                "fields_count": {"$size": {"$objectToArray": "$$ROOT"}},
            }
        },
        {
            "$match": {
                "fields_count": 1  # Only _id field will have count as 1
            }
        }
    ]

    # Execute aggregation pipeline
    result = list(db.aggregate(pipeline))

    # Print the documents that have only _id field
    for doc in result:
        print(doc)

    # Extracting _id values from the aggregation result
    ids_to_delete = [doc['_id'] for doc in result]

    # Deleting documents based on the obtained _id values
    delete_result = db.delete_many({"_id": {"$in": ids_to_delete}})

    print(f"{delete_result.deleted_count} documents deleted.")


class MechSeal:
    def __init__(self, site, sku=None, style=None, size=None, supplier=None, description=None, serial_number=None,
                 supplier_code=None, asset_compatibility=None, location=None, quantity=None, critical=None):
        self.site = site
        self.sku = sku
        self.style = style
        self.size = size
        self.supplier = supplier
        self.description = description
        self.serial_number = serial_number
        self.supplier_code = supplier_code
        self.asset_compatibility = asset_compatibility
        self.location = location
        self.quantity = quantity
        self.critical = critical


class SparesObject:
    def __init__(self, site, sku=None, description=None, location=None, quantity=None, critical=None,
                 asset_compatibility=None, category=None, manufacturer=None, supplier=None, supplier_code=None):
        self.site = site
        self.sku = sku
        self.description = description
        self.location = location
        self.quantity = quantity
        self.critical = critical
        self.asset_compatibility = asset_compatibility
        self.category = category
        self.manufacturer = manufacturer
        self.supplier = supplier
        self.supplier_code = supplier_code

    # This is the create method, it requires the name of the site as an argument
    def create(self):
        collection_name = get_database.get_main_db(self.site)
        # Auto increment sku
        if not self.sku:
            new_sku = int
            temp_list = []
            for i in collection_name.find():
                temp_list.append(int(i["sku"]))
                new_sku = str(max(temp_list) + 1).zfill(6)
            self.sku = new_sku
        # Convert the quantity to an integer value and add to DB or fail whole transaction
        try:
            self.quantity = int(self.quantity)
            # Transform comma separated values into a list to us as mongoDB array (unless empty)
            if self.asset_compatibility is None or type(list):
                pass
            else:
                self.asset_compatibility = self.asset_compatibility.split(",")

            # Convert the object into a dictionary for mongoDB insert
            insert_object = deepcopy(vars(self))
            insert_object.pop("site")
            # Remove all default None values (if any) before insert
            remove_none(insert_object)

            # Insert the newly created item
            insert_result = collection_name.insert_one(insert_object)
            # Check if the new entry was correctly inserted
            if insert_result.acknowledged:
                return f"Successfully added {self.quantity} of {self.sku} to {self.site} database"
        except ValueError:
            return "Item NOT inserted! Quantity is non-integer type, please change and submit"

    # This function is for returning the current quantity from the database
    def read_qty(self):
        try:
            collection_name = get_database.get_main_db(self.site)
            result = collection_name.find_one({"sku": self.sku}, {"_id": 0})
            quantity = result["quantity"]
            return quantity
        except TypeError:
            return 0

    # This is the change location method, it requires new location, quantity and site as arguments
    def change_location(self, quantity, new_location=None, new_site=None):
        try:
            quantity = int(quantity)
        except ValueError:
            return "quantity must be an integer"
        setattr(self, "location", new_location)
        # Moving all the spares to a new location
        if quantity == self.read_qty():
            # at the same site
            if new_site is None:
                collection_name = get_database.get_main_db(self.site)
                move_result = collection_name.find_one_and_update({"sku": self.sku},
                                                                  {"$set": {"location": self.location}},
                                                                  return_document=pymongo.ReturnDocument.AFTER)
                if move_result is None:
                    return "ERROR! Sku is not in the list"
                else:
                    return f"New location is {move_result['location']}"
            else:
                # to a new site
                # create a deep copy, so I don't mess up the original link
                copied_item = deepcopy(self)
                # Remove from existing site
                collection_name = get_database.get_main_db(self.site)
                collection_name.delete_one({"sku": self.sku})
                # Check if sku exists at new site
                exist_result = copied_item.exists(new_site)
                # Select new site
                setattr(copied_item, "site", new_site)
                # If it doesn't exist at the new site create it
                if not exist_result:
                    copied_item.create()
                    # If it does exist at the new site update qty
                else:
                    copied_item.change_quantity(quantity)

        # Moving some spares to a new location (only possible to a different site)
        elif quantity <= self.read_qty():
            copied_item = deepcopy(self)
            # warn about dual locations
            if new_site is None:
                return "You cannot store items in more than one location at a site"
            else:
                # Remove from existing location
                self.change_quantity(-quantity)
                # add to the new site
                setattr(copied_item, "site", new_site)
                setattr(copied_item, "quantity", quantity)
                at_new_site = copied_item.change_quantity(quantity)
                if at_new_site is None:
                    setattr(copied_item, "quantity", copied_item.quantity)
                    result = copied_item.create()
                    return result
                else:
                    copied_item.change_quantity(-quantity)
                    setattr(copied_item, "quantity", copied_item.read_qty())
                    result = copied_item.change_quantity(quantity)
                    return result

        # Error handling for if the quantity to move is too large
        else:
            return "Error! You can't move more than you have. Make an adjustment first"

    def change_quantity(self, quantity):
        if self.read_qty() + quantity < 0:
            return "Insufficient Stock"
        else:
            setattr(self, "quantity", self.read_qty() + quantity)
            collection_name = get_database.get_main_db(self.site)
            result = collection_name.find_one_and_update({"sku": self.sku},
                                                         {"$set": {"quantity": self.quantity}},
                                                         return_document=pymongo.ReturnDocument.AFTER)
            return result

    def add_to_used(self, quantity):
        try:
            collection_name = get_database.get_used_db(self.site)
        except TimeoutError:
            return "Unable to connect to used db"
        try:
            # Convert the object into a dictionary for mongoDB insert
            setattr(self, "date_used", datetime.datetime.now())
            insert_object = vars(self)
            delattr(self, "_id")
            insert_object["quantity"] = quantity
            # Insert copy into used
            insert_result = collection_name.insert_one(insert_object)
            delattr(self, "date_used")
            # Check if the new entry was correctly inserted
            if insert_result.acknowledged:
                return "Successfully added to used db"
            else:
                return "Error! Item not inserted (connection was made)"
        except TimeoutError:
            return "Timeout Error! Item not inserted"

    def exists(self, new_site):
        # Check if sku already exists at the new site
        collection_name = get_database.get_main_db(new_site)
        existing_entry = collection_name.find_one({"sku": self.sku}, {"_id": 0})
        if existing_entry is None:
            return False
        else:
            return existing_entry

    def consume(self, quantity):
        self.change_quantity(-quantity)
        self.add_to_used(quantity)
        return f"{quantity} items of {self.sku} consumed"
