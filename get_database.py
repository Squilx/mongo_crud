import pymongo
from config import connection_string

def get_database(selected_site):
    site = ""
    if selected_site == "Stores":
        site = "stores"
    if selected_site == "Middlesbrough":
        site = "stores_middlesbrough"
    elif selected_site == "Billingham":
        site = "stores_billingham"
    client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    try:
        return client[site]
    except TimeoutError:
        print("Unable to connect to server")


def get_main_db(selected_site):
    site = ""
    if selected_site == "Stores":
        site = "stores"
    if selected_site == "Middlesbrough":
        site = "stores_middlesbrough"
    elif selected_site == "Billingham":
        site = "stores_billingham"
    elif selected_site == "Test":
        site = "backup_test"
    client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    try:
        dbname = client[site]
        # Select the relevant spares list depending on the site
        spares_list = ""
        if selected_site == "Stores":
            spares_list = "items"
        if selected_site == "Middlesbrough":
            spares_list = "spares_middlesbrough"
        elif selected_site == "Billingham":
            spares_list = "spares_billingham"
        elif selected_site == "Test":
            spares_list = "backup_test"
        else:
            pass
        return dbname[spares_list]
    except TimeoutError:
        print("Unable to connect to server")


def get_used_db(selected_site):
    site = ""
    if selected_site == "Middlesbrough":
        site = "stores_middlesbrough"
    elif selected_site == "Billingham":
        site = "stores_billingham"
    client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    try:
        dbname = client[site]
        # Select the relevant spares list depending on the site
        spares_list = ""
        if selected_site == "Middlesbrough":
            spares_list = "used_middlesbrough"
        elif selected_site == "Billingham":
            spares_list = "used_billingham"
        else:
            pass
        return dbname[spares_list]
    except TimeoutError:
        print("Unable to connect to server")

