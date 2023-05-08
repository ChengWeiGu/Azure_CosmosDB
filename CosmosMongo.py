import numpy as np
import pandas as pd
import os
from dotenv import load_dotenv
import pymongo
import datetime
import time
import random
load_dotenv()

mongo_conn_str = os.getenv("MONGO_PRIMARY_CONNECTION_STRING")

def create_database_if_not_exist(DB_NAME):
    client = pymongo.MongoClient(mongo_conn_str)
    ## Create database if it doesn't exist
    db = client[DB_NAME]
    if DB_NAME not in client.list_database_names():
        # Create a database with 400 RU throughput that can be shared across
        # the DB's collections
        db.command({"customAction": "CreateDatabase", "offerThroughput": 400})
        print("Created db '{}' with shared throughput.\n".format(DB_NAME))
    else:
        print("Using database: '{}'.\n".format(DB_NAME))
    return db

def create_collection_if_not_exist(DB,COLLECTION_NAME):
    ## Create collection if it doesn't exist
    db = DB
    collection = db[COLLECTION_NAME]
    if COLLECTION_NAME not in db.list_collection_names():
        # Creates a unsharded collection that uses the DBs shared throughput
        db.command(
            {"customAction": "CreateCollection", "collection": COLLECTION_NAME}
        )
        print("Created collection '{}'.\n".format(COLLECTION_NAME))
    else:
        print("Using collection: '{}'.\n".format(COLLECTION_NAME))
    return collection


if __name__ == "__main__":
    DB_NAME="test_db0420"
    COLLECTION_NAME="collect1"
    '''
    ## Example1: insert a data to a container
    db = create_database_if_not_exist(DB_NAME)
    collection = create_collection_if_not_exist(db,COLLECTION_NAME)
    product = {
    "category": "gear-surf-surfboards",
    "name": "Yamba Surfboard-{}".format(random.randint(50, 5000)),
    "quantity": 1,
    "sale": False,
    }
    collection.insert_one(product)
    collection.insert_one({"name":"TestName","amt":random.randint(50, 5000)})
    '''

    '''
    ## Example2: query your data by filter
    db = create_database_if_not_exist(DB_NAME)
    collection = create_collection_if_not_exist(db,COLLECTION_NAME)
    # insert data
    insert_result = collection.insert_one({"name":"TestName","amt":random.randint(50, 5000)})
    insert_id = insert_result.inserted_id
    print("data id: %s"%insert_id)
    # find data
    filter = {"_id":insert_id}
    doc = collection.find_one(filter)
    print("Found a document with _id {}: {}\n".format(insert_id, doc))
    '''

    ## Example3: update your data
    db = create_database_if_not_exist(DB_NAME)
    collection = create_collection_if_not_exist(db,COLLECTION_NAME)
    # update the data by the new data
    product = {
    "category": "gear-surf-surfboards",
    "name": "Yamba Surfboard-{}".format(random.randint(50, 5000)),
    "quantity": 520,
    }
    filter = {"name":"Yamba Surfboard-1579"}
    update_result = collection.update_one(filter, {"$set": product})
    print("Upserted document with _id {}\n".format(update_result.upserted_id))


