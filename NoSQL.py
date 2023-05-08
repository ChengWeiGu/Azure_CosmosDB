import os
import json
from azure.cosmos import CosmosClient, PartitionKey
from dotenv import load_dotenv
load_dotenv()

endpoint = os.getenv("ENDPOINT")
key = os.getenv("KEY")

## create database by giving name
def create_database(database_name = "chatgpt_albert"):
    client = CosmosClient(url=endpoint, credential=key)
    database = client.create_database_if_not_exists(id=database_name)
    print("Database\t", database.id)
    print("create db success")
    return database

## create container by giving both database name and container names
## note that partition key should be unique!
def create_container(database_name = "chatgpt_albert",container_name="chatgpt_api_header",partition_key="/categoryId"):
    client = CosmosClient(url=endpoint, credential=key)
    database = client.create_database_if_not_exists(id=database_name)
    print("Database\t", database.id)   
    key_path = PartitionKey(path=partition_key)
    container = database.create_container_if_not_exists(
        id=container_name,
        partition_key=key_path,
        offer_throughput=400)
    print("Container\t", container.id)
    print("create container success")
    return container

# get database by name
def get_database(database_name = "chatgpt_albert"):
    client = CosmosClient(url=endpoint, credential=key)
    database = client.get_database_client(database_name)
    print("Database\t", database.id)   
    return database

# get container by name
def get_container(database_name, container_name):
    client = CosmosClient(url=endpoint, credential=key)
    database = client.get_database_client(database_name)
    print("Database\t", database.id)   
    container = database.get_container_client(container_name)
    print("Container\t", container.id)
    return container

# list containers under a db
def list_DB_container(database_name):
    client = CosmosClient(url=endpoint, credential=key)
    database = client.get_database_client(database_name)
    print("Database\t", database.id)
    containers = database.list_containers()
    for container in containers:
        print("Containers:\t",container['id'])

##################################################
### Eample to get data by id and partition_key ###
# Specify the var: ###############################
# database = chatgpt_albert ######################
# container = chatgpt_api_record #################
##################################################
def example_get_data():
    container = get_container("chatgpt_albert","chatgpt_api_record")
    id = "15aae9ae-e558-46df-9973-6e1bb94a08e2"
    partition_key = "20230418095249942992"
    existing_item = container.read_item(
        item=id,
        partition_key=partition_key,
        )
    print("Point read\t", existing_item)

#############################################
### Example to query data by SQL language ###
#############################################
def example_query_data():
    container = get_container("chatgpt_albert","chatgpt_api_record")
    query_sql = "SELECT * FROM chatgpt_api_record r where r.id = @id and r.date_index = @date_index"
    id = "Chat_key_7"
    date_index = "20230413150940299222"
    params = [dict(name="@id",value=id),dict(name="@date_index",value=date_index)]
    results = container.query_items(
        query=query_sql, parameters=params, enable_cross_partition_query=True
    )
    items = [item for item in results]
    print("total %d items estimated:"%(len(items)))
    for item in items:
        print(item)

#############################################
### Example to insert data to a container ###
##### note that partition_key is unique #####
#############################################
def example_insert_data():
    container = get_container("testdb_0508","chatgpt")
    data = {
        "chat_key": "e8a2e0ba-59bf-41ea-8697-8afc9c972755",
        "date_index": "20230419164334640358",
        "message_question": "你好",
        "message_answer": "你好！有什么我可以帮助你的吗？",
        "update_time": 1681922617203,
        "last_message": None,
        "completion_tokens": 18,
        "prompt_tokens": 2065,
        "total_tokens": 2083,
        "openai_api_start_time": 1681922614657,
        "openai_api_end_time": 1681922617203,
        "openai_api_time_spent_sec": 2,
        "model": "gpt-35-turbo",
        "object": "chat.completion",
        "start_time": None,
        "end_time": None,
        "time_spent_sec": None,
        "id": "e8a2e0ba-59bf-41ea-8697-8afc9c972755",
        "categoryId": "20230419164334640358", # must be unique
    }
    try:
        container.create_item(data)
        print("create item done")
    except Exception as e:
        print("got an error: ",e)


#################################################
### Example to delete all data in a container ###
#################################################
def example_delete_data():
    container = get_container("testdb_0508","chatgpt")
    query = "SELECT * FROM chatgpt"
    items = container.query_items(query, enable_cross_partition_query=True)
    for item in items:
        container.delete_item(item, partition_key=item['categoryId'])
    print("delete done")


if __name__ == "__main__":
    ### Function Reference ###
    create_database(database_name="testdb_0508")
    create_container(database_name = "testdb_0508",container_name="chatgpt")
    db = get_database(database_name = "chatgpt_albert")
    container = get_container(database_name = "chatgpt_albert",container_name = "chatgpt_api_header")
    list_DB_container(database_name = "chatgpt_albert")
    ##########################
    
    ####### Examples ########
    example_get_data()
    example_query_data()
    example_insert_data()
    example_delete_data()
    #########################



