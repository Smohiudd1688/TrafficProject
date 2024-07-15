from random import randint
from pymongo import MongoClient
import time

#Connecting to MongoDB Database 'traffic_test' and the 'traffic_col' collection.
client = MongoClient("mongodb://localhost:27017/")
db = client['traffic_test']
traffic_col = db['traffic']

# Generates a random value of either 'vehicle' or 'pedestrian' which will be added to the document when generate_traffic() is called
def traffic_type():
    type_arr = ["Pedestrian", "Vehicle"]
    type_num = randint(0,1)
    ped_veh = type_arr[type_num]
    return ped_veh

# Generates a random value of North, South, East, or West which will be added to the document when generate_traffic() is called
def traffic_direction():
    dir_arr = ["North", "South", "East", "West"]

    src_dir_num = randint(0,3)
   
    dest_dir_num = src_dir_num
    while dest_dir_num == src_dir_num:
        dest_dir_num = randint(0,3)


    src_dir = dir_arr[src_dir_num]
    dest_dir = dir_arr[dest_dir_num]

    return {"source_dir": src_dir, "dest_dir": dest_dir}

def generate_traffic():
    counter = 0
    while (counter < 10):
        traffic_doc = {
            "traffic_type": traffic_type(),
            "traffic_direction": traffic_direction()
        }
        
        print(traffic_doc)
    
        traffic_col.insert_one(traffic_doc) 
        time.sleep(2)
        counter = counter + 1

generate_traffic()

# To pull up all data from collection in vsCode terminal
# 
# for x in traffic_col.find():
#     print(x)