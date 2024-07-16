from random import randint
from random import choices
import string
from pymongo import MongoClient
import time

#Connecting to MongoDB Database 'traffic_test' and the 'traffic_col' collection.
client = MongoClient("mongodb://localhost:27017/")
db = client['traffic_test']
traffic_col = db['traffic']

# Generates a random value of either 'vehicle' or 'pedestrian' which will be added to the document when generate_traffic() is called
def traffic_type():
    type_arr = ["Pedestrian", "Vehicle"]
    #the choices() method allows us to add weight to each ped_veh, so we can have more vehicles passing through the intersection than pedestrians
    ped_veh = choices(type_arr, weights = [1,7])[0]
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

def generate_license_plate():
    return ''.join(choices(string.ascii_uppercase + string.digits, k=6))
    

def generate_traffic():
    while True:
        traf_type = traffic_type()
        print(traf_type)
        
        traffic_doc = {
            "traffic_type": traf_type,
            "traffic_direction": traffic_direction()
        }

        if traf_type == 'Vehicle':
            license = generate_license_plate()
            print(license)
            traffic_doc["vehicle_license_plate"] = license


        print(traffic_doc)

        traffic_col.insert_one(traffic_doc) 
        time.sleep(1)
        

generate_traffic()

# To pull up all data from collection in vsCode terminal
# 
# for x in traffic_col.find():
#     print(x)