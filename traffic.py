from random import randint
from random import choices
from random import choice
import string
from pymongo import MongoClient
import time
import threading

#Connecting to MongoDB Database 'traffic_test' and the 'traffic_col' collection.
client = MongoClient("mongodb://localhost:27017/")
db = client['traffic_test']
traffic_col = db['traffic']

# Generates a random value of either 'vehicle' or 'pedestrian' which will be added to the document when generate_traffic() is called
def traffic_type():
    type_arr = ["Pedestrian", "Vehicle"]
    #the choices() method allows us to add weight to each ped_veh, 
    #so we can have more vehicles passing through the intersection than pedestrians
    ped_veh = choices(type_arr, weights = [1,7])[0]
    return ped_veh

# Generates a random value of North, South, East, or West which will be added to the document when generate_traffic() is called
def traffic_direction():
    dir_arr = ["North", "South", "East", "West"]

    src_dir = choice(dir_arr)
   
    dest_dir = src_dir
    while dest_dir == src_dir:
        dest_dir= choice(dir_arr)

    return {"source_dir": src_dir, "dest_dir": dest_dir}

#Generates a random string of numbers and capital letters for a license plate
def generate_license_plate():
    return ''.join(choices(string.ascii_uppercase + string.digits, k=6))

#Sets the light cycle for the stop lights. Every 30 seconds, it switches, with the 0th index always moving to the end of the array so the loop will continue
current_light = "ns_thru"
def stop_lights():
    light_conditions = ["ns_thru", "ew_thru"]
    while True:
        current_light = light_conditions[0]
        print(current_light)

        light_conditions.append(light_conditions[0])
        light_conditions.pop(0)
        time.sleep(20)

    

def generate_traffic():
    while True:
        traf_type = traffic_type()
        traffic_doc = {
            "traffic_type": traf_type,
            "traffic_direction": traffic_direction()
        }

        #generates a licenseplate number and appends it to the dpcument, only if it is a vehicle.
        if traf_type == 'Vehicle':
            license = generate_license_plate()
            traffic_doc["vehicle_license_plate"] = license


        print(traffic_doc)
        traffic_col.insert_one(traffic_doc)

        time.sleep(1)


# The stop lights and the traffic need to be on two separate threads because they both operate on timers within while loops.
# The below code will run both threads.

# IMPORTANT: in vsCode, `ctrl + c` won't terminate the script, so the processes must be terminated by killing the vscode terminal (click the trashcane)
thread_one = threading.Thread(target=stop_lights)
thread_two = threading.Thread(target=generate_traffic)
thread_one.start()
thread_two.start()



# To pull up all data from collection in vsCode terminal
# 
# for x in traffic_col.find():
#     print(x)