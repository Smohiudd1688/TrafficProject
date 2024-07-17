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
    ped_veh = choices(type_arr, weights = [0.1, 0.9])[0]
    return ped_veh


# Generates a random value of North, South, East, or West which will be added to the document when generate_traffic() is called
def traffic_direction():
    dir_arr = ["North", "South", "East", "West"]

    src_dir = choice(dir_arr)
   
    dest_dir = src_dir
    while dest_dir == src_dir:
        dest_dir= choice(dir_arr)

    return f"{src_dir}/{dest_dir}"


#generates speed for the vehicle, giving more weight to speeds within the speed limit of 35 mph
def traffic_speed():
    thru_traf_speeds_arr = [randint(30,45), randint(46, 55)]
    vehicle_speed = choices(thru_traf_speeds_arr, weights = [0.9, 0.1])[0]
    return {"speed_limit_mph": 35,
            "vehicle_speed_mph": vehicle_speed}


#Generates a random string of numbers and capital letters for a license plate
def generate_license_plate():
    license_state_arr = ["Arizona", "California", "Nevada", "Utah", "New Mexico"]

    license_num = ''.join(choices(string.ascii_uppercase + string.digits, k=6))
    license_state = ''.join(choices(license_state_arr, weights = [0.85, 0.07, 0.03, 0.02, 0.03]))

    return {"license_plate_num": license_num,
            "license_plate_state": license_state}


#Sets the light cycle for the stop lights. Every 30 seconds, it switches, with the 0th index always moving to the end of the array so the loop will continue
current_light = "North/South_South/North"
def stop_lights():
    global current_light
    light_conditions = ["North/South_South/North", "East/West_West/East"]
    while True:
        current_light = light_conditions[0]
        print(current_light)

        light_conditions.append(light_conditions[0])
        light_conditions.pop(0)
        time.sleep(7)

    

def generate_traffic():
    while True:
        traf_type = traffic_type()
        traffic_doc = {
            "traffic_type": traf_type,
            "enter_dir/exit_dir": traffic_direction(),
            "green_light_direction": current_light
        }

        #generates a license plate and vehicle speed and appends it to the document, only if it is a vehicle.
        if traf_type == 'Vehicle':
            license = generate_license_plate()
            traffic_doc["vehicle_license_plate"] = license

            speed = traffic_speed()
            traffic_doc["speed_data"] = speed


        print(traffic_doc)
        traffic_col.insert_one(traffic_doc)

        time.sleep(1)


# The stop lights and the traffic need to be on two separate threads because they both operate on timers within while loops.
# The below code will run both threads.

# IMPORTANT: in vsCode, `ctrl + c` won't terminate the script, so the processes must be terminated by killing the vscode terminal (click the trashcan)
thread_one = threading.Thread(target=stop_lights)
thread_two = threading.Thread(target=generate_traffic)
thread_one.start()
thread_two.start()





# To pull up all data from collection in vsCode terminal
# 
# for x in traffic_col.find():
#     print(x)