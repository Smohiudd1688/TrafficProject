from random import randint
from random import choices
from random import choice
import string
from pymongo import MongoClient
import time
import threading

#Connecting to MongoDB Database 'traffic_test' and the 'traffic_col' collection.
client = MongoClient("mongodb://localhost:27017/")
db = client["traffic_test"]
traffic_col = db["traffic"]
queues_col = db["queues"]


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
   
    if src_dir == "North":
        dest_dir = "South"
    elif src_dir == "South":
        dest_dir = "North"
    elif src_dir == "East":
        dest_dir = "West"
    else:
        dest_dir = "East"

    return f"{src_dir}/{dest_dir}"


#generates speed for the vehicle, giving more weight to speeds within the speed limit of 35 mph
def traffic_speed():
    thru_traf_speeds_arr = [randint(31,45), randint(46, 55)]
    vehicle_speed = choices(thru_traf_speeds_arr, weights = [0.85, 0.15])[0]
    return {"speed_limit_mph": 35,
            "vehicle_speed_mph": vehicle_speed}


#Generates a random string of numbers and capital letters for a license plate
def generate_license_plate():
    license_state_arr = ["Arizona", "California", "Nevada", "Utah", "New Mexico"]

    license_num = "".join(choices(string.ascii_uppercase + string.digits, k=6))
    license_state = "".join(choices(license_state_arr, weights = [0.80, 0.09, 0.04, 0.03, 0.04]))

    return {"license_plate_num": license_num,
            "license_plate_state": license_state}


#Sets the light cycle for the stop lights. Every 30 seconds, it switches, with the 0th index always moving to the end of the array so the loop will continue
current_light = "North/South_South/North"

red_light_queues = {
    "North/South": [],
    "South/North": [],
    "East/West": [],
    "West/East":[]
}

  #for loop to loop through red_light_queues. If the key is in the current_light, loop through the array in that key's value to add each vehicle to the collection. This will only dump it unless we run it in a separate thread.
def empty_red_light_queues():
    for key in red_light_queues:
            if key in current_light:
                for element in red_light_queues[key]:
                    element["green_light_direction"] = current_light
                    traffic_col.insert_one(element)
                red_light_queues[key] = []


def stop_lights():
    global current_light
    global red_light_queues
    light_conditions = ["North/South_South/North", "East/West_West/East"]
    while True:
        current_light = light_conditions[0]
        print(current_light)

        empty_red_light_queues()

        light_conditions.append(light_conditions[0])
        light_conditions.pop(0)
        time.sleep(30)



def generate_traffic():
    global red_light_queues
    global current_light
    
    while True:
        traf_type = traffic_type()
        traf_dir = traffic_direction()
        traffic_doc = {
            "traffic_type": traf_type,
            "enter_dir-exit_dir": traf_dir,
            "green_light_direction": current_light
        }

        #generates a license plate and vehicle speed and appends it to the document, only if it is a vehicle.
        if traf_type == "Vehicle":
            license = generate_license_plate()
            traffic_doc["vehicle_license_plate"] = license

            speed = traffic_speed()
            traffic_doc["speed_data"] = speed

        red_light_runner = randint(1,4)

        if traf_dir in current_light:
            traffic_col.insert_one(traffic_doc)
        elif traf_dir not in current_light and red_light_runner == 1 and red_light_queues[traf_dir] == []:
            traffic_doc["alert"] = "Red light runner!"
            traffic_col.insert_one(traffic_doc)
        else:
            red_light_queues[traf_dir].append(traffic_doc)
            queues_col.insert_one(traffic_doc)

        print(traffic_doc)

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