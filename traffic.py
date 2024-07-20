import json
from bson import ObjectId
from random import randint, choices, choice
from random import randint, choices, choice
import string
from pymongo import MongoClient
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import threading
import time
import time

# MongoDB client and collections initialization
# MongoDB client and collections initialization
client = MongoClient("mongodb://localhost:27017/")
db = client["traffic_test"]
vehicle_traffic_col = db["vehicle_traffic"]
pedestrian_traffic_col = db["pedestrian_traffic"]
vehicle_traffic_col = db["vehicle_traffic"]
pedestrian_traffic_col = db["pedestrian_traffic"]
queues_col = db["queues"]
red_light_runners_col = db["red_light_runners"]
jay_walkers_col = db["jay_walkers"]
speeding_vehicles_col = db["speeding_vehicles"]

# Kafka configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka producer and consumer
producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)

# Kafka topics
vehicle_traffic_topic = 'vehicle_traffic'
pedestrian_traffic_topic = 'pedestrian_traffic'
# Kafka topics
vehicle_traffic_topic = 'vehicle_traffic'
pedestrian_traffic_topic = 'pedestrian_traffic'
red_light_topic = 'red_light_runners'
speeding_vehicles_topic = 'speeding_vehicles'
jay_walkers_topic = 'jay_walkers'

# Function to produce Kafka message
def produce_to_kafka(topic, message):
    message = convert_objectid_to_string(message)
    serialized_message = json.dumps(message).encode('utf-8')
    producer.produce(topic, serialized_message)
    producer.flush()

# Function to insert message into MongoDB
def insert_to_mongodb(collection, message):
    try:
        message = convert_objectid_to_string(message)
        collection.insert_one(message)
        print(f"Inserted into MongoDB ({collection.name}): {message} \n \n")
    except Exception as e:
        print(f"Failed to insert into MongoDB ({collection.name}): {e} \n \n")

# Function to consume messages from Kafka
def consume_from_kafka():
    try:
        consumer.subscribe([red_light_topic])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            message = json.loads(msg.value().decode('utf-8'))
            # Insert message into MongoDB
            insert_to_mongodb(red_light_runners_col, message)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Utility function to convert ObjectId to string in nested dictionaries
def convert_objectid_to_string(message):
    if isinstance(message, dict):
        for key, value in message.items():
            if isinstance(value, ObjectId):
                message[key] = str(value)
            elif isinstance(value, (dict, list)):
                message[key] = convert_objectid_to_string(value)
    elif isinstance(message, list):
        for i, item in enumerate(message):
            message[i] = convert_objectid_to_string(item)
    return message

# Generates a random value of either 'vehicle' or 'pedestrian'
# Generates a random value of either 'vehicle' or 'pedestrian'
def traffic_type():
    type_arr = ["Pedestrian", "Vehicle"]
    ped_veh = choices(type_arr, weights=[0.1, 0.9])[0]
    ped_veh = choices(type_arr, weights=[0.1, 0.9])[0]
    return ped_veh

# Generates a random value of North, South, East, or West
# Generates a random value of North, South, East, or West
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

# Generates speed for the vehicle
# Generates speed for the vehicle
def traffic_speed():
    thru_traf_speeds_arr = [randint(31,45), randint(46, 55)]
    vehicle_speed = choices(thru_traf_speeds_arr, weights = [0.85, 0.15])[0]
    return {"speed_limit_mph": 40,
    return {"speed_limit_mph": 40,
            "vehicle_speed_mph": vehicle_speed}

# Generates a random license plate
# Generates a random license plate
def generate_license_plate():
    license_state_arr = ["Arizona", "California", "Nevada", "Utah", "New Mexico"]

    license_num = "".join(choices(string.ascii_uppercase + string.digits, k=6))
    license_state = "".join(choices(license_state_arr, weights=[0.80, 0.09, 0.04, 0.03, 0.04]))
    license_state = "".join(choices(license_state_arr, weights=[0.80, 0.09, 0.04, 0.03, 0.04]))

    return {"license_plate_num": license_num, "license_plate_state": license_state}
    return {"license_plate_num": license_num, "license_plate_state": license_state}

# Sets the light cycle for the stop lights
# Sets the light cycle for the stop lights
current_light = "North/South_South/North"
red_light_queues = {
    "North/South": [],
    "South/North": [],
    "East/West": [],
    "West/East": []
    "West/East": []
}

pedestrian_queues = {
    "North/South": [],
    "South/North": [],
    "East/West": [],
    "West/East": []
}

# Function to empty red light queues and insert into MongoDB
def empty_red_light_queues():
    global current_light
    global red_light_queues

    for key in red_light_queues:
        if key in current_light:
            for element in red_light_queues[key]:
                element["green_light_direction"] = current_light
            red_light_queues[key] = []

# Function to control stop lights cycle
def stop_lights():
    global current_light
    light_conditions = ["North/South_South/North", "East/West_West/East"]
    while True:
        current_light = light_conditions[0]
        print(current_light)
        empty_red_light_queues()
        light_conditions.append(light_conditions[0])
        light_conditions.pop(0)
        time.sleep(30)

# Generates traffic documents and produces to Kafka based on traffic type
def generate_traffic():
    global red_light_queues
    global current_light
    
    while True:
        traf_type = traffic_type()
        traf_dir = traffic_direction()
        traffic_doc = {
            "traffic_type": traf_type,
            "enter_dir-exit_dir": traf_dir,
            "green_light_direction": current_light,
            "_id": ObjectId()
        }

        red_light_runner = randint(1, 4)

        if traf_type == "Vehicle":
            license = generate_license_plate()
            traffic_doc["vehicle_license_plate"] = license
            vehicle_speed = traffic_speed()
            traffic_doc["vehicle_speed_mph"] = vehicle_speed

            #handles red light runners
            if traf_dir not in current_light:
                if red_light_runner == 1 and red_light_queues[traf_dir] == []:
                    traffic_doc["alert"] = "Red light runner!"
                    produce_to_kafka(red_light_topic, traffic_doc)
                    insert_to_mongodb(red_light_runners_col, traffic_doc)  # Insert into red_light_runners_col 
                    insert_to_mongodb(vehicle_traffic_col, traffic_doc) # Insert into vehicle traffic
                    print(f"Red light runner detected: {traffic_doc} \n \n")
                else:
                    red_light_queues[traf_dir].append(traffic_doc)
                    insert_to_mongodb(queues_col, traffic_doc)
                    print(f"Queued: {traffic_doc} \n \n")
            #As long as they are not stopped at the red light, they are entered into general vehicle traffic.
            elif vehicle_speed["vehicle_speed_mph"] >= 51 and traf_dir in current_light:
                speeding_amount = vehicle_speed["vehicle_speed_mph"] - 40  # Calculate speeding amount
                traffic_doc["speeding_amount_mph"] = speeding_amount
                traffic_doc["alert"] = f"Vehicle is speeding by {speeding_amount} mph!"
                produce_to_kafka(speeding_vehicles_topic, traffic_doc)
                insert_to_mongodb(speeding_vehicles_col, traffic_doc)  # Insert into speeding_vehicles_col
                produce_to_kafka(vehicle_traffic_topic, traffic_doc)
                insert_to_mongodb(vehicle_traffic_col, traffic_doc)
                print(f"Speeding vehicle detected: {traffic_doc} \n \n")
            else:
                produce_to_kafka(vehicle_traffic_topic, traffic_doc)
                insert_to_mongodb(vehicle_traffic_col, traffic_doc)  # Insert into vehicle_traffic_col

        else:  # Pedestrian
            if traf_dir not in current_light and red_light_runner == 1:
                produce_to_kafka(jay_walkers_topic, traffic_doc)
                produce_to_kafka(pedestrian_traffic_topic, traffic_doc)
                insert_to_mongodb(jay_walkers_col, traffic_doc)
                insert_to_mongodb(pedestrian_traffic_col, traffic_doc)
            elif traf_dir not in current_light and red_light_runner > 1:
                pedestrian_queues[traf_dir].append(traf_dir)
            else:
                produce_to_kafka(pedestrian_traffic_topic, traffic_doc)
                insert_to_mongodb(pedestrian_traffic_col, traffic_doc)  # Insert into pedestrian_traffic_col

        time.sleep(1)

# Start Kafka consumer thread
consumer_thread = threading.Thread(target=consume_from_kafka)
consumer_thread.start()

# Start traffic generation and stop lights control threads
traffic_thread = threading.Thread(target=generate_traffic)
lights_thread = threading.Thread(target=stop_lights)
traffic_thread.start()
lights_thread.start()

# Join threads (optional if you want to wait for threads to finish)
# traffic_thread.join()
# lights_thread.join()
# consumer_thread.join()
# Start Kafka consumer thread
consumer_thread = threading.Thread(target=consume_from_kafka)
consumer_thread.start()

# Start traffic generation and stop lights control threads
traffic_thread = threading.Thread(target=generate_traffic)
lights_thread = threading.Thread(target=stop_lights)
traffic_thread.start()
lights_thread.start()

# Join threads (optional if you want to wait for threads to finish)
# traffic_thread.join()
# lights_thread.join()
# consumer_thread.join()