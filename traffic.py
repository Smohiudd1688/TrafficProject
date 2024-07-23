import json
from bson import ObjectId
from random import randint
import threading
import time

from pymongo import MongoClient
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

from traf_utils import traffic_type, traffic_direction, traffic_speed, generate_license_plate

# MongoDB client and collections initialization
"""
Setting up the MongoDB client through Pymongo, connecting to local host port 27017.
Below the connection to the traffic_test database, each collection is established.
"""

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
"""
Setting up the Kafka producer through confluent, connecting to local host port 9092
Configuring the consumer.
"""
producer_conf = {'bootstrap.servers': 'localhost:9092'}
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka producer and consumer
"""
Using the confluent_kafka module to create the kafka producer and consumer 
(which will be MongoDB) using the configurations established above.
"""
producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)

# Kafka topics
vehicle_traffic_topic = 'vehicle_traffic'
pedestrian_traffic_topic = 'pedestrian_traffic'
red_light_topic = 'red_light_runners'
speeding_vehicles_topic = 'speeding_vehicles'
jay_walkers_topic = 'jay_walkers'

# Function to produce Kafka message
"""
A function that takes in a topic and the message to be added to it as parameters.
producer.procuse() creates the message.
producer.flush() makes sure that all messages are sent before the script ends.
"""
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


"""
A few global variables. The first is which direction is currently given the green light at the intersection.
This is updated with the stop_lights() function below, but it is global so it can be read by the separate generate_traffic() thread to determine
if they pass through the intersection or if they are added to a red_light_queue

The other two variables are dictionaries that contain empty arrays. Each array is the red light queue for each direction of traffic. If traffic is
generated while that direction is not congruent with the current_light, it gets added to the red light queue. One dictionary is for vehicle traffic,
and the other is for pedestrian traffic. 
"""
current_light = "North/South_South/North"

red_light_queues = {
    "North/South": [],
    "South/North": [],
    "East/West": [],
    "West/East": []
}

pedestrian_queues = {
    "North/South": [],
    "South/North": [],
    "East/West": [],
    "West/East": []
}

# Function to empty red light queues and insert into MongoDB
"""
A function that is called within the stop_lights() while loop. When the green light changes directions,
This will take the queues for traffic that has been stopped at the red light and adds it to traffic_topic/collection. It also taggs the traffic with "stopped_at_red_light: True" to indicate that it was stopped. 
"""
def empty_red_light_queues():
    global current_light
    global red_light_queues

    for key in red_light_queues:
        if key in current_light:
            for element in red_light_queues[key]:
                element["green_light_direction"] = current_light
                element["stopped_at_red_light"] = True
                produce_to_kafka(vehicle_traffic_topic, element)
                insert_to_mongodb(vehicle_traffic_col, element)
            red_light_queues[key] = []

# Function to control stop lights cycle
"""
Every 30 seconds, the current 0th gets added to the end of the conditions array,
and is then deleted from the beginning of the array, making it so that the element that was
at index 1 is now at index 0. In this way, every 30 seconds the light moves through the cycle.
"""
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

    """
    A while loop that generates traffic every 1 second.
    """
    while True:
        traf_type = traffic_type()
        traf_dir = traffic_direction()

        #All traffic will have a type, a direction that it enters and exits from, the current green light, and a mongo _id
        traffic_doc = {
            "traffic_type": traf_type,
            "enter_dir-exit_dir": traf_dir,
            "green_light_direction": current_light,
            "_id": ObjectId()
        }

        # This randomly generates the possibility for an instance of traffic (vehicle or ped) to run a red light.
        red_light_runner = randint(1, 4)

        #Applies the license plate and speed details to vehicles.
        if traf_type == "Vehicle":
            license = generate_license_plate()
            traffic_doc["vehicle_license_plate"] = license
            vehicle_speed = traffic_speed()
            traffic_doc["speed_data"] = vehicle_speed
            if traffic_doc["speed_data"]["speeding_amount_mph"] > 10:
                traffic_doc["speed_alert"] = f"Vehicle is speeding by {traffic_doc["speed_data"]["speeding_amount_mph"]} miles per hour."

            #handles red light runners.
            #If the red_light_runner value is a 1, and the red_light_queue for that vehicle's direction is empty,
            #they will run the red light. They are added to both the regular vehicle_traffic collection as well as the 
            #collection exclusively tracking red light runners. An alert is added to their document.
            if traf_dir not in current_light:
                if red_light_runner == 1 and red_light_queues[traf_dir] == []:
                    traffic_doc["red_alert"] = "Red light runner!"
                    produce_to_kafka(red_light_topic, traffic_doc)
                    insert_to_mongodb(red_light_runners_col, traffic_doc)
                    insert_to_mongodb(vehicle_traffic_col, traffic_doc)
                    print(f"Red light runner detected: {traffic_doc} \n \n")

                #If they are not a red light runner, but they still don't have the green light, they will be added to the queue.
                else:
                    red_light_queues[traf_dir].append(traffic_doc)
                    insert_to_mongodb(queues_col, traffic_doc)
                    print(f"Queued: {traffic_doc} \n \n")

            #If they are not stopped at the red light, their speed is evaluated and compared to the speed limit
            #to determine if they should be ticketed. Speeders are added to their own kafka-topic/mongo-collection as well as that of
            #general traffic
            elif vehicle_speed["vehicle_speed_mph"] >= 51 and traf_dir in current_light:
                produce_to_kafka(speeding_vehicles_topic, traffic_doc)
                insert_to_mongodb(speeding_vehicles_col, traffic_doc)
                produce_to_kafka(vehicle_traffic_topic, traffic_doc)
                insert_to_mongodb(vehicle_traffic_col, traffic_doc)
                print(f"Speeding vehicle detected: {traffic_doc} \n \n")
            else:
                produce_to_kafka(vehicle_traffic_topic, traffic_doc)
                insert_to_mongodb(vehicle_traffic_col, traffic_doc)

        #If the instance of traffic is pedestrian, jay-walkers are the equivilent of red-light runners.
        #Speed is not tracked for pedestrians, and pedestrians can jay-walk even if there is a queue in front of them.
        else:
            if traf_dir not in current_light and red_light_runner == 1:
                produce_to_kafka(jay_walkers_topic, traffic_doc)
                produce_to_kafka(pedestrian_traffic_topic, traffic_doc)
                insert_to_mongodb(jay_walkers_col, traffic_doc)
                insert_to_mongodb(pedestrian_traffic_col, traffic_doc)
                traffic_doc["alert"] = "Jay-walker detected!"
                print("Jay-walker detected")
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