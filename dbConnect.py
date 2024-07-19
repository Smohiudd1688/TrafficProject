import mysql.connector
from pymongo import MongoClient

# MongoDB Connection
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["traffic_test"]
mongo_collection = mongo_db["traffic"]

# MySQL Connection
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="user",  
    password="pass",  
    database="traffic_data"  # The database created in MySQL Workbench
)
mysql_cursor = mysql_conn.cursor()

# MySQL Table Creation
create_table_query = """
CREATE TABLE IF NOT EXISTS traffic_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    traffic_type VARCHAR(50),
    enter_dir_exit_dir VARCHAR(50),
    green_light_direction VARCHAR(50),
    license_plate_num VARCHAR(50),
    license_plate_state VARCHAR(50),
    speed_limit_mph INT,
    vehicle_speed_mph INT,
    alert VARCHAR(50)
)
"""

create_redlight_runner_table_query = """
CREATE TABLE IF NOT EXISTS redlight_runners (
    id INT AUTO_INCREMENT PRIMARY KEY,
    runner_count INT
)
"""

create_speeding_vehicles_table_query = """
CREATE TABLE IF NOT EXISTS speeding_vehicles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    speeding_vehicle_count INT
)
"""

mysql_cursor.execute(create_table_query)
mysql_cursor.execute(create_redlight_runner_table_query)
mysql_cursor.execute(create_speeding_vehicles_table_query)

# Fetch Data from MongoDB
mongo_data = mongo_collection.find()

# Insert Data into MySQL
insert_query = """
INSERT INTO traffic_info (
    traffic_type,
    enter_dir_exit_dir,
    green_light_direction,
    license_plate_num,
    license_plate_state,
    speed_limit_mph,
    vehicle_speed_mph,
    alert
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

for doc in mongo_data:
    traffic_type = doc.get("traffic_type", None)
    enter_dir_exit_dir = doc.get("enter_dir-exit_dir", None)
    green_light_direction = doc.get("green_light_direction", None)
    license_plate_num = doc.get("vehicle_license_plate", {}).get("license_plate_num", None)
    license_plate_state = doc.get("vehicle_license_plate", {}).get("license_plate_state", None)
    speed_limit_mph = doc.get("speed_data", {}).get("speed_limit_mph", None)
    vehicle_speed_mph = doc.get("speed_data", {}).get("vehicle_speed_mph", None)
    alert = doc.get("alert", None)
    
    data_tuple = (
        traffic_type,
        enter_dir_exit_dir,
        green_light_direction,
        license_plate_num,
        license_plate_state,
        speed_limit_mph,
        vehicle_speed_mph,
        alert
    )
    
    mysql_cursor.execute(insert_query, data_tuple)

mysql_conn.commit()

# Aggregate to find the count of red light runners
redlight_runners_pipeline = [
    {"$match": {"alert": "Red light runner!"}},
    {"$count": "red_light_runner_count"}
]

redlight_runners_count = list(mongo_collection.aggregate(redlight_runners_pipeline))

if redlight_runners_count:
    runner_count = redlight_runners_count[0]["red_light_runner_count"]
else:
    runner_count = 0

# Insert red light runner count into MySQL
insert_runner_count_query = """
INSERT INTO redlight_runners (runner_count) VALUES (%s)
"""
mysql_cursor.execute(insert_runner_count_query, (runner_count,))
mysql_conn.commit()

# Aggregate to find the count of vehicles going 10 mph above the speed limit
speeding_vehicles_pipeline = [
    {"$match": {"$expr": {"$gt": [{"$subtract": ["$speed_data.vehicle_speed_mph", "$speed_data.speed_limit_mph"]}, 10]}}},
    {"$count": "speeding_vehicle_count"}
]

speeding_vehicles_count = list(mongo_collection.aggregate(speeding_vehicles_pipeline))

if speeding_vehicles_count:
    speeding_count = speeding_vehicles_count[0]["speeding_vehicle_count"]
else:
    speeding_count = 0

# Insert speeding vehicles count into MySQL
insert_speeding_count_query = """
INSERT INTO speeding_vehicles (speeding_vehicle_count) VALUES (%s)
"""
mysql_cursor.execute(insert_speeding_count_query, (speeding_count,))
mysql_conn.commit()

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

print("Data transfer from MongoDB to MySQL completed successfully!")



