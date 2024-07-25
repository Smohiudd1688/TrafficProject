import mysql.connector
from pymongo import MongoClient
import datetime
import time


# MongoDB Connection
def get_mongo_connection():
    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["traffic_data"]
    return mongo_db

# MySQL Connection
def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user="user",  # Replace with MySQL username
        password="pass",  # Replace with your MySQL password
        database="traffic_data"  # The database created in MySQL Workbench
    )

# MySQL Table Creation
def create_mysql_tables(cursor):

    # Table for general traffic information without using license_plate_num as a key
    cursor.execute("""
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
    """)

    # Table for red light runners
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS redlight_runners (
        id INT AUTO_INCREMENT PRIMARY KEY,
        license_plate_num VARCHAR(50)
    )
    """)

    # Table for speeding vehicles
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS speeding_vehicles (
        id INT AUTO_INCREMENT PRIMARY KEY,
        license_plate_num VARCHAR(50)
    )
    """)

    # New table to hold traffic statistics every 5 minutes
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS traffic_stats (
        id INT AUTO_INCREMENT PRIMARY KEY,
        recorded_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        north_south_traffic INT,
        east_west_traffic INT,
        redlight_runner_count INT,
        speeding_vehicle_count INT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS traffic_stats (
        id INT AUTO_INCREMENT PRIMARY KEY,
        recorded_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        north_south_traffic INT,
        east_west_traffic INT,
        redlight_runner_count INT,
        speeding_vehicle_count INT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pedestrian_stats (
        id INT AUTO_INCREMENT PRIMARY KEY,
        recorded_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        north_south_ped_count INT,
        east_west_ped_count INT,
        jay_walkers_count INT
    )
    """)


# Fetch Data from MongoDB and Insert into MySQL
def transfer_data(mongo_collection, mysql_cursor, mysql_conn):
    mongo_data = mongo_collection.find({"time_stamp": {"$gt": now}})
    insert_query = """
    INSERT INTO traffic_info (
        traffic_type, enter_dir_exit_dir, green_light_direction,
        license_plate_num, license_plate_state, speed_limit_mph,
        vehicle_speed_mph, alert
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    for doc in mongo_data:
        data_tuple = (
            doc.get("traffic_type"),
            doc.get("enter_dir-exit_dir"),
            doc.get("green_light_direction"),
            doc.get("vehicle_license_plate", {}).get("license_plate_num"),
            doc.get("vehicle_license_plate", {}).get("license_plate_state"),
            doc.get("speed_data", {}).get("speed_limit_mph"),
            doc.get("speed_data", {}).get("vehicle_speed_mph"),
            doc.get("alert")
        )
        mysql_cursor.execute(insert_query, data_tuple)
    mysql_conn.commit()  # Correct way to commit using the connection object

def transfer_redlight_data(mongo_db, mysql_cursor, mysql_conn):
    # Assuming red_light_runners collection has documents with license_plate_num and possibly other fields
    redlight_runners = mongo_db["red_light_runners"]
    
    # Fetch data from MongoDB; you could use aggregation here if you need to process data
    redlight_data = redlight_runners.find({"time_stamp": {"$gt": now}})
    
    # SQL query to insert/update data in MySQL
    insert_query = """
    INSERT INTO redlight_runners (license_plate_num)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE license_plate_num = VALUES(license_plate_num);
    """
    
    for doc in redlight_data:
        license_plate_num = doc.get("vehicle_license_plate", {}).get("license_plate_num")
        if license_plate_num:
            mysql_cursor.execute(insert_query, (license_plate_num,))
    
    mysql_conn.commit()

def transfer_speeding_vehicles_data(mongo_db, mysql_cursor, mysql_conn):
    # Assuming red_light_runners collection has documents with license_plate_num and possibly other fields
    speeding_vehicles = mongo_db["speeding_vehicles"]
    
    # Fetch data from MongoDB; you could use aggregation here if you need to process data
    speeding_vehicles_data = speeding_vehicles.find({"time_stamp": {"$gt": now}})
    
    # SQL query to insert/update data in MySQL
    insert_query = """
    INSERT INTO speeding_vehicles (license_plate_num)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE license_plate_num = VALUES(license_plate_num);
    """
    
    for doc in speeding_vehicles_data:
        license_plate_num = doc.get("vehicle_license_plate", {}).get("license_plate_num")
        if license_plate_num:
            mysql_cursor.execute(insert_query, (license_plate_num,))
    
    mysql_conn.commit()

# Aggregate and Transfer Counts
def aggregate_and_transfer_pedestrian_counts(mongo_db, mysql_cursor, mysql_conn):
    # Get traffic count by direction from MongoDB
    north_south_traffic = mongo_db["pedestrian_traffic"].count_documents({"enter_dir-exit_dir": {"$regex": "North|South"}, "time_stamp": {"$gt": now}})
    east_west_traffic = mongo_db["pedestrian_traffic"].count_documents({"enter_dir-exit_dir": {"$regex": "East|West"}, "time_stamp": {"$gt": now}})
    jay_walkers_count = mongo_db["jay_walkers"].count_documents({"time_stamp": {"$gt": now}})

    # Insert the counts into the MySQL traffic_stats table
    insert_stats_query = """
    INSERT INTO pedestrian_stats (north_south_ped_count, east_west_ped_count, jay_walkers_count) 
    VALUES (%s, %s, %s)
    """
    mysql_cursor.execute(insert_stats_query, (north_south_traffic, east_west_traffic, jay_walkers_count))  # Placeholder for real-time counts
    mysql_conn.commit()



def aggregate_and_transfer_counts(mongo_db, mysql_cursor, mysql_conn):
    # Get traffic count by direction from MongoDB
    north_south_traffic = mongo_db["vehicle_traffic"].count_documents({"enter_dir-exit_dir": {"$regex": "North|South"}, "time_stamp": {"$gt": now}})
    east_west_traffic = mongo_db["vehicle_traffic"].count_documents({"enter_dir-exit_dir": {"$regex": "East|West"}, "time_stamp": {"$gt": now}})
    redlight_runners_count = mongo_db["red_light_runners"].count_documents({"time_stamp": {"$gt": now}})
    speeding_vehicles_count = mongo_db["speeding_vehicles"].count_documents({"time_stamp": {"$gt": now}})

    # Insert the counts into the MySQL traffic_stats table
    insert_stats_query = """
    INSERT INTO traffic_stats (north_south_traffic, east_west_traffic, redlight_runner_count, speeding_vehicle_count) 
    VALUES (%s, %s, %s, %s)
    """
    mysql_cursor.execute(insert_stats_query, (north_south_traffic, east_west_traffic, redlight_runners_count, speeding_vehicles_count))  # Placeholder for real-time counts
    mysql_conn.commit()


def main():
    global now
    now = "2024-07-24 16:24:26"
    global specific_datetime

    while True:
        try:
            mysql_conn = get_mysql_connection()
            mysql_cursor = mysql_conn.cursor()
            mongo_db = get_mongo_connection()
            
            create_mysql_tables(mysql_cursor)
            transfer_data(mongo_db["vehicle_traffic"], mysql_cursor, mysql_conn)
            transfer_redlight_data(mongo_db, mysql_cursor, mysql_conn)
            transfer_speeding_vehicles_data(mongo_db, mysql_cursor, mysql_conn)
            aggregate_and_transfer_counts(mongo_db, mysql_cursor, mysql_conn)
            aggregate_and_transfer_pedestrian_counts(mongo_db, mysql_cursor, mysql_conn)
            now = datetime.datetime.now()
            now = str(now).split('.')[0]

            print("Data transfer from MongoDB to MySQL completed successfully!")
            time.sleep(180)  # Pauses for 5 minutes before the next iteration

        finally:
            mysql_cursor.close()
            mysql_conn.close()
            mongo_db.client.close()

if __name__ == "__main__":
    main()

