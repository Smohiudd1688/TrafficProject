from random import choices, choice, randint
import string

"""
This is a utils file that contains functions that generates various components of each instance of traffic
"""

# Generates a random value of either 'vehicle' or 'pedestrian'
"""
This function will decide if each instance of traffic is a vehicle or a pedestrian.
It uses the the choices() method from the random module to pseudo-randomly pick a value from 
the array with two elements, either Pedestrian, or Vehicle. the choices() method allows us to add a weight 
to the generation, so we have opted to have about 90% of traffic be vehicles and 10% being pedestrians.
Because the choices() method returns an array, we take the 0th index from that array and assign it to the ped_veh variable.
"""
def traffic_type():
    type_arr = ["Pedestrian", "Vehicle"]
    ped_veh = choices(type_arr, weights=[0.1, 0.9])[0]
    return ped_veh

# Generates a random value of North, South, East, or West
"""
In this traffic simulator, there are no turns allowed at this intersection. As such, all traffic is through-traffic.
Using the choice() method from the random module, a source direction is generated. This indicates the side of the intersection
that the instance of traffic enters from. In it's current iteration (only allowing through-traffic) the destination direction 
is just set to the opposite of the source direction. However, if turns were allowed, then the destination direction
would also include a random choice() as long as it doesn't equal the source direction.
"""
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
"""
This function uses the randint() method from the random module to pseudo-randomly generate
two values: a law-abiding speed, and a law-breaking speed. The speed limit at this intersection is 40 mph.
So law abiding speeds can be up to 50 mph before a speeding ticket would be issued. Once those values are set,
we use the choice() method again to randomly choose one of those two values to apply to the vehicle traffic. The choices()
method gives more weight to law-abiding speeds, so we only expect about 15% of traffic to be speeding.
If a vehicle is speeding, their mph over the speed limit is logged 
"""
def traffic_speed():
    speed_limit = 40
    thru_traf_speeds_arr = [randint(37,50), randint(51, 60)]
    vehicle_speed = choices(thru_traf_speeds_arr, weights = [0.85, 0.15])[0]
    speed_subdoc = {"speed_limit_mph": speed_limit,
                    "vehicle_speed_mph": vehicle_speed,
                    "speeding_amount_mph": 0}
    
    if vehicle_speed > speed_limit + 10:
        speed_subdoc["speeding_amount_mph"] = vehicle_speed - speed_limit
    
    return speed_subdoc

# Generates a random license plate
"""
Randomly generates a 6-character string of alphanumeric characters to be the license plate number, returned as an array, which
is then joined into a string.
Then the choices() method chooses a State for the license plate to be issued from, with most traffic being from Arizona. 
"""
def generate_license_plate():
    license_state_arr = ["Arizona", "California", "Nevada", "Utah", "New Mexico"]

    license_num = "".join(choices(string.ascii_uppercase + string.digits, k=6))
    license_state = "".join(choices(license_state_arr, weights=[0.80, 0.09, 0.04, 0.03, 0.04]))

    return {"license_plate_num": license_num, "license_plate_state": license_state}
