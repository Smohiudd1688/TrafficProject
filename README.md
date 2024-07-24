# Traffic Monitoring Capstone

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Table of Contents

[Description](#description)

[Installation](#installation)

[Usage](#usage)

[Grafana](#Grafana)

[Contribute](#contribute)

[Test](#test)

[Credits](#credits)

[License](#license)

[Questions](#questions)

## Description

This application similuates simple traffic at an intersection, and then stores that information in a MongoDB database, going through Apache Kafka as a broker.

## Installation

The application has many dependencies. First, it requires python. It was built using python verison 3.12.3. Second, it requires Docker, as the MongoDB database and Apache Kafka producers and consumers are all encompassed in Docker containers. Docker Desktop is a valuable tool for managing these containers. It is also recommended to use MongoDB Compass as a user interface for exploring your documents and collections. You will also need to `pip install` conluent_kafka and pymongo.

## Usage

First, run the docker-compose.yml file from your terminal while in the appropriate directory. It's advised that you double check Docker Desktop to ensure that all of your containers are running. If all of your containers are running, you can then run the python script in the traffic.py file. This will start populating kafka and mongodb with generated vehicle and pedestrian traffic at this intersection.

![alt text](./screencap1.PNG)
![alt text](./screencap2.PNG)

## Grafana

Access Grafana by navigating to 'http://localhost:3001'  
Log in using credentials('admin'/'password')  
Configure the connection to MySQL database:  
 Host: 'mysql:3306'  
 Database: 'traffic_data'  
 User: 'user'  
 Password: 'pass'

Prometheus:  
 Host: 'http://prometheus:9090'

## Credits

Contributors include Dara Prak, Sana Mohiuddin, and Adam Johnson.

## License

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This project is licensed under the terms of the Apache 2.0 license.

## Questions?

Contact us:

Adam Johnson:

    Email: adamgjohnson92@gmail.com

    GitHub: AdamJohnson92

Dara Prak:

    Email: dprak107@gmail.com

    GitHub: daraprak

Sana Mohiuddin:

    Email: smohiuddin1688@gmail.com

    GitHub: Smohiudd1688
