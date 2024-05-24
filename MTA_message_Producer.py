"""
Created by: A. C. Coffin
Date: 24 May 2024

Producer designed to send messages to a queue on the RabbitMQ server. 
Make sure the RabbitMQ server is on an connected before running.
Run this After Running Consumers. We will need multiple consumers to exicute our task.

This emiter was designed to filter through all of the data entires found in Data_MTA_Subway_Hourly_Rideship.csv.
The process can be interrupted using Ctrl + C if an escape is needed.
"""

import pika
import sys
import webbrowser
import csv
import time

# Configuer Logging
from utils.util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)


# Define Program functions
#--------------------------------------------------------------------------

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print("Website Opened")
        logger.info("Website Opened")

# Function to send message
def send_message(host: str, first_queue_name: str, second_queue_name: str, input_file:str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queues each message needs it's own queue.
        # create two different queues, first_queue_name and second_queue_name
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=first_queue_name, durable=True)
        ch.queue_declare(queue=second_queue_name, durable=True)
        
        # read each row from Data_MTA_Subway_Hourly_Rideship.csv
        with open(input_file_name, "r") as input_file:
            reader=csv.reader(input_file, delimiter=",")
            for row in reader:
            # Seperate row into variables by column
                transit_date, transit_timestamp, station_complex_id, station_complex, borough, ridership = row
                header = next(reader)
                first_message = str(station_complex)
                second_message = (station_complex_id)

                # Setting up First Message exchange
                ch.basic_publish(exchange="", 
                                 routing_key=first_queue_name, 
                                 body=first_message)
                # print a message to the console for the user
                logger.info(f" [x] Sent {first_message} to {first_queue_name}")

                #Setting up Second Message exchange
                ch.basic_publish(exchange="", 
                                 routing_key=second_queue_name, 
                                 body=second_message, 
                                 properties=pika.BasicProperties(content_type='text/plain', delivery_mode=2))
                # print a message to the console for the user:
                logger.info(f" [x] Sent {second_message} to {second_queue_name}")
                # wait 3 seconds before sending the next message to the queue
                time.sleep(3)

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
   
    #------------------------------------------------------------------------#
    # Modifications to send message

    host = "localhost"
    first_queue_name = "MTACaps_queue"
    second_queue_name = "Flushings_queue"
    input_file_name = "Data_MTA_Subway_Hourly_Ridership.csv"
    # Input File Name:
    # send the message to the queue
    #send_message("localhost","task_queue2",message)

    # Modified for function written
    send_message(host, first_queue_name, second_queue_name, input_file_name )