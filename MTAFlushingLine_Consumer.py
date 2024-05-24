"""
Created by: A. C. Coffin
Date: 24 May 2024

Consumer 2 was created to determine if a station from the data set was from the Number 7 Line, Flushing Line. 

This emiter was designed to filter through all of the data entires found in Data_MTA_Subway_Hourly_Rideship.csv.
The process can be interrupted using Ctrl + C if an escape is needed.
"""
import pika
import sys
import csv

# Configuer Logging
from utils.util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)

output_file_name = "MTA_FlushingLine_Output.csv"

# Outlining which stations we are seeking that are on the Number 7 Line
def is_7_train_line(station_complex_id):
    return station_complex_id in [447, 448, 449, 450, 452, 453, 455, 457, 456, 458, 459, 460, 461, 463, 464, 471, 606]


def callback(ch, method, properties, body):
    station_complex_id = (body.decode())
    if is_7_train_line(station_complex_id):
        logger.info(f"Station {station_complex_id} is on the 7 Train or Flushing Line.")
    else:
        logger.info(f"Station {station_complex_id} is not on the 7 Train or Flushing Line")  
    with open("MTACaps_Output.csv", 'a') as file:
        writer = csv.writer(file, delimiter = ',')
        writer.writerow(station_complex_id)
    logger.info(" [x] Created CSV.")


def main(hn: str, qn: str = "Flushings_queue"):
    """Continuously listen for the task messages on a named queue."""
    # Creating a conditional in case something goes wrong.
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        # except, if there's an error, do this
    except Exception as e:
       # Left one error message for print to ensure error handing was done propelry.
        print(f"ERROR: connection to RabbitMQ server failed. The error says: {e}")
        logger.error(f"ERROR: connection to RabbitMQ server failed. The error is {e}.")
        logger.error(f"Verify the server is running on host={hn}.")
        sys.exit(1)
    try:
        # use the connection to create a communication channel
        channel = conn.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, on_message_callback=callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        logger.error(f"Error: Something whent wrong. Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        logger.info("KeyboardInterrupt. Stopping the Program")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        logger.info("\nclosing connection. Goodby\n")
        conn.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    host = "localhost"
    main("localhost", "Flushings_queue")    
     