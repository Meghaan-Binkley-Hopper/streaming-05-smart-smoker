"""
    This program reads a CSV file with timestamps and temperatures and sends as a messages to 3 queues on the RabbitMQ server.  The purpose of the 
    program is to measure the temperature of a smart smoker and 2 types of food to insure optimal cooking conditions.
    
    Author: Meghaan Binkley-Hopper
    Date: February 14, 2023

"""

import pika #RabbitMQ
import sys #Exceptions
import webbrowser #open a web browser 
import csv #Enables Python to read the csv file
import time #Allows for time delay

host = "localhost"

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
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
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def delete_queue (host: str, last_queue: str):
    # create a blockin connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    #delete the last queue
    ch.queue_delete(queue = last_queue)



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # Delete last queue for smoker
    delete_queue (host, "01-smoker")
    # Delete last queue for food A
    delete_queue (host,"02-food-A")
    # Delete last queue for food B
    delete_queue (host,"03-food-B")

    #creating a variable for file name
    message_file_name = "smoker-temps.csv" 

    # read from a file 
    input_file = open(message_file_name, "r")

    # create a csv reader for our comma delimited data
    reader = csv.reader(input_file, delimiter=",")

    #iterate through csv file to send data
    for row in reader:
        # pull timestamp
        fstring_timestamp = f"{row [0]}"
        # pull smoker temp
        fstring_channel1 = f"{row [1]}"
        # pull food A temp
        fstring_channel2 = f"{row [2]}"
        # pull food B temp
        fstring_channel3 = f"{row [3]}"

        # convert non-empty temp strings into floats
        try:
            fstring_channel1 = float(fstring_channel1)
            fstring_channel2 = float(fstring_channel2)
            fstring_channel3 = float(fstring_channel3)
        # skip the float conversion on empty strings
        except ValueError:
            pass


        #create tuple with timestamp and smoker temp
        smoker_01 = f"{(fstring_timestamp, fstring_channel1)}"
        #create tuble with timestamp and food A temp
        food_A_02 = f"{(fstring_timestamp, fstring_channel2)}"
        #create tuple with timestamp and food B temp
        food_B_03 = f"{(fstring_timestamp, fstring_channel3)}"
        
    
        # prepare a binary (1s and 0s) message to stream
        message_smoker = smoker_01.encode()
        message_foodA = food_A_02.encode()
        message_foodB = food_B_03.encode()
        
        #send messages to the producer
        send_message(host,"01-smoker",message_smoker)
        send_message(host,"02-food-A",message_foodA)
        send_message(host,"03-food-B",message_foodB)

        #time delay for 30 seconds between message sends
        time.sleep(30)
    #close csv file
    input_file.close()

