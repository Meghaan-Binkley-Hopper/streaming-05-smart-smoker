"""
    This program listens for work messages contiously. 
    Start multiple terminals to add more workers. 
    Press CTRL+C to end program. 
    Author: Meghaan Binkley-Hopper
    Date: February 3, 2023
"""

import pika
import sys
from collections import deque
import time

"""Define Variables"""
#Smoker alerts when temperature drops by 15 or more degrees
temp_alert = -15
hn = "localhost"
# limited to the the 5 most recent readings (2.5 minutes * 1 reading / 0.5 minutes)
smoker_deque = deque(maxlen=5) 


# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    smoker_message = body.decode()
    # decode the binary message body to a string
    print(f" [x] Received {smoker_message}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    # sleep for 1 second
    time.sleep(1)

    #Add current message to deque
    smoker_deque.append(smoker_message)

    """Set first temperature from deque for calculation below"""
    #declare position
    first_temp = smoker_deque[0] 
    #split timestamp and temp
    sp_first_temp = first_temp.split(",") 
    #convert temp into float
    form_first_temp = float(sp_first_temp[1][:-1]) 

    """Set current temperature from deque for calculation below"""
    #declare position
    current_temp = smoker_message
    #split timestamp and temp
    sp_current_temp = current_temp.split(",")
    #convert temp into float
    form_current_temp = float(sp_current_temp[1][:-1])

    #If statement to make sure the deque is full before making calculation
    if len(smoker_deque) == 5:
        #subtract first temp from current temp and round to see if temp has fallen 15 degrees
        temp_assessment = round(form_current_temp - form_first_temp, 1)
        #If statement to determine if alert message or temp as expected message should be sent
        if temp_assessment <= temp_alert:
            print("Alert! Smoker Temperature has dropped by more than 15 degrees.")
        else:
            print("Smoker temperature is as expected")




# define a main function to run the program
def main(hn: str = "localhost", qn: str = "queue_name"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        ch = conn.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        ch.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        ch.basic_consume( queue=qn, auto_ack=False, on_message_callback=callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        ch.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        conn.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "01-smoker")