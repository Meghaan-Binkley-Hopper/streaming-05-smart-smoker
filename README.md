# streaming-05-smart-smoker
> Use RabbitMQ to send messages with time and date stamps and temperature for a smoker and two items of food in order to monitor the temperature for optimal cooking conditions. 

One process will create task messages. Multiple worker processes will share the work. 


## RabbitMQ Admin 

RabbitMQ comes with an admin panel. When you run the task emitter, reply y to open it. 


## Execute the Producer

1. Run smart_smoker_producer_MBinkley-Hopper.py (say y to monitor RabbitMQ queues)

Explore the RabbitMQ website.


## Execute a Consumer / Worker

1. Run (consumer file name)

2. Push CTRL+C to quit.


## Reference

- [RabbitMQ Tutorial - Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python.html)


## Screenshot

See a running example here:
1. Producer
![Producer](Console_Producer.PNG)
