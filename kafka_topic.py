from confluent_kafka import Consumer, KafkaError
import json

# Kafka Consumer configuration settings
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker
    'group.id': 'my-consumer-group',        # Consumer group
    'auto.offset.reset': 'earliest'         # Start reading at the earliest offset
}

# Sample schema
class MessageSchema:
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email
        }

# Create a Consumer instance
consumer = Consumer(consumer_conf)

# Subscribe to the topic
topic = 'topic1'
consumer.subscribe([topic])
print(f'Subscribed to topic: {topic}')

try:
    while True:
        # Poll for new messages from Kafka
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue  # No message available, continue polling
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, not an actual error
                print(f"Reached end of partition for {msg.topic()}")
            else:
                print(f"Error: {msg.error()}")
        else:
            # Process the message
            message = json.loads(msg.value().decode('utf-8'))
            message_schema = MessageSchema(**message)
            print(f"Received message: {message_schema.to_dict()} from {msg.topic()}")
except KeyboardInterrupt:
    pass
finally:
    # Close the consumer when done
    consumer.close()
