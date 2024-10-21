from confluent_kafka import admin
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json

# Kafka Admin configuration
admin_conf = {
    'bootstrap.servers': 'localhost:9092',
    'security.protocol': 'SSL',
    'ssl.certificate.location': '/path/to/client.crt',
    'ssl.key.location': '/path/to/client.key',
    'ssl.ca.location': '/path/to/ca.crt'
}

# Schema Registry configuration
schema_registry_conf = {
    'url': 'https://localhost:8081',
    'basic.auth.user.info': 'username:password',
    'ssl.certificate.location': '/path/to/client.crt',
    'ssl.key.location': '/path/to/client.key',
    'ssl.ca.location': '/path/to/ca.crt'
}

# Sample message schema
message_schema = {
    "type": "record",
    "name": "Message",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"}
    ]
}

# Create Kafka Admin client
admin_client = admin.AdminClient(admin_conf)

# Create Schema Registry client
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Register the schema in the Schema Registry
avro_serializer = AvroSerializer(schema_registry_client)
schema_id = avro_serializer.register_schema('message-value', json.dumps(message_schema))

# Create the Kafka topic with the registered schema
topic_name = 'topic1'
num_partitions = 3
replication_factor = 1

topic_spec = admin.NewTopic(topic_name, num_partitions, replication_factor)
topic_spec.config['confluent.value.schema.id'] = str(schema_id)

admin_client.create_topics([topic_spec])
print(f"Created topic '{topic_name}' with schema ID {schema_id}")
























#consume the topic


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
