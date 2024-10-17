from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient

class GenericKafkaConsumer:
    def __init__(self, bootstrap_server, consumer_group_id, topic_name, schema_registry_url=None):
        self.bootstrap_server = bootstrap_server
        self.consumer_group_id = consumer_group_id
        self.topic_name = topic_name
        self.schema_registry_url = schema_registry_url

        # Consumer configuration
        self.consumer_conf = {
            'bootstrap.servers': self.bootstrap_server,
            'group.id': self.consumer_group_id,
            'auto.offset.reset': 'earliest'  # Start from the earliest message
        }

        # Initialize schema registry if provided
        if self.schema_registry_url:
            self.schema_registry = SchemaRegistryClient({'url': self.schema_registry_url})
            self.consumer = AvroConsumer(self.consumer_conf, schema_registry=self.schema_registry)
        else:
            self.consumer = Consumer(self.consumer_conf)

        self.consumer.subscribe([self.topic_name])

    def retrieve_schema_info(self):
        if not self.schema_registry_url:
            print("Schema registry URL is not provided.")
            return None

        # Assuming the topic uses a specific subject name for schema
        subject = f"{self.topic_name}-value"
        schema = self.schema_registry.get_latest_version(subject)
        return schema.schema

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Poll messages with a timeout of 1 second

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {msg.partition()}")
                    elif msg.error():
                        print(f"Error occurred: {msg.error().str()}")
                    continue

                # Print the message value
                print(f"Consumed message: {msg.value().decode('utf-8')}")

        except SerializerError as e:
            print(f"Message deserialization failed for {msg}: {e}")
        finally:
            self.consumer.close()

# Example Usage:
bootstrap_server = 'localhost:9092'
consumer_group_id = 'my-consumer-group'
topic_name = 'my-topic'
schema_registry_url = 'http://localhost:8081'

kafka_consumer = GenericKafkaConsumer(bootstrap_server, consumer_group_id, topic_name, schema_registry_url)
print(kafka_consumer.retrieve_schema_info())  # Retrieve schema information
kafka_consumer.consume_messages()  # Consume messages from the topic
