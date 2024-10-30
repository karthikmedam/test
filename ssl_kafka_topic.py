import boto3
from botocore.exceptions import ClientError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import ssl
import tempfile
import os

def get_ssl_secrets():
    """Retrieve SSL certificates from AWS Secrets Manager"""
    secret_name = "test"
    region_name = "us-east-1"
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(get_secret_value_response['SecretBinary'])
        return secret
    except ClientError as e:
        raise e

def write_temp_cert(cert_content, suffix):
    """Write certificate content to temporary file"""
    temp_cert = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temp_cert.write(cert_content.encode())
    temp_cert.close()
    return temp_cert.name

def create_kafka_topic():
    # Get SSL certificates from AWS Secrets Manager
    ssl_secrets = get_ssl_secrets()
    
    # Write certificates to temporary files
    ca_path = write_temp_cert(ssl_secrets['caPath'], '.crt')
    cert_path = write_temp_cert(ssl_secrets['certPath'], '.pem')
    key_path = write_temp_cert(ssl_secrets['keyPath'], '.key')
    
    try:
        # Kafka configuration
        kafka_conf = {
            'bootstrap.servers': 'test.amazonaws.com:9092',
            'security.protocol': 'SSL',
            'ssl.ca.location': ca_path,
            'ssl.certificate.location': cert_path,
            'ssl.key.location': key_path,
            'ssl.key.password': ssl_secrets.get('keyPassword', None)
        }
        
        # Create AdminClient instance
        admin_client = AdminClient(kafka_conf)
        
        # Topic configuration
        topic_name = 'Issue_raj_04'
        num_partitions = 2
        replication_factor = 2
        
        # Create new topic
        topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        # Create the topic
        fs = admin_client.create_topics([topic])
        
        # Wait for topic creation
        for topic, f in fs.items():
            try:
                f.result()  # Returns None on success
                print(f"Topic '{topic}' created successfully.")
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")
                
    finally:
        # Clean up temporary certificate files
        for file_path in [ca_path, cert_path, key_path]:
            try:
                os.unlink(file_path)
            except:
                pass

if __name__ == "__main__":
    create_kafka_topic()
