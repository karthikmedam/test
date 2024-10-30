import boto3
from botocore.exceptions import ClientError
from base64 import b64decode
import json

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
        
        # Handle SecretBinary response
        if 'SecretBinary' in get_secret_value_response:
            secret_binary = get_secret_value_response['SecretBinary']
            # If it's already a string (which seems to be the case from your screenshot)
            if isinstance(secret_binary, str):
                return json.loads(secret_binary)
            # If it's actual binary data
            else:
                decoded_binary_secret = b64decode(secret_binary)
                return json.loads(decoded_binary_secret)
        
        # Handle SecretString response
        elif 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
            
    except ClientError as e:
        raise e
    except json.JSONDecodeError as e:
        print(f"Error decoding secret: {str(e)}")
        print(f"Raw secret content: {get_secret_value_response.get('SecretBinary', '')[:100]}...")
        raise e

def create_kafka_topic():
    try:
        # Get SSL certificates
        ssl_certs = get_ssl_secrets()
        print("Successfully retrieved SSL certificates")
        
        # Configure Kafka with SSL
        kafka_conf = {
            'bootstrap.servers': 'test.amazonaws.com:9092',
            'security.protocol': 'SSL',
            'ssl.ca.location': write_cert_to_file(ssl_certs['caPath'], 'ca.crt'),
            'ssl.certificate.location': write_cert_to_file(ssl_certs['certPath'], 'cert.pem'),
            'ssl.key.location': write_cert_to_file(ssl_certs['keyPath'], 'key.pem')
        }
        
        # Create AdminClient instance
        admin_client = AdminClient(kafka_conf)
        
        # Create topic
        topic = NewTopic(
            topic='Issue_raj_04',
            num_partitions=2,
            replication_factor=2
        )
        
        # Create the topic
        fs = admin_client.create_topics([topic])
        
        # Check results
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic '{topic}' created successfully")
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")
                
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e

def write_cert_to_file(cert_content, filename):
    """Write certificate content to a temporary file"""
    import tempfile
    import os
    
    temp_dir = tempfile.gettempdir()
    cert_path = os.path.join(temp_dir, filename)
    
    with open(cert_path, 'w') as f:
        f.write(cert_content)
    
    return cert_path

if __name__ == "__main__":
    try:
        # Test secret retrieval first
        secrets = get_ssl_secrets()
        print("Successfully retrieved secrets")
        print("Available keys:", list(secrets.keys()))
        
        # If successful, create the topic
        create_kafka_topic()
    except Exception as e:
        print(f"Error in main: {str(e)}")
