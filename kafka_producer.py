import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import random

TOPIC_NAME = "test_flink"
KAFKA_BROKER = '127.0.0.1:9092'

def serializer(value):
    return json.dumps(value).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=serializer
)

class Device:
    def __init__(self, device_id, mu_temp, sigma_temp, mu_hum, sigma_hum):
        assert isinstance(device_id, str)
        assert isinstance(mu_temp, float)
        assert isinstance(sigma_temp, float)
        assert isinstance(mu_hum, float)
        assert isinstance(sigma_hum, float)
        self.device_id = device_id
        self.mu_temp = mu_temp
        self.sigma_temp = sigma_temp
        self.mu_hum = mu_hum
        self.sigma_hum = sigma_hum

    def get(self):
        value_temp = random.gauss(mu=self.mu_temp, sigma=self.sigma_temp)
        value_hum = random.gauss(mu=self.mu_hum, sigma=self.sigma_hum)
        return {
            'id': self.device_id,
            'temperature': value_temp,
            'humidity': value_hum,
            'timestamp': time.time(),
        }

devices = [
    Device('DEV-001', 0.0, 1.0, 0.0, 1.0),
    Device('DEV-002', 5.0, 1.0, 5.0, 1.0),
]

#send messages
for i in range(10000000):
    # message = {'number': i, 'timestamp': time.time()}
    for device in devices:
        msg = device.get()
        producer.send(TOPIC_NAME, value=msg)
        print(f"Sent message: {msg}")
    producer.flush()
    time.sleep(1) # Sleep for a second

#ensure all messages are sent
producer.flush()
print("Finished sending messages.")