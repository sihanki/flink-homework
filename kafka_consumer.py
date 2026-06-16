from kafka import KafkaConsumer
import json

TOPIC_NAME = "flink_result"
KAFKA_BROKER = '127.0.0.1:9092'

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest', # Start reading from the beginning of the topic earliest, latest
    enable_auto_commit=True
    #value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening for messages on topic: {TOPIC_NAME}")

#consume and print messages
for message in consumer:
    print(f"Received message: {message.value}, topic: {message.topic}, partition: {message.partition}")

#close the consumer connection (this part might not be reached in a continuous loop in Colab)
#consumer.close()

