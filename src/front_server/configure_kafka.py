from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import socket


def delete_topics():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.delete_topics(topics=["recognized_frames"])
    admin_client.delete_topics(topics=["stream_urls"])
    admin_client.delete_topics(topics=["parsed_frames"])
    admin_client.close()


def init_kafka_topics():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.create_topics(new_topics=[NewTopic(name="stream_urls", num_partitions=2, replication_factor=1)],
                               validate_only=False)
    admin_client.create_topics(new_topics=[NewTopic(name="recognized_frames", num_partitions=2, replication_factor=1)],
                               validate_only=False)
    admin_client.create_topics(new_topics=[
        NewTopic(name="parsed_frames", num_partitions=2, replication_factor=1),
    ],
        validate_only=False)
    admin_client.close()
