from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
import socket
from logger import Loger

@Loger
def delete_topics():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.delete_topics(topics=["recognized_frames"])
    admin_client.close()
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.delete_topics(topics=["stream_urls"])
    admin_client.close()
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.delete_topics(topics=["parsed_frames"])
    admin_client.close()


@Loger
def init_kafka_topics():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.create_topics(new_topics=[NewTopic(name="stream_urls", num_partitions=2, replication_factor=1)],
                               validate_only=False)
    # admin_client.close()

    # admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.create_topics(new_topics=[NewTopic(name="recognized_frames", num_partitions=2, replication_factor=1)],
                               validate_only=False)
    # admin_client.close()

    # admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
    admin_client.create_topics(new_topics=[
        NewTopic(name="parsed_frames", num_partitions=2, replication_factor=1),
    ],
        validate_only=False)
    admin_client.close()


@Loger
def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=["localhost:9092"])


@Loger
def get_kafka_consumer(topic: str):
    return KafkaConsumer(topic, bootstrap_servers="localhost:9092", enable_auto_commit=True,
                         auto_offset_reset='earliest')


def main():
    # try:
        # delete_topics()
    # except Exception as ex:
    #     pass
    init_kafka_topics()


if __name__ == "__main__":
    main()
