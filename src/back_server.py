import redis
import kafka
from redis_functions import get_session_redis
from kafka_functions import get_kafka_producer, get_kafka_consumer
from logger import Loger, logger
import yolov5
import asyncio
from pyspark.sql import SparkSession
import msgpack_numpy as magic


def get_spark_session():
    spark = SparkSession.Builder \
        .appName("YoloSession") \
        .config("spark.some.config.option", "") \
        .getOrCreate()
    return spark


def get_stream_from_kafka(spark):
    kafka_topic_stream = spark.readStream.formet("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "parsed_frames") \
        .option("startingOffsets", "latest") \
        .load()
    return kafka_topic_stream


class BackServer(object):
    @Loger
    def __init__(self):
        logger.info("Object BackServer init starting")
        self.__consumer: kafka.KafkaConsumer = get_kafka_consumer("parsed_frames")
        self.__producer: kafka.KafkaProducer = get_kafka_producer()
        self.__redis: redis.Redis = get_session_redis()
        self.__model = yolov5.load('yolov5s.pt')

    @Loger
    def __enter__(self):
        return self

    @Loger
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__consumer.close()
        self.__producer.close()
        self.__redis.close()
        self.__consumer, self.__producer, self.__redis, self.__frame_id = None, None, None, None
        logger.info("Object BackServer distracted")

    @Loger
    async def infinity_run(self):
        logger.info("Class started checking Kafka")
        while True:
            for url, link in self.__get_message():
                if link is not None and url is not None:
                    logger.info("Server got message from kafka")
                    frame = magic.unpackb(self.__redis.get(link))
                    if frame is not None:
                        print(frame, link, url, sep="\n")
                        await self.__picture_recognition(url, link, frame)
                    else:
                        print("No value on redis by link: ", link, " and url: ", url)

    async def __picture_recognition(self, url, link, frame):
        print("In __picture_recognition")
        frame = self.__model(frame).render()[0]
        self.__send_message(url, link, frame)

    def __get_message(self):
        message = self.__consumer.poll(timeout_ms=1.0)
        if len(message.keys()) == 0:
            yield None, None
        for key in message:
            for record in message[key]:
                yield record.key.decode("utf-8"), record.value.decode("utf-8")

    def __send_message(self, url, link, frame):
        link = link.encode()
        self.__redis.set(link, magic.packb(frame))
        self.__producer.send("recognized_frames", key=url.encode(), value=link)
        logger.info("Back sended message to karfka and redis")


@Loger
def start_back_server():
    with BackServer() as back:
        asyncio.run(back.infinity_run())


if __name__ == "__main__":
    start_back_server()
