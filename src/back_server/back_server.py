import redis
import kafka
from loguru import logger
import yolov5
from pyspark.sql import SparkSession
import msgpack_numpy as magic
from logger import Loger


class BackServer(object):
    def __init__(self):
        self.__producer = kafka.KafkaProducer(bootstrap_servers=["localhost:9092"])
        self.__spark = self.__get_spark_session()
        self.__spark_stream = self.__get_stream_from_kafka()
        self.__redis = redis.Redis(host="localhost", port=6380)
        self.__model = yolov5.load('../yolov5s.pt')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__producer.close()
        self.__redis.close()
        self.__spark.stop()
        self.__consumer, self.__producer, self.__redis, self.__frame_id = None, None, None, None
        logger.info("Object BackServer distracted")

    def __get_spark_session(self):
        try:
            spark = SparkSession.builder \
                .appName("ParserSession") \
                .master("local[*]") \
                .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
                .getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
        except Exception as ex:
            logger.error(f"Spark connection don't created\n.Exception:\n{ex}")
        return spark

    def __get_stream_from_kafka(self):
        try:
            kafka_topic_stream = self.__spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "parsed_frames") \
                .option("startingOffsets", "latest") \
                .load()
        except Exception as ex:
            logger.error(f"Kafka stream doesn't open\n.Exeption:\n{ex}")
        return kafka_topic_stream

    def __get_df_from_kafka_stream(self):
        queue = self.__spark_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        return queue

    @Loger
    def __send_message(self, url, frame_id, frame):
        frame_id = frame_id.encode()
        self.__redis.set(frame_id, magic.packb(frame))
        self.__redis.expire(frame_id, 5)
        self.__producer.send("recognized_frames", key=url.encode(), value=frame_id)
        logger.info("Back sended message to karfka and redis")

    @Loger
    def __picture_recognition(self, url, frame_id, frame):
        frame = self.__model(frame).render()[0]
        self.__send_message(url, frame_id, frame)

    @Loger
    def __get_frame(self, batch_df, batch_id):
        data_collect = batch_df.collect()
        for data_row in data_collect:
            frame_id = data_row["value"]
            self.__picture_recognition(data_row["key"], frame_id, magic.unpackb(self.__redis.get(frame_id)))

    def infinity_run(self):
        logger.info("Class started checking Kafka")
        query = self.__get_df_from_kafka_stream().writeStream.foreachBatch(self.__get_frame).outputMode(
            "append").start()
        query.awaitTermination()


def start_back_server():
    logger.info(f"File: back_server.py started")
    try:
        with BackServer() as back:
            back.infinity_run()
    except KeyboardInterrupt:
        logger.warning("Back-end stoped working")


if __name__ == "__main__":
    start_back_server()
else:
    logger.error("The back_server.py module cannot be run by module")
