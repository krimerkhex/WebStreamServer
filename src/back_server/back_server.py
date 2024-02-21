import redis
import kafka
from loguru import logger
import yolov5
from pyspark.sql import SparkSession
import msgpack_numpy as magic
from logger import Loger


class BackServer(object):
    """
    Represents a backend server responsible for processing video frames from Kafka,
    performing picture recognition using YOLOv5, and sending messages to Kafka and Redis.

    Attributes:
        - __producer: KafkaProducer for sending messages to Kafka.
        - __spark: SparkSession for processing streaming data.
        - __spark_stream: Kafka streaming DataFrame retrieved for processing.
        - __redis: Redis connection for caching data.
        - __model: YOLOv5 model for picture recognition.

    Methods:
        - __init__: Initializes the backend server with required connections and setups.
        - __enter__: Enters the context manager.
        - __exit__: Exits the context manager and closes connections.
        - __get_spark_session: Sets up and returns a SparkSession.
        - __get_stream_from_kafka: Retrieves Kafka streaming DataFrame.
        - __get_df_from_kafka_stream: Processes Kafka streaming DataFrame.
        - __send_message: Sends processed frames to Kafka and Redis.
        - __picture_recognition: Performs picture recognition using YOLOv5.
        - __get_frame: Processes batch DataFrame with frame data.
        - infinity_run: Runs the infinite loop for processing Kafka stream data.

    Note:
        This class integrates Kafka, Spark, Redis, and YOLOv5 for real-time video processing and recognition.
    """

    def __init__(self):
        """
        Initializes the backend server with necessary connections and setups.

        This method sets up the KafkaProducer, establishes a connection to Spark using '__get_spark_session',
        retrieves a streaming DataFrame from Kafka using '__get_stream_from_kafka', connects to Redis,
        and loads the YOLOv5 model for picture recognition.

        Returns:
            None
        """
        self.__producer = kafka.KafkaProducer(bootstrap_servers=["localhost:9092"])
        self.__spark = self.__get_spark_session()
        self.__spark_stream = self.__get_stream_from_kafka()
        self.__redis = redis.Redis(host="localhost", port=6380)
        self.__model = yolov5.load('../yolov5s.pt')

    def __enter__(self):
        """
        Enters the context manager and returns the instance itself.

        This method is called when entering the context manager and returns the instance of the class itself.

        Returns:
            self
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exits the context manager and closes connections.

        This method is executed when exiting the context manager and performs cleanup tasks such as closing
        connections to KafkaProducer, Redis, stopping SparkSession, resetting internal attributes to None,
        and logging a message indicating the distraction of the BackServer object.

        Args:
            exc_type: The type of exception, if any.
            exc_val: The exception value, if any.
            exc_tb: The exception traceback, if any.

        Returns:
            None
        """
        self.__producer.close()
        self.__redis.close()
        self.__spark.stop()
        self.__consumer, self.__producer, self.__redis, self.__frame_id = None, None, None, None
        logger.info("Object BackServer distracted")

    def __get_spark_session(self):
        """
        Sets up and returns a SparkSession instance for processing streaming data.

        This method attempts to create a SparkSession with the given configurations. If successful, it sets the log level
        to 'ERROR'. If an exception occurs during SparkSession creation, it logs the error and returns None.

        Returns:
            SparkSession: A SparkSession instance for processing streaming data.
        """
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
        """
        Retrieves the Kafka streaming DataFrame for processing.

        This method tries to read a streaming DataFrame from the Kafka topic 'parsed_frames'. If successful,
        it returns the loaded streaming DataFrame. If an exception occurs during the process, it logs the error
        and returns None.

        Returns:
            DataFrame: Kafka streaming DataFrame for further processing.
        """
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
        """
        Processes the Kafka streaming DataFrame and returns the processed data as a DataFrame.

        This method processes the received DataFrame from Kafka streaming, casting 'key' and 'value' columns to strings.
        It returns the processed DataFrame ready for further handling and analysis.

        Returns:
            DataFrame: Processed DataFrame from Kafka streaming for downstream usage.
        """
        queue = self.__spark_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        return queue

    def __send_message(self, url, frame_id, frame):
        """
        Sends a message containing processed frame data to Kafka and Redis.

        This method encodes the frame_id, sets the frame data in Redis, sets an expiry time of 5 seconds for the frame_id,
        and sends the data to the 'recognized_frames' topic in Kafka with the provided URL as the key.
        It logs a message indicating the successful transmission to Kafka and Redis.

        Args:
            url (str): URL associated with the frame.
            frame_id (str): ID of the frame.
            frame (bytes): Frame data to be sent.

        Returns:
            None
        """
        frame_id = frame_id.encode()
        self.__redis.set(frame_id, magic.packb(frame))
        self.__redis.expire(frame_id, 5)
        self.__producer.send("recognized_frames", key=url.encode(), value=frame_id)
        logger.info("Back sended message to karfka and redis")

    def __picture_recognition(self, url, frame_id, frame):
        """
        Performs picture recognition using the YOLOv5 model on the provided frame.

        This method processes the frame using the YOLOv5 model and sends the recognized frame using '__send_message'.

        Args:
            url (str): URL associated with the frame.
            frame_id (str): ID of the frame.
            frame (bytes): Frame data for recognition.

        Returns:
            None
        """
        frame = self.__model(frame).render()[0]
        self.__send_message(url, frame_id, frame)

    def __get_frame(self, batch_df, batch_id):
        """
        Processes batch DataFrame with frame data for picture recognition.

        This method collects data from the batch DataFrame, extracts frame data, performs picture recognition using
        '__picture_recognition', and handles the recognition results.

        Args:
            batch_df (DataFrame): Batch DataFrame with frame data.
            batch_id: Identifier for the batch.

        Returns:
            None
        """
        data_collect = batch_df.collect()
        for data_row in data_collect:
            frame_id = data_row["value"]
            self.__picture_recognition(data_row["key"], frame_id, magic.unpackb(self.__redis.get(frame_id)))

    def infinity_run(self):
        """
        Initiates the process of continuously checking Kafka for new data and processing video frames.

        This method starts a streaming query from Kafka using the processed DataFrame obtained from Kafka streaming.
        It specifies an output mode of 'append' and starts the streaming query, waiting for termination.

        Returns:
            None
        """
        logger.info("Class started checking Kafka")
        query = self.__get_df_from_kafka_stream().writeStream.foreachBatch(self.__get_frame).outputMode(
            "append").start()
        query.awaitTermination()


def start_back_server():
    """
    Starts the backend server for processing Kafka stream data.

    This function initiates the BackServer, enters the context manager, starts the streaming process, and handles exceptions.
    It logs the start of the 'back_server.py' file and provides a warning message upon unexpected interruption by the user.

    Returns:
        None
    """
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
