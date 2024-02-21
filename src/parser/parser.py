import redis
import kafka
from loguru import logger
import asyncio
import cv2 as cv
import msgpack_numpy as magic


class Parser(object):
    """
    A class representing a parser for processing video frames.
     Attributes:
        __consumer (KafkaConsumer): A Kafka consumer instance for retrieving stream URLs.
        __producer (KafkaProducer): A Kafka producer instance for sending parsed frames.
        __redis (Redis): A Redis client for storing frame data.
        __frame_id (int): An integer representing the current frame ID.
     Methods:
        __init__(): Initializes the Parser object by setting up Kafka consumers, producers, Redis client, and frame ID.
        __enter__(): Context manager entry method.
        __exit__(exc_type, exc_val, exc_tb): Context manager exit method for closing resources.
        infinity_run(): Asynchronous method to run an infinite loop for processing video frames.
        __parse_video(url: str): Asynchronous method to parse video frames from the specified URL.
        __get_image(cap): Generator function to retrieve frames from a video capture object.
        __send_message(frame, url): Sends the processed frame to Redis and Kafka.
        __get_message(): Retrieves messages from the Kafka consumer.
    """

    def __init__(self):
        """
        Initializes the KafkaConsumer, KafkaProducer, Redis connection, and sets the frame_id to 1.

        This method sets up the Kafka consumer to subscribe to 'stream_urls', initializes the Kafka producer with
        the bootstrap servers, creates a connection to Redis, and initializes the frame_id to 1.

        Returns:
            None
        """
        self.__consumer = kafka.KafkaConsumer("stream_urls", bootstrap_servers="localhost:9092",
                                              enable_auto_commit=True,
                                              auto_offset_reset='latest')
        self.__producer = kafka.KafkaProducer(bootstrap_servers=["localhost:9092"])
        self.__redis = redis.Redis(host="localhost", port=6380)
        self.__frame_id = 1

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
        Exits the context manager and closes KafkaConsumer, KafkaProducer, and Redis connections.

        This method is executed when exiting the context manager and closes the KafkaConsumer, KafkaProducer, and Redis
        connections. It also resets the internal attributes to None.

        Args:
            exc_type: The type of exception, if any.
            exc_val: The exception value, if any.
            exc_tb: The exception traceback, if any.

        Returns:
            None
        """
        self.__consumer.close()
        self.__producer.close()
        self.__redis.close()
        self.__consumer, self.__producer, self.__redis, self.__frame_id = None, None, None, None

    async def infinity_run(self):
        """
        Continuously checks for messages from Kafka, processes video streams asynchronously, and handles exceptions.

        This method initiates the process of checking Kafka for messages and asynchronously parsing video streams
        associated with the received messages. If valid client and URL data is retrieved from Kafka messages,
        it launches a task to parse the video stream using '__parse_video' asynchronously.

        Returns:
            None

        Note:
            - The function runs in an infinite loop, checking Kafka for messages continuously.
            - For each received client and URL data, it asynchronously launches the '__parse_video' task.
            - Any exceptions that occur during the process are logged with appropriate details.

        Raises:
            Any unexpected exceptions that occur during Kafka message retrieval or video parsing are logged.
        """
        try:
            logger.info("Class started checking Kafka")
            while True:
                for client, url in self.__get_message():
                    if client is not None and url is not None:
                        task = asyncio.create_task(self.__parse_video(url))
                        await task
        except Exception as ex:
            logger.exception(f"\n[CLASS] Parser\n[FUNC] infinity_run\nException:\n{ex}")

    async def __parse_video(self, url: str):
        """
        Processes a video stream from the provided URL by capturing frames, resizing them, and sending
        them for further processing.

        Args:
            url (str): The URL of the video stream to be processed.

        Returns:
            None

        Note:
            The function continuously captures frames from the video stream, resizes each frame to (1024, 1024) dimensions,
            and sends the processed frame along with the URL for further processing.
            The function releases the video capture source and closes all OpenCV windows if an exception occurs.

        Raises:
            Any exceptions that may occur during video processing are caught and ignored.
        """
        try:
            while True:
                cap = cv.VideoCapture(url)
                for ret, frame in self.__get_image(cap):
                    if ret:
                        frame = cv.resize(frame, (1024, 1024))
                        self.__send_message(frame, url)
                        self.__frame_id += 1
                cap.release()
                cv.destroyAllWindows()
        except Exception as ex:
            pass

    def __get_image(self, cap):
        """
        Generates image frames from a video capture source.

        Args:
            cap: The video capture source.

        Yields:
            A tuple (ret, frame) where 'ret' is a boolean indicating whether a frame was captured successfully
            and 'frame' is the captured frame. If no frame is captured, yields (None, None).

        Note:
            The function runs in an infinite loop continuously fetching frames from the video capture source.
        """
        while True:
            ret, frame = cap.read()
            if ret:
                yield ret, frame

            yield None, None

    def __send_message(self, frame, url):
        """
        Sends a message containing the frame data and URL to Redis and Kafka for processing.

        Args:
            frame: The frame data to be sent.
            url: The URL associated with the frame.

        Returns:
            None

        Raises:
            Any exceptions that may occur during data storing or message sending operations.
        """
        frame_id = str(self.__frame_id).encode()
        self.__redis.set(frame_id, magic.packb(frame))
        self.__redis.expire(frame_id, 5)
        self.__producer.send("parsed_frames", key=url.encode(), value=frame_id)

    def __get_message(self):
        """
        Reads a message from the Kafka consumer and decodes the key and value fields to UTF-8 format.

        Returns:
            A generator yielding tuples of (key, value) if messages are read from Kafka.
            If no messages are read, yields (None, None).

        Raises:
            Any exceptions that occur during Kafka message polling may be propagated.
        """
        message = self.__consumer.poll(timeout_ms=1.0)
        if len(message.keys()) != 0:
            for key in message:
                for record in message[key]:
                    yield record.key.decode("utf-8"), record.value.decode("utf-8")
        yield None, None


def start_parser_server():
    """
    Starts the parser server for processing video frames.
    """
    try:
        logger.info(f"File: parser.py started")
        with Parser() as parser:
            asyncio.run(parser.infinity_run())
    except KeyboardInterrupt:
        logger.warning("Parser stoped working")


if __name__ == "__main__":
    start_parser_server()
else:
    logger.error("The parser.py module cannot be run by module")
