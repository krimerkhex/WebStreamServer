import time
import redis
from redis_functions import get_session_redis
from kafka_functions import get_kafka_producer, get_kafka_consumer
import kafka
from logger import Loger, logger
import asyncio
import cv2 as cv
import requests
import msgpack_numpy as magic


class Parser(object):
    def __init__(self):
        logger.info("Object Parser init starting")
        self.__consumer: kafka.KafkaConsumer = get_kafka_consumer("stream_urls")
        self.__producer: kafka.KafkaProducer = get_kafka_producer()
        self.__redis: redis.Redis = get_session_redis()
        self.__frame_id = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__consumer.close()
        self.__producer.close()
        self.__redis.close()
        self.__consumer, self.__producer, self.__redis, self.__frame_id = None, None, None, None

    async def infinity_run(self):
        try:
            logger.info("Class started checking Kafka")
            while True:
                for client, url in self.__get_message():
                    if client is not None and url is not None:
                        task = asyncio.create_task(self.__parse_video(url))
                        await task
        except Exception as ex:
            logger.exception(f"\n[CLASS] Parser\n[FUNC] infinity_run\nException:\n{ex}")

    def __check_url_status(self, url: str):
        code = requests.get(url).status_code
        counter = 0
        while code != 200:
            code = requests.get(url).status_code
            counter += 1
            if counter == 60:
                raise TimeoutError(f"Url: {url}. After 60 seconds status code not 200")
            time.sleep(1)
        return code

    async def __parse_video(self, url: str):
        # code = self.__check_url_status(url)
        code = 200
        if code == 200:
            while True:
                cap = cv.VideoCapture(url)
                for ret, frame in self.__get_image(cap):
                    if ret:
                        self.__send_message(frame, url)
                        self.__frame_id += 1
                cap.release()
                cv.destroyAllWindows()
        else:
            logger.error(f"Parsing frame's from {url} failed, status code: {code}")
            pass

    def __get_image(self, cap):
        while True:
            ret, frame = cap.read()
            if ret:
                yield ret, frame
            yield None, None

    def __send_message(self, frame, url):
        link = str(self.__frame_id).encode()
        self.__redis.set(link, magic.packb(frame))
        self.__producer.send("parsed_frames", key=url.encode(), value=link)

    def __get_message(self):
        message = self.__consumer.poll(timeout_ms=1.0)
        if len(message.keys()) != 0:
            for key in message:
                for record in message[key]:
                    yield record.key.decode("utf-8"), record.value.decode("utf-8")
        yield None, None


def start_parser_server():
    try:
        with Parser() as parser:
            asyncio.run(parser.infinity_run())
    except KeyboardInterrupt:
        logger.warning("Parser stoped working")


if __name__ == "__main__":
    start_parser_server()
