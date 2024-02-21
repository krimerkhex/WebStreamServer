import redis
import kafka
from loguru import logger
import asyncio
import cv2 as cv
import msgpack_numpy as magic


class Parser(object):
    def __init__(self):
        self.__consumer = kafka.KafkaConsumer("stream_urls", bootstrap_servers="localhost:9092",
                                              enable_auto_commit=True,
                                              auto_offset_reset='latest')
        self.__producer = kafka.KafkaProducer(bootstrap_servers=["localhost:9092"])
        self.__redis = redis.Redis(host="localhost", port=6380)
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

    async def __parse_video(self, url: str):
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
        while True:
            ret, frame = cap.read()
            if ret:
                yield ret, frame

            yield None, None

    def __send_message(self, frame, url):
        frame_id = str(self.__frame_id).encode()
        self.__redis.set(frame_id, magic.packb(frame))
        self.__redis.expire(frame_id, 5)
        self.__producer.send("parsed_frames", key=url.encode(), value=frame_id)

    def __get_message(self):
        message = self.__consumer.poll(timeout_ms=1.0)
        if len(message.keys()) != 0:
            for key in message:
                for record in message[key]:
                    yield record.key.decode("utf-8"), record.value.decode("utf-8")
        yield None, None


def start_parser_server():
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
