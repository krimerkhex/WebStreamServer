import kafka
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from kafka_functions import get_kafka_consumer, get_kafka_producer
from uvicorn import run
from redis_functions import get_session_redis
import redis
from logger import Loger, logger
from PIL import Image
import msgpack_numpy as magic
import io


# yield image_bytes
# http://localhost:8888/streaming/http:++211.132.61.124+mjpg+video.mjpg/
# http://localhost:8888/streaming/http:++77.73.54.134:88+webcapture.jpg?command=snap&channel=8?0.3060441690920612/
# http://localhost:8888/streaming/http:++24.227.79.246+cgi-bin+viewer+video.jpg?r=1707923972
# http://77.73.54.134:88/webcapture.jpg?command=snap&channel=8?0.3060441690920612/

class FrontServer(FastAPI):
    def __init__(self, **fastapi_init):
        logger.info("Object FrontServer init starting")
        super().__init__(**fastapi_init)
        self.__templates = Jinja2Templates(directory="templates")
        self.__consumer: kafka.KafkaConsumer = get_kafka_consumer("recognized_frames")
        self.__producer: kafka.KafkaProducer = get_kafka_producer()
        self.__redis: redis.Redis = get_session_redis()
        self.add_api_route(path="/", endpoint=self.__hello_world, response_class=HTMLResponse, methods=["GET"])
        self.add_route("/stream_video/", self.__upload_stream, methods=["GET"])
        self.add_api_route(path="/streaming/{url}/", endpoint=self.__prepare_stream, response_class=HTMLResponse,
                           methods=["GET"])
        self.num = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__consumer.close()
        self.__producer.close()
        self.__redis.close()
        self.__consumer, self.__producer, self.__redis, self.__frame_id = None, None, None, None
        logger.info("Object FrontServer distracted")

    async def __hello_world(self, request: Request):
        return self.__templates.TemplateResponse("index.html", {'request': request})

    @Loger
    def __produce_url(self, host: str, port: int, url: str):
        client = f"{host}:{port}".encode()
        url = url.replace("+", "/").encode()
        self.__producer.send("stream_urls", key=client, value=url)

    @Loger
    async def __get_video_frames(self):
        flag = True
        logger.info("Class started checking Kafka")
        while flag:
            try:
                message = self.__consumer.poll(timeout_ms=10)
                if len(message.keys()) != 0:
                    for key in message:
                        for record in message[key]:
                            url, value = record.key.decode("utf-8"), record.value.decode("utf-8")
                            frame = self.__redis.get(value)
                            if frame is not None:
                                self.__redis.delete(value)
                                image = Image.fromarray(magic.unpackb(frame))
                                with io.BytesIO() as output:
                                    image.save(output, format='JPEG')
                                    image_bytes = output.getvalue()
                                logger.info("Server sended frame")
                                yield image_bytes
            except Exception as ex:
                logger.exception(
                    f"\n[CLASS] FrontServer\n[FUNC] __start_streaming\n[DESCRIPTION] Something wrong\n{ex}")
                flag = False

    @Loger
    def __upload_stream(self, request: Request):
        return StreamingResponse(self.__get_video_frames(), status_code=200)

    async def __prepare_stream(self, request: Request, url: str):
        print(request.client, request.url)
        if len(request.query_params) != 0:
            url = url + "?" + str(request.query_params)
        self.__produce_url(request.client.host, request.client.port, url)
        return self.__templates.TemplateResponse("stream.html", {'request': request})


def start_front_server():
    try:
        with FrontServer(title="We're streaming slowly...") as server:
            run(server, host="127.0.0.1", port=8888, log_level="info")
    except KeyboardInterrupt:
        logger.warning("Front-end stoped working")


if __name__ == "__main__":
    start_front_server()
