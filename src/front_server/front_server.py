import kafka
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from uvicorn import run
import redis
from PIL import Image
import msgpack_numpy as magic
import io
from time import time_ns


class FrontServer(FastAPI):
    """
    A class representing a front-end server for streaming video frames.
     Attributes:
        __templates (Jinja2Templates): Jinja2Templates instance for rendering templates.
        __consumer (KafkaConsumer): Kafka consumer instance for receiving recognized frames.
        __producer (KafkaProducer): Kafka producer instance for sending stream URLs.
        __redis (Redis): Redis client instance for storing frame data.
     Methods:
        __init__(self, **fastapi_init): Initializes the FrontServer object with additional FastAPI parameters.
        __enter__(self): Context manager entry method.
        __exit__(self, exc_type, exc_val, exc_tb): Context manager exit method.
        __hello_world(self, request: Request): Handles the 'GET /' endpoint to display the index.html template.
        __produce_url(self, host: str, port: int, url: str): Produces a URL for streaming.
        __get_video_frames(self): Generator function to retrieve video frames from Kafka and Redis.
        __upload_stream(self, request: Request): Handles the 'GET /stream_video/' endpoint for streaming video frames.
        __prepare_stream(self, request: Request, url: str): Prepares the stream for the specified URL.
    """

    def __init__(self, **fastapi_init):
        """Initializes the FrontServer object.
         Args:
            **fastapi_init: Additional initialization parameters for FastAPI.
        """
        logger.info("Object FrontServer init starting")
        super().__init__(**fastapi_init)
        self.__templates = Jinja2Templates(directory="templates")
        self.__consumer = kafka.KafkaConsumer("recognized_frames", bootstrap_servers="localhost:9092",
                                              enable_auto_commit=True,
                                              auto_offset_reset='latest')
        self.__producer = kafka.KafkaProducer(bootstrap_servers=["localhost:9092"])
        self.__redis = redis.Redis(host="localhost", port=6380)
        self.add_api_route(path="/", endpoint=self.__hello_world, response_class=HTMLResponse, methods=["GET"])
        self.add_route("/stream_video/", self.__upload_stream, methods=["GET"])
        self.add_api_route(path="/streaming/{url}/", endpoint=self.__prepare_stream, response_class=HTMLResponse,
                           methods=["GET"])

    def __enter__(self):
        """Context manager entry method."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit method."""
        self.__consumer.close()
        self.__producer.close()
        self.__redis.close()
        self.__consumer, self.__producer, self.__redis, self.__frame_id = None, None, None, None
        logger.info("Object FrontServer distracted")

    async def __hello_world(self, request: Request):
        """Handles the 'GET /' endpoint to display the index.html template.
         Args:
            request (Request): The incoming request object.
         Returns:
            TemplateResponse: The response containing the rendered index.html template.
        """
        return self.__templates.TemplateResponse("index.html", {'request': request})

    def __produce_url(self, host: str, port: int, url: str):
        """Produces a URL for streaming.
         Args:
            host (str): The host of the URL.
            port (int): The port of the URL.
            url (str): The URL to be produced for streaming.
        """
        client = f"{host}:{port}".encode()
        url = url.replace("+", "/").encode()
        self.__producer.send("stream_urls", key=client, value=url)

    async def __get_video_frames(self):
        """Generator function to retrieve video frames from Kafka and Redis.
         Yields:
            bytes: Image frames in JPEG format.
        """
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
                                image = Image.fromarray(magic.unpackb(frame))
                                with io.BytesIO() as output:
                                    image.save(output, format='JPEG')
                                    image_bytes = output.getvalue()
                                yield (b'--frame\r\n'
                                       b'Content-Type: image/jpeg\r\n\r\n' + image_bytes + b'\r\n')
            except Exception as ex:
                logger.exception(
                    f"\n[CLASS] FrontServer\n[FUNC] __start_streaming\n[DESCRIPTION] Something wrong\n{ex}")
                flag = False

    def __upload_stream(self, request: Request):
        """Handles the 'GET /stream_video/' endpoint for streaming video frames.
         Args:
            request (Request): The incoming request object.
         Returns:
            StreamingResponse: The streaming response with video frames.
        """
        return StreamingResponse(self.__get_video_frames(), media_type='multipart/x-mixed-replace; boundary=frame')

    async def __prepare_stream(self, request: Request, url: str):
        """Prepares the stream for the specified URL.
         Args:
            request (Request): The incoming request object.
            url (str): The URL for streaming.
         Returns:
            TemplateResponse: The response containing the rendered stream.html template.
        """
        if len(request.query_params) != 0:
            url = url + "?" + str(request.query_params)
        self.__produce_url(request.client.host, request.client.port, url)
        return self.__templates.TemplateResponse("stream.html", {'request': request})


def start_front_server():
    """Starts the front-end server for streaming video frames."""
    try:
        logger.add(f"logs/front_server_{time_ns()}.log")
        with FrontServer(title="We're streaming slowly...") as server:
            run(server, host="127.0.0.1", port=8888, log_level="info")
    except KeyboardInterrupt:
        logger.warning("Front-end stoped working")


if __name__ == "__main__":
    start_front_server()
else:
    logger.error("The front_server.py module cannot be run by module")
