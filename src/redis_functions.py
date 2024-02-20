import numpy as np
from loguru import logger
from logger import Loger
import redis
import base64


@Loger
def check_connection_redis():
    client_session = get_session_redis()
    return client_session.ping()


@Loger
def get_session_redis():
    return redis.Redis(host="localhost", port=6380)

