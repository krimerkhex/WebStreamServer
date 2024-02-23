import os
import subprocess
import sys
from time import time_ns, sleep
import logging
from subprocess import Popen
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from socket import gethostname


def init_kafka_topics(admin_client):
    try:
        logging.info("Start create topics")
        admin_client.create_topics(new_topics=[NewTopic(name="stream_urls", num_partitions=2, replication_factor=1)],
                                   validate_only=False)
        admin_client.create_topics(
            new_topics=[NewTopic(name="recognized_frames", num_partitions=2, replication_factor=1)],
            validate_only=False)
        admin_client.create_topics(new_topics=[
            NewTopic(name="parsed_frames", num_partitions=2, replication_factor=1),
        ],
            validate_only=False)
    except Exception as ex:
        logging.exception(ex)


def check_kafka_topics():
    logging.info("Checking kafka topics")
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id=gethostname())
    current_topics = admin_client.list_topics()
    if len(current_topics) == 0:
        init_kafka_topics(admin_client)
    else:
        admin_client.delete_topics(current_topics, timeout_ms=0)
        sleep(20)
        init_kafka_topics(admin_client)
    admin_client.close()


def check_platform():
    return sys.platform == 'win32' or sys.platform == 'linux'


def check_python_version():
    return sys.version_info.major == 3 and sys.version_info.minor >= 11


def run_all_file():
    try:
        files = (
            "src/front_server/front_server.py", "src/parser/frame_parser.py", "src/back_server/yolov_recognition.py")
        for file in files:
            Popen(args=["start", "python", file], shell=True, stdout=subprocess.PIPE)
        logging.info("The files started working in their terminals.")
        return True
    except Exception as ex:
        logging.exception(ex)
        return False


def install_requirements():
    logging.info("Install requirements started.")
    flag = True
    if os.system("pip install -r requirements.txt") == 0:
        logging.info("Python modules installed. Trying to start project.")
    else:
        flag = False
        logging.info("Error on installing python modules.")
    return flag


def run_docker():
    try:
        logging.info("Building and running the entire application.")
        flag = True
        if os.system("docker compose -f docker_containers/docker-compose.yml up -d") != 0:
            flag = False
            logging.error("Error on docker container creation.")
    except Exception as ex:
        logging.exception(ex)
        flag = False
    return flag


def make():
    if run_docker():
        if install_requirements():
            print("Wait 20 second full init of docker containers")
            sleep(20)
            check_kafka_topics()
            if run_all_file():
                print(
                    "Project ready to use. Web application start work by http://127.0.0.1:8888. Good luck!")


def main():
    logging.basicConfig(filename=f'logs/build_{time_ns()}.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    if check_platform():
        if check_python_version():
            make()
        else:
            logging.error(
                f"This project must work on python 3.11. Current python version: {sys.version_info.major}.{sys.version_info.minor}")
    else:
        logging.error(f"This project must work on Windows or Linux. Current system is: {sys.platform}")


if __name__ == "__main__":
    main()
