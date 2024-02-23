import os
import subprocess
import sys
from time import time_ns, sleep
import logging
from subprocess import Popen


def check_platform():
    return sys.platform == 'win32' or sys.platform == 'linux'


def check_python_version():
    return sys.version_info.major == 3 and sys.version_info.minor >= 11


def run_all_file():
    try:
        files = (
            "src/front_server/front_server.py", "src/parser/frame_parser.py", ""
                                                                              "src/back_server/yolov_recognition.py" )
        # files = (
        #     "src/parser/frame_parser.py", "src/front_server/front_server.py")

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
            logging.info("The docker container with kafka needs about 40 seconds to fully initialize.")
            sleep(40)
            logging.info("Running python files. Wait please.")
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
