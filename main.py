import os
import subprocess
import sys
from docker_containers.configure_dockers import init_kafka_topics, delete_topics
from loguru import logger
from subprocess import Popen

delimeter = "/"
files = ("src/back_server/back_server.py", "src/parser/parser.py", "src/front_server/front_server.py")

def check_platform():
    return sys.platform == 'win32' or sys.platform == 'linux'


def check_python_version():
    return sys.version_info.major == 3 and sys.version_info.minor >= 11


def check_venv():
    return 'venv' in os.listdir()


def run_all_file():
    for file in files:
        Popen(args=["start", "python", file], shell=True, stdout=subprocess.PIPE)


def install_requirements():
    if os.system("pip install -r requirements.txt") == 0:
        print("Python modules installed. Trying to start project")
    else:
        print("Error on installing python modules.")


def venv():
    print("Creating virtual environment.")
    if os.system("python -m venv venv") == 0:
        if check_venv():
            print(f"Virtual environment created.\nInstalling python modules")
    else:
        print("Error while virtual environment creating.")


def init_kafka():
    try:
        delete_topics()
        init_kafka_topics()
    except Exception as ex:
        logger.exception(ex)


def run_docker():
    os.system("docker compose -f docker_containers/docker-compose.yml up -d")


def stop_socker():
    os.system("docker compose -f docker_containers/docker-compose.yml down")


def make():
    logger.info("Building and running the entire application.")
    run_docker()

    logger.info("Install requirements started")
    install_requirements()

    logger.info("Init topics in process")
    init_kafka()
    logger.info("Topics created")
    logger.info("Running python files. Wait please.")
    run_all_file()
    logger.info("Project ready to use. Web application start work by http://127.0.0.1:8888. Good luck!")


def main():
    if len(sys.argv) == 2:
        if check_platform() and check_python_version():
            if sys.argv[1] == "make":
                make()
            elif sys.argv[1] == "venv":
                venv()
        else:
            print("This application work's only on windows or linux with python 3.10 and more.")
            print("Something wrong with you environment!")
    else:
        print("A lot of arguments line parameters. You can use only: [make, venv]")


if __name__ == "__main__":
    main()
