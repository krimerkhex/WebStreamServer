import os
import sys
from docker_containers.apache_kafka import make_kafka as kafka
from docker_containers.redis import make_redis as redis
from src.front_server import start_server


def check_platform():
    return sys.platform == 'win32' or sys.platform == 'linux'


def check_python_version():
    return sys.version_info.major == 3 and sys.version_info.minor >= 10


def check_venv():
    return 'venv' in os.listdir()


def venv():
    print("Creating virtual environment.")
    if os.system("python -m venv venv") == 0:
        if check_venv():
            print(f"Virtual environment created.\nInstalling python modules")
            if os.system("pip install -r requirements.txt") == 0:
                print("Python modules installed.")
            else:
                print("Error on installing python modules.")
    else:
        print("Error while virtual environment creating.")


def make():
    print("Building and running the entire application.")
    if not check_venv():
        venv()
    kafka.run_docker()
    # spark.run_docker()
    redis.run_docker()
    start_server()


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
