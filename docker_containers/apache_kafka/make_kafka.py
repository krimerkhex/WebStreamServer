import argparse
import os


def stop_docker():
    os.system("docker compose -f docker-compose.yml down")


def run_docker():
    os.system("docker compose -f docker-compose.yml up -d")


def init_parser():
    argc = argparse.ArgumentParser("Kafka docker file")
    argc.add_argument("command", choices=['run', 'stop'], help="Commands of this python file")
    return argc.parse_args()


def main():
    args = init_parser()
    if args.command == "run":
        run_docker()
    elif args.command == "stop":
        stop_docker()


if __name__ == "__main__":
    main()
