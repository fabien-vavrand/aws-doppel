import sys
import logging
import pathlib


def get_root_path():
    return str(pathlib.Path(__file__).parent.parent)


def console_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(
        fmt='%(asctime)-15s %(name)-15s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')
    )
    logger.addHandler(handler)
