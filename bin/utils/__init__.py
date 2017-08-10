import time
from .s3 import S3Location, S3Agent, S3ObjectTagger
from .parallel_logger import logger


def measure_duration_and_rate(func,  *args, size):
    retval, duration = measure_duration(func, *args)
    rate_mb_s = (size / duration) / (1024 * 1024)
    return retval, duration, rate_mb_s


def measure_duration(func, *args):
    start_time = time.time()
    retval = func(*args)
    end_time = time.time()
    return retval, end_time - start_time


def sizeof_fmt(num, suffix='B'):
    """
    From https://stackoverflow.com/a/1094933
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)
