import time
from os import stat
import urllib3
from urllib3.util import parse_url
from ftplib import FTP

KB = 1024
MB = KB * KB
GB = KB * MB
TB = KB * GB

http = urllib3.PoolManager()


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


def file_size(url: str) -> int:
    urlbits = parse_url(url)
    if urlbits.scheme == 'http':
        return int(http.request('HEAD', url).headers['Content-Length'])
    elif urlbits.scheme == 'ftp':
        return ftp_file_size(urlbits)
    elif urlbits.scheme == 'file':
        return stat(urlbits.path).st_size
    else:
        raise RuntimeError(f"Don't know how to size a file of scheme: {urlbits.scheme}: {url}")


def ftp_file_size(ftp_url):
    ftp = FTP(ftp_url.netloc)
    ftp.login()
    size = ftp.size(ftp_url.path)
    ftp.quit()
    return size
