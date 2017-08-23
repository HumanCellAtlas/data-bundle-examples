from os import stat
import urllib3
from urllib3.util import parse_url
from ftplib import FTP

KB = 1024
MB = KB * KB
GB = KB * MB
TB = KB * GB

http = urllib3.PoolManager()


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
