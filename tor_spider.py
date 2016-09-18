import urllib.parse
import time

import socks
import asyncio
from typing import Dict
import aiohttp
import aiosocks
from http_parser.parser import HttpParser
from aiosocks.connector import SocksConnector


class SiteNotFound(Exception):
    """Exception raised when the given URL has not been found"""

    def __str__(self):
        return "Site not found"


class Response:

    def __init__(self, url: str, status_code: int=0, headers: Dict=None, content: bytes=''):
        self.url = url
        self.status_code = status_code
        self.headers = headers
        self.content = content

    def __str__(self):
        return '<Response [{}]>'.format(self.status_code)

    @property
    def text(self):
        return self.content.decode('utf8')

    def has_headers(self):
        return bool(self.headers)

    def has_status_code(self):
        return bool(self.status_code)


async def get2(url: str):

    conn = SocksConnector(proxy=aiosocks.Socks5Addr('127.0.0.1', 9150), proxy_auth=None, remote_resolve=True)
    try:
        with aiohttp.ClientSession(connector=conn) as session:
            async with session.get(url) as resp:
                r = Response(url)
                r.status_code = resp.status
                r.headers = resp.headers
                r.content = await resp.text()
                return r
    except aiohttp.ProxyConnectionError:
        # connection problem
        print('oups')
    except aiosocks.SocksError:
        # communication problem
        print('oula')


async def get(url: str, allow_redirects: bool=True, referrer: str='https://www.google.com/') -> Response:
    """Performs a GET request on the given url. Returns a Response object."""

    url_obj = urllib.parse.urlsplit(url)
    s = socks.socksocket()

    # Sends request
    # if url_obj.scheme == 'https':
    #     s.connect((url_obj.hostname, 443))
    #     connect = asyncio.open_connection(server_hostname='', ssl=True, sock=s)
    # else:
    #     s.connect((url_obj.hostname, 80))
    #     connect = asyncio.open_connection(sock=s)
    # reader, writer = await connect

    reader, writer = await aiosocks.open_connection(
        proxy=aiosocks.Socks5Addr('127.0.0.1', 9150),
        # dst=('url', 443),
        dst=('url', 80),
        remote_resolve=True,
        proxy_auth=None
    )

    query = ('GET {path} HTTP/1.0\r\n'
             'Host: {hostname}\r\n'
             'Connection: keep-alive\r\n'
             'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n'
             'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) '
             'Chrome/39.0.2171.95 Safari/537.36\r\n'
             'Referrer: {referrer}\r\n'
             # 'Accept-Encoding: gzip, deflate, sdch\r\n'
             'Accept-Language: en-US,en;q=0.8\r\n'
             '\r\n').format(path=url_obj.path or '/', hostname=url_obj.hostname, referrer=referrer)
    writer.write(query.encode('latin-1'))

    # Reads response
    p = HttpParser()
    body = []
    r = Response(url)
    while True:
        line = await reader.readline()
        if not line:
            break
        recved = len(line)
        nparsed = p.execute(line, recved)
        assert nparsed == recved
        if not r.has_status_code():
            status_code = int(line.decode('utf8').split(' ')[1])
            r.status_code = status_code
        if p.is_headers_complete() and not r.has_headers():
            r.headers = dict(p.get_headers())
            # Immediately follow redirection if allowed, don't wait for the body
            if r.status_code in (301, 302) and allow_redirects:
                writer.close()
                return await get(r.headers['Location'], allow_redirects=allow_redirects, referrer=url)
        if p.is_partial_body():
            body.append(p.recv_body())
        if p.is_message_complete():
            break
    r.content = b''.join(body)

    writer.close()
    if r.status_code == 0:
        raise SiteNotFound

    return r


async def test(url):

    print(url)
    r = await get2(url)
    print(r.status_code)
    # print(r.headers)


def main():

    # socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 9150, True)
    loop = asyncio.get_event_loop()
    # task = asyncio.ensure_future(test('http://oxwugzccvk3dk6tj.onion/'))
    tasks = [
        asyncio.ensure_future(test('http://www.github.com')),
        asyncio.ensure_future(test('http://oxwugzccvk3dk6tj.onion/')),
        asyncio.ensure_future(test('http://vichandcxw4gm3wy.onion/')),
        asyncio.ensure_future(test('http://gurochanocizhuhg.onion/')),
        asyncio.ensure_future(test('http://xiwayy2kn32bo3ko.onion/')),
        asyncio.ensure_future(test('http://redditor3a2spgd6.onion'))
    ]

    loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == '__main__':
    main()