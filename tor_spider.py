import logging
import re
from typing import Iterable

from tld import get_tld
from bs4 import BeautifulSoup
import asyncio
from typing import Dict
import aiohttp
import async_timeout
import aiosocks
from aiosocks.connector import SocksConnector
from concurrent.futures._base import TimeoutError
import motor.motor_asyncio

# TODO: one queue for internal links, one queue for external links, redis cache

crawled_urls = set()
client = motor.motor_asyncio.AsyncIOMotorClient()
db = client['test']
coll = db['crawls']


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

    def has_content(self):
        return bool(self.content)


def get_internal_links(bs_obj: BeautifulSoup, include_url: str) -> Iterable[str]:
    """Finds all links that begin with a "/" or which contain the page url"""

    already_saw = {include_url}
    for link in bs_obj.find_all("a", href=re.compile(r"^(/(?!/)|.*" + get_tld('http://' + include_url) + ")")):
        if link.attrs['href'] is not None:
            ref = link.attrs['href']
            if ref.startswith('http'):
                url = ref.split('//')[1]
            elif ref.startswith('/'):
                url = include_url + ref
            else:
                logging.info('The internal link {} does not start with "http" or "/"'.format(ref))
                continue
            url = url.strip('/')
            if url not in already_saw:
                already_saw.add(url)
                yield url


def get_external_links(bs_obj: BeautifulSoup, exclude_url: str) -> Iterable[str]:
    """Finds all links that start with "http" or "www" that do not contain the current URL"""

    already_saw = {exclude_url}
    exclude_domain = exclude_url.replace('http://', '')
    for link in bs_obj.find_all("a", href=re.compile(r"^(http|https|www)(?!"+exclude_domain+").*$")):
        if link.attrs['href'] is not None:
            link = link.attrs['href']
            if link not in already_saw and link.endswith('.onion'):
                already_saw.add(link)
                yield link


async def get(session: aiohttp.ClientSession, url: str):

    headers = {
        'Connection': 'keep-alive',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'Referrer': 'https:/www.google.com/',
        'Accept-Language': 'en-US,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
    }

    try:
            with async_timeout.timeout(20):
                async with session.get(url, headers=headers) as resp:
                    r = Response(url, status_code=resp.status, headers=resp.headers)
                    r.content = await resp.read()
                    return r
    except TimeoutError:
        return None
    except aiohttp.ProxyConnectionError as err:
        logging.exception(err)
    except aiosocks.SocksError as err:
        logging.exception(err)


async def crawl(queue):
    """Get a new url from the Queue, crawls it, store page in Mongodb, extracts external links, adds them to Queue"""

    bulk = []
    conn = SocksConnector(proxy=aiosocks.Socks5Addr('127.0.0.1', 9150), proxy_auth=None, remote_resolve=True)
    with aiohttp.ClientSession(connector=conn) as session:
        while not queue.empty():
            url = await queue.get()
            print(url)
            crawled_urls.add(url)
            r = await get(session, url)
            if r and r.has_content:
                bulk.append({'_id': url, 'source': r.content})
                if len(bulk) >= 1:
                    await coll.insert(bulk)
                    count = await db.test.count()
                    logging.debug('Inserted {} new documents in database'.format(count))
                    bulk = []
                bs_obj = BeautifulSoup(r.content, 'html.parser')
                for new_url in get_external_links(bs_obj, url):
                    if new_url not in crawled_urls:
                        queue.put_nowait(new_url)


def main():

    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()
    urls = [
        'http://www.github.com',
        'http://oxwugzccvk3dk6tj.onion/',
        'http://vichandcxw4gm3wy.onion/',
        'http://gurochanocizhuhg.onion/',
        'http://xiwayy2kn32bo3ko.onion/',
        'http://redditor3a2spgd6.onion'
    ]
    for url in urls:
        queue.put_nowait(url)
    tasks = [asyncio.ensure_future(crawl(queue)) for _ in range(100)]

    loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == '__main__':
    main()