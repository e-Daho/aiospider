import logging
import re
from typing import Iterable

from bs4 import BeautifulSoup
import asyncio
from typing import Dict
import aiohttp
import async_timeout
import aiosocks
from aiosocks.connector import SocksConnector
import aioredis
from concurrent.futures._base import TimeoutError
import motor.motor_asyncio

# TODO: redis cache

Url = str
Urls = Iterable(Url)


class SiteNotFound(Exception):
    """Exception raised when the given URL has not been found"""

    def __str__(self):
        return "Site not found"


class Response:
    """Class for manipulating server reponse"""

    def __init__(self, url: Url, status_code: int = 0, headers: Dict = None, content: bytes = ''):
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


class TorSpider:
    """Class to crawl Tor"""

    def __init__(self, n_tasks: int = 100):
        self.n_tasks = n_tasks
        self.external_links_queue = asyncio.Queue()
        self.internal_links_queue = asyncio.Queue()

        # TODO: improve this
        self.crawled_urls = set()
        client = motor.motor_asyncio.AsyncIOMotorClient()
        self.db = client['test']
        self.coll = self.db['crawls']
        self.bulk = []
        self.bulk_size = 10

    def __str__(self):
        return 'TorSpider object'

    def feed(self, urls: Urls):
        """Setup staring urls for crawling"""

        for link in urls:
            self.external_links_queue.put_nowait(link)

    @staticmethod
    def get_internal_links(bs_obj: BeautifulSoup, include_url: Url) -> Urls:
        """Finds all links that begin with a "/" or which contain the page url"""

        already_saw = {include_url}
        include_link = include_url.replace('http://', '').split('/')[0]
        for link in bs_obj.find_all("a", href=re.compile(r"^/(?!/)|.*" + re.escape(include_link))):
            if link.attrs['href'] is not None:
                ref = link.attrs['href']
                url = include_url + ref if ref.startswith('/') else ref
                url = url.strip('/')
                if url not in already_saw:
                    already_saw.add(url)
                    yield url

    @staticmethod
    def get_external_links(bs_obj: BeautifulSoup, exclude_url: Url) -> Urls:
        """Finds all links that start with "http" or "www" that do not contain the current URL"""

        already_saw = {exclude_url}
        exclude_domain = exclude_url.replace('http://', '').split('/')[0]
        for link in bs_obj.find_all("a", href=re.compile(r"^(?:http://|https://)(?!" + re.escape(exclude_domain) + ").*$")):
            if link.attrs['href'] is not None:
                link = link.attrs['href']
                if link not in already_saw and link.endswith('.onion'):
                    already_saw.add(link)
                    yield link

    @staticmethod
    async def get(session: aiohttp.ClientSession, url: Url) -> Response:

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
        except Exception as exc:
            logging.exception(exc)

    async def archive(self, r: Response):
        """Archives the given response in MongoDB"""

        self.bulk.append({'url': r.url, 'source': r.content})
        if len(self.bulk) == self.bulk_size:
            await self.coll.insert(self.bulk)
            count = await self.coll.count()
            logging.debug('Inserted {} new documents in database'.format(count))
            self.bulk = []

    async def crawl(self):
        """
        Get a new url from the Queue, crawls it, extracts external links, adds them to external links queue,
        get internal links, adds them to internal links queue, store de page in database. If external links queue
        is empty, crawls links from internal links queue until an external link is found
        """

        redis_pool = await aioredis.create_pool(('localhost', 6389), minsize=5, maxsize=10)
        conn = SocksConnector(proxy=aiosocks.Socks5Addr('127.0.0.1', 9150), proxy_auth=None, remote_resolve=True)
        async with redis_pool.get() as redis_cache:
            with aiohttp.ClientSession(connector=conn) as session:
                # while not (self.external_links_queue.empty() and self.internal_links_queue.empty()):
                while 1:
                    print(self.external_links_queue.qsize(), self.internal_links_queue.qsize())
                    if not self.external_links_queue.empty():
                        url = await self.external_links_queue.get()
                    else:
                        url = await self.internal_links_queue.get()
                    self.crawled_urls.add(url)
                    r = await self.get(session, url)
                    if r and r.has_content:
                        await self.archive(r)
                        bs_obj = BeautifulSoup(r.content, 'html.parser')
                        for new_url in self.get_internal_links(bs_obj, url):
                            if await redis_cache.sadd('to_crawl', new_url) and not \
                                    await redis_cache.sismember('crawled', new_url):
                                await self.internal_links_queue.put(new_url)
                        for new_url in self.get_external_links(bs_obj, url):
                            if await redis_cache.sadd('to_crawl', new_url) and not \
                                    await redis_cache.sismember('crawled', new_url):
                                await self.external_links_queue.put(new_url)
                    await redis_cache.sadd('crawled', url)
                    await redis_cache.sadd('sites', url.replace('http://', '').split('/')[0])
        await redis_pool.clear()

    def run(self):
        """Starts the spider"""

        loop = asyncio.get_event_loop()
        tasks = [asyncio.ensure_future(self.crawl()) for _ in range(4)]
        loop.run_until_complete(asyncio.gather(*tasks))


def main():
    asyncio.get_event_loop().set_debug(True)
    urls = [
        'http://oxwugzccvk3dk6tj.onion',
        'http://vichandcxw4gm3wy.onion',
        'http://xiwayy2kn32bo3ko.onion',
        'http://redditor3a2spgd6.onion'
    ]
    spider = TorSpider()
    spider.feed(urls)
    spider.run()


if __name__ == '__main__':
    main()
