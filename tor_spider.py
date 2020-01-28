import logging
import re
from typing import Dict, Iterable
import textwrap

from bs4 import BeautifulSoup
import asyncio
import aiohttp
import async_timeout
import aiosocks
from aiosocks.connector import SocksConnector
import redis
import aioredis
import tld
from concurrent.futures._base import TimeoutError
import motor.motor_asyncio

Url = str  # e.g: http://www.google.com/hello.php
Urls = Iterable(Url)
Raw_Url = str # e.g www.google.com/hello.php
Domain = str  # e.g www.google.com


class SiteNotFound(Exception):
    """Exception raised when the given URL has not been found"""

    def __str__(self):
        return "Site not found"


class Model:
    """Class to manage database classes"""

    def __init__(self):
        self.sites = {}

    def __repr__(self):
        return "Model object"

    def add_site(self, site: 'Site'):
        self.sites[site.name] = site

    def site(self, domain: Domain):
        return self.sites[domain]


class Site:
    """Class which represents a website"""

    def __init__(self, name: str):
        self.name = name
        self.pages = []

    def __repr__(self):
        return "Site object: " + self.name

    @property
    def tld(self) -> Domain:
        if self.name.endswith('.onion'):
            return '.'.join(self.name.split('.')[-2:])
        try:
            return tld.get_tld(self.name)
        except tld.TldBadUrl as err:
            logging.error(err)
        except tld.TldDomainNotFound as err:
            logging.error(err)

    @property
    def n_pages(self) -> int:
        return len(self.pages)

    @property
    def external_links(self) -> Urls:
        return list(set(link for page in self.pages for link in page.external_links))

    @property
    def internal_links(self) -> Urls:
        return list(set(link for page in self.pages for link in page.internal_links))

    @property
    def max_deepness(self) -> int:
        return max(page.deepness for page in self.pages)

    def add_page(self, page: 'Page'):
        self.pages.append(page)

    def to_dict(self):
        return {'name': self.name, 'external_links': self.external_links}


class Page:
    """Class which represents a web page"""

    def __init__(self, model: Model, url: Url, response: 'Response'):
        self.url = url
        self._response = response
        self._soup = None
        self._model = model
        if self.domain not in model.sites:
            model.add_site(Site(self.domain))
        self.site = model.site(self.domain)
        self.site.add_page(self)

    def __repr__(self):
        return "Page object: " + self.url

    @property
    def status(self) -> int:
        return self._response.status_code

    @property
    def domain(self) -> Domain:
        return self.raw_url.split('/')[0]

    @property
    def raw_url(self) -> Raw_Url:
        return self.url.replace(self.scheme, '')

    @property
    def soup(self) -> BeautifulSoup:
        if not self._soup:
            self._soup = BeautifulSoup(self._response.text, 'html.parser')
        return self._soup

    @property
    def title(self) -> str:
        return self.soup.title.string

    @property
    def is_empty(self) -> bool:
        return self._response.has_content()

    @property
    def body(self) -> str:
        return str(self.soup.body)

    @property
    def text(self) -> str:
        return self._response.text

    @property
    def deepness(self) -> int:
        """Returns the deepness of the page in the web site"""
        return len(self.raw_url.split('/')) - 1

    @property
    def scheme(self) -> str:
        if self.url.startswith('http://'):
            return 'http://'
        elif self.url.startswith('https://'):
            return 'https://'
        else:
            raise ValueError('Unknow protocol for page {}'.format(self.url))

    @property
    def external_links(self) -> Urls:
        """Finds all links that start with "http" or "https" that do not contain the page domain"""

        external_urls = set()
        for link in self.soup.find_all(
                "a", href=re.compile(r"^(?:http://|https://)(?!" + re.escape(self.domain) + ").*$")):
            if link.attrs['href'] is not None:
                link = link.attrs['href']
                if link not in external_urls:
                    external_urls.add(link)
        return list(external_urls)

    @property
    def parent_urls(self) -> Urls:
        """e.g ['www.google.com', 'www.google.com/bonjour', 'www.google.com/bonjour/hello'] for
        www.google.com/bonjour/hello/lol"""

        parts = self.raw_url.split('/')
        return list('/'.join(parts[:i]) for i in range(1, len(parts)))

    @property
    def internal_links(self) -> Urls:
        """Finds all urls that begin with a "/" or which contain the page url"""

        internal_links = set()
        for link in self.soup.find_all("a", href=re.compile(r"^/(?!/)|.*" + re.escape(self.domain))):
            if link.attrs['href'] is not None:
                ref = link.attrs['href']
                url = self.scheme + self.domain + ref if ref.startswith('/') else ref
                url = url.strip('/')
                if url not in internal_links:
                    internal_links.add(url)
        return list(internal_links)

    @property
    def iter_forms(self) -> Iterable['Form']:
        # TODO: implement me
        pass

    def to_dict(self) -> Dict:
        return {'url': self.url, 'text': str(self.soup), 'site': self.domain, 'external_links': self.external_links,
                'internal_links': self.internal_links}


class Form:
    """Interface which represents a web form"""

    def __init__(self, method: str, **kwargs):
        pass


class Response:
    """Class for manipulating server response"""

    def __init__(self, status_code: int, headers: Dict, text: str = ''):
        self.status_code = status_code
        self.headers = headers
        self.text = text

    def __str__(self):
        return '<Response [{}]>'.format(self.status_code)

    def has_headers(self):
        return bool(self.headers)

    def has_status_code(self):
        return bool(self.status_code)

    def has_content(self):
        return bool(self.text)


class TorSpider:
    """Class to crawl Tor"""

    def __init__(self, n_tasks: int = 100):
        self.n_tasks = n_tasks
        self.external_links_queue = asyncio.Queue()
        self.internal_links_queue = asyncio.Queue()

        self.model = Model()
        self.coll = motor.motor_asyncio.AsyncIOMotorClient()['test']['crawls']
        self.bulk = []
        self.bulk_size = 10

        self.r_pool = None

    def __str__(self):
        return 'TorSpider object'

    def feed(self, urls: Urls):
        """Setup staring urls for crawling"""

        for link in urls:
            self.external_links_queue.put_nowait(link)

    @staticmethod
    def url_to_domain(url: Url) -> Domain:
        return url.replace('http://', '').replace('https://', '').split('/')[0]

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

        with async_timeout.timeout(20):
            async with session.get(url, headers=headers) as resp:
                r = Response(resp.status, resp.headers)
                r.text = await resp.text()
                return r

    async def archive_page(self, page: Page):
        """Archives the given page in MongoDB"""

        self.bulk.append(page.to_dict())
        if len(self.bulk) == self.bulk_size:
            await self.coll.insert(self.bulk)
            count = await self.coll.count()
            logging.debug('Inserted {} new documents in database'.format(count))
            self.bulk = []

    # async def explore(self, url: Url, max_depth: int=0):
    #     """Explore a given url"""
    #
    #     if max_depth < 0:
    #         raise ValueError('max_depth attribute must be positive')
    #
    #     # TODO: implement me
    #     if not self.r_pool:
    #         self.r_pool = await aioredis.create_pool(('localhost', 6389), minsize=5, maxsize=100)
    #     if url.endswith('.onion'):
    #         conn = SocksConnector(proxy=aiosocks.Socks5Addr('127.0.0.1', 9150), proxy_auth=None, remote_resolve=True)
    #     else:
    #         conn = None
    #     async with self.r_pool.get() as redis_cache:
    #         async with aiohttp.ClientSession(connector=conn) as session:
    #             while depth < max_depth or max_depth == 0:
    #                 try:
    #                     response = await self.get(session, url)
    #                 except Exception as exc:
    #                     await redis_cache.sadd('error', url)
    #                     response = None
    #                 if response:
    #                     page = Page(self.model, url, response)
    #                     if len(page.site.internal_links) / (len(page.site.external_links) + 1) >= 200:
    #                         continue
    #                     for new_url in page.internal_links + page.parent_urls:
    #                         if await redis_cache.sadd('to_crawl', new_url) and not \
    #                                 await redis_cache.sismember('crawled', new_url):
    #                             await self.internal_links_queue.put(new_url)
    #                     for new_url in page.external_links:
    #                         if await redis_cache.sadd('to_crawl', new_url) and not \
    #                                 await redis_cache.sismember('crawled', new_url):
    #                             await self.external_links_queue.put(new_url)
    #                     await self.archive_page(page)
    #                 await redis_cache.sadd('crawled', url)

    async def crawl(self, tor_only=False):
        """
        Get a new url from the Queue, crawls it, extracts external links, adds them to external links queue,
        get internal links, adds them to internal links queue, store de page in database. If external links queue
        is empty, crawls links from internal links queue until an external link is found
        """

        if not self.r_pool:
            self.r_pool = await aioredis.create_pool(('localhost', 6389), minsize=5, maxsize=100)
        conn = SocksConnector(proxy=aiosocks.Socks5Addr('127.0.0.1', 9150), proxy_auth=None, remote_resolve=True)
        async with self.r_pool.get() as redis_cache:
            async with aiohttp.ClientSession() as std_session, aiohttp.ClientSession(connector=conn) as tor_session:
                # while not (self.external_links_queue.empty() and self.internal_links_queue.empty()):
                while 1:
                    if not self.external_links_queue.empty():
                        url = await self.external_links_queue.get()
                    else:
                        url = await self.internal_links_queue.get()
                    if tor_only and not self.url_to_domain(url).endswith('.onion'):
                        continue
                    print(self.external_links_queue.qsize(), self.internal_links_queue.qsize(), url[:60])
                    await redis_cache.srem('to_crawl', url)
                    session = tor_session if self.url_to_domain(url).endswith('.onion') else std_session
                    try:
                        response = await self.get(session, url)
                    except Exception as exc:
                        await redis_cache.sadd('error', url)
                        response = None
                    if response:
                        page = Page(self.model, url, response)
                        if len(page.site.internal_links) / (len(page.site.external_links) + 1) >= 200:
                            continue
                        for new_url in page.internal_links + page.parent_urls:
                            if await redis_cache.sadd('to_crawl', new_url) and not \
                                    await redis_cache.sismember('crawled', new_url):
                                await self.internal_links_queue.put(new_url)
                        for new_url in page.external_links:
                            if await redis_cache.sadd('to_crawl', new_url) and not \
                                    await redis_cache.sismember('crawled', new_url):
                                await self.external_links_queue.put(new_url)
                        await self.archive_page(page)
                    await redis_cache.sadd('crawled', url)
        await self.r_pool.clear()

    def run(self, n_tasks: int=20, tor_only=False):
        """Starts the spider"""

        loop = asyncio.get_event_loop()
        tasks = [asyncio.ensure_future(self.crawl(tor_only=tor_only)) for _ in range(n_tasks)]
        loop.run_until_complete(asyncio.gather(*tasks))


def main():
    n_tasks = 1000
    # asyncio.get_event_loop().set_debug(True)
    r_cache = redis.StrictRedis(port=6389)
    urls = []
    for _ in range(n_tasks):
        url = r_cache.spop('to_crawl')
        if url:
            urls.append(url.decode('utf8'))
    if not urls:
        with open('tor_websites.txt') as file:
            urls = [line.split(' ')[0] for line in file if line.startswith('http://')]
    spider = TorSpider()
    spider.feed(urls)
    spider.run(n_tasks=n_tasks, tor_only=True)

    # http://underdj5ziov3ic7.onion/help/crawler/index.php/crawler/index.php/crawler/index.php/crawler/index.php/crawler/index.php/page/login/crawler/index.php/crawler/index.php/crawler/index.php/category/BOOKS/category/ITALIAN/crawler/index.php/crawler/index.php/crawler/index.php/help/category/SOCIAL/link/AcmjRPOqapvliz3krwh/link/lnxAly6G9FMYU0vUPsRo/link/AcmjRPOqapvliz3krwh/link/lnxAly6G9FMYU0vUPsRo/link/XoI3teoUrX5UKtsDrjPu/category/SOCIAL/pg/3/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/help/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/ijGwHWMYxEAX4KccxzA/link/uzuhvqOpVbAuzPRqzaUZ/category/INTRODUCTION_POINTS/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/category/GAMBLING_OTHER/category/ADULT/link/uzuhvqOpVbAuzPRqzaUZ/category/FORUMS/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/category/EROTICA/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/category/JAPANESE/link/uzuhvqOpVbAuzPRqzaUZ/category/OTHER_LANGUAGES/link/ijGwHWMYxEAX4KccxzA/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/link/uzuhvqOpVbAuzPRqzaUZ/category/GAMBLING_OTHER
    # http://underdj5ziov3ic7.onion/crawler/index.php

if __name__ == '__main__':
    main()
