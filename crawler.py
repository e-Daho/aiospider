"""
PyCrawler
A python crawler base on asyncio and mongodb.

@author Thomas Perrot

inspired by:
* Web Scraping with Python Collecting Data from the Modern Web, Ryan Mitchell
* Paul P.
"""

import re
from typing import Iterable
import logging

from bs4 import BeautifulSoup
from tld import get_tld
import aiohttp
import asyncio
import pymongo
from pymongo.errors import BulkWriteError


BATCH_SIZE = 5

root_url = 'www.cybelangel.com'
crawled_urls = set()
url_hub = [root_url, "{}/sitemap.xml".format(root_url), "{}/robots.txt".format(root_url)]

db = pymongo.MongoClient().get_database('test')
coll = db.get_collection('crawls')


async def get_body(url: str) -> str:
    """Returns the html from the given url"""

    try:
        with aiohttp.ClientSession() as session, aiohttp.Timeout(5):
            async with session.get('http://' + url) as response:
                return await response.text()
    except asyncio.TimeoutError as tme:
        logging.warning('Timeout exception for {}\n{}'.format(url, tme))
        return None
    except aiohttp.errors.ClientOSError as coe:
        logging.warning('ClientOSError exception for {}\n{}'.format(url, coe))
        return None


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
                logging.warning('The internal link {} does not start with "http" or "/"'.format(ref))
                continue
            url = url.strip('/')
            if url not in already_saw:
                already_saw.add(url)
                yield url


def get_external_links(bs_obj: BeautifulSoup, exclude_url: str) -> Iterable[str]:
    """Finds all links that start with "http" or "www" that do not contain the current URL"""

    already_saw = {exclude_url}
    exclude_tld = get_tld('http://' + exclude_url)
    for link in bs_obj.find_all("a", href=re.compile(r"^(http|https|www)(?!"+exclude_tld+").*$")):
        if link.attrs['href'] is not None:
            tld = get_tld(link.attrs['href'])
            if tld not in already_saw:
                already_saw.add(tld)
                yield tld


def insert_bulk(bulk: dict):
    """Insert the crawled pages in Mongodb"""

    try:
        coll.insert_many(bulk)
    except BulkWriteError as bwe:
        logging.warning('Error in bulk insertion:\n' +
                        '\n'.join([str(err['errmsg']) for err in bwe.details.get('writeErrors')]))

    logging.debug('Bulk emptied')


async def handle_task(work_queue):

    bulk = []
    while not work_queue.empty():
        queue_url = await work_queue.get()
        crawled_urls.add(queue_url)
        body = await get_body(queue_url)
        if body:
            bulk.append({'_id': queue_url, 'source': body})
            if len(bulk) >= BATCH_SIZE:
                await asyncio.get_event_loop().run_in_executor(None, insert_bulk, bulk)
                bulk = []
            bs_obj = BeautifulSoup(body, 'html.parser')
            for new_url in get_external_links(bs_obj, queue_url):
                if new_url not in crawled_urls:
                    work_queue.put_nowait(new_url)
                    print(new_url)


def main():

    q = asyncio.Queue()
    [q.put_nowait(url) for url in url_hub]
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    tasks = [handle_task(q) for _ in range(3)]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.WARNING)
    main()
