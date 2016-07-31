"""
Author Thomas Perrot

inspired by:
* Web Scraping with Python Collecting Data from the Modern Web, Ryan Mitchell
"""


import re
from typing import Iterable

from tld import get_tld
from bs4 import BeautifulSoup
from tld import get_tld
import aiohttp
import asyncio


root_url = "https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Accueil_principal"
crawled_urls, url_hub = set(), [root_url, "{}/sitemap.xml".format(root_url), "{}/robots.txt".format(root_url)]


async def get_body(url: str) -> str:
    """Returns the html from the given url"""

    with aiohttp.ClientSession() as session, aiohttp.Timeout(10):
        async with session.get(url) as response:
            #if response.status == 200
            return await response.text()


def get_internal_links(bs_obj: BeautifulSoup, include_url: str) -> Iterable[str]:
    """Finds all links that begin with a "/" or which contain the page url"""

    already_saw = {include_url}
    for link in bs_obj.find_all("a", href=re.compile("^(/(?!/)|.*" + include_url + ")")):
        if link.attrs['href'] is not None:
            ref = link.attrs['href']
            if ref.startswith('http'):
                url = ref.split('//')[1]
            elif ref.startswith('/'):
                url = include_url + ref
            else:
                raise ValueError('The internal link {} does not start with "http" of "/"'.format(ref))
            url = url.strip('/')
            if url not in already_saw:
                already_saw.add(url)
                yield url


def get_external_links(bs_obj: BeautifulSoup, exclude_url: str) -> Iterable[str]:
    """Finds all links that start with "http" or "www" that do not contain the current URL"""

    already_saw = {exclude_url}
    exclude_tld = get_tld('http://' + exclude_url)
    for link in bs_obj.find_all("a", href=re.compile("^(http|https|www)((?!"+exclude_tld+").)*$")):
        if link.attrs['href'] is not None:
            tld = get_tld(link.attrs['href'])
            if tld not in already_saw:
                already_saw.add(tld)
                yield tld


async def handle_task(task_id, work_queue):
    while not work_queue.empty():
        queue_url = await work_queue.get()
        crawled_urls.add(get_tld(queue_url))
        body = await get_body(queue_url)
        bs_obj = BeautifulSoup(body, 'html.parser')
        for new_url in get_external_links(bs_obj, queue_url):
            if get_tld(new_url) not in crawled_urls:
                work_queue.put_nowait(new_url)
        # await asyncio.sleep(5)


def main():

    q = asyncio.Queue()
    [q.put_nowait(url) for url in url_hub]
    loop = asyncio.get_event_loop()
    tasks = [handle_task(task_id, q) for task_id in range(3)]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

def test_get_internal_links():
    import requests
    url = 'www.cybelangel.com'
    r = requests.get('http://' + url)
    for link in get_internal_links(BeautifulSoup(r.text, 'html.parser'), url):
        print(link)


if __name__ == '__main__':
    # main()
    test_get_internal_links()
