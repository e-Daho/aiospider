import pytest
from bs4 import BeautifulSoup
import requests

from crawler import get_external_links, get_internal_links


def test_get_external_links():

    url = 'www.magiccorporation.com'
    r = requests.get('http://' + url, 'html.parser')
    links = [l for l in get_external_links(BeautifulSoup(r.text, 'html.parser'), url)]
    assert set(links) == {'magiccorporation.com', 'tcgdatabase.fr', 'finalyugi.com', 'cartabella.fr', 'pikalab.com',
                          'playfactory.fr', 'ultrajeux.com', 'designertoyz.com', 'kokeshi.fr', 'mibbit.com',
                          'mogg.fr', 'xiti.com'}


def test_get_internal_links():

    url = 'www.cybelangel.com'
    r = requests.get('http://' + url, 'html.parser')
    links = {link for link in get_internal_links(BeautifulSoup(r.text, 'html.parser'), url)}
    
    assert links == {'www.cybelangel.com/detection-of-counterfeiting',
                     'www.cybelangel.com/detection-of-counterfeiting/%20',
                     'www.cybelangel.com/detection-of-data-leaks-on-the-internet',
                     'www.cybelangel.com/event',
                     'www.cybelangel.com/form-demo',
                     'www.cybelangel.com/jobs',
                     'www.cybelangel.com/legal-notices',
                     'www.cybelangel.com/our-vision',
                     'www.cybelangel.com/press-room',
                     'www.cybelangel.com/privacy-cookie',
                     'www.cybelangel.com/privacy-cookie-policy',
                     'www.cybelangel.com?lang=fr',
                     'blog.cybelangel.com',
                     'www.cybelangel.com/#contact'}
