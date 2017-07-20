#! /usr/bin/env python
# coding=utf-8
#
# Copyright © 2017 Gael Lederrey <gael.lederrey@epfl.ch>
#
# Distributed under terms of the MIT license.

from classes.crawler import *
from classes.parser import *
import time
import datetime
import os


def run():

    # Create directory for the data
    os.makedirs('../data')

    print("Process starting at : {}".format(datetime.datetime.now()))

    start = time.time()

    # Initialize classes
    n_threads = 10
    crawler = Crawler(n_threads)
    parser = Parser(n_threads)

    # Crawl the styles pages
    print('Crawling the styles page...')
    crawler.crawl_styles_page()
    print('Getting the links from the styles page...')
    crawler.get_links_styles()
    print('Crawling the pages with all the beers for each style...')
    crawler.crawl_all_styles()

    # Parse the styles pages
    print('Parsing the Beers from the styles pages...')
    parser.parse_beers_from_styles()

    # Crawl all the breweries
    print('Crawling all the breweries...')
    crawler.crawl_all_breweries()

    # Parse all the beers from the breweries
    print('Parsing all the breweries...')
    parser.parse_breweries()

    # Crawl all the ratings
    print('Crawling all the ratings...')
    crawler.crawl_all_beers()

    stop = time.time()

    elapsed = str(datetime.timedelta(seconds=stop-start))

    print('Time to complete the crawling: {}'.format(elapsed))

if __name__ == "__main__":
    run()
