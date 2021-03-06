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
    data_folder = '../data/'
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    print("Process starting at : {}".format(datetime.datetime.now()))

    start = time.time()

    # Initialize classes
    delta_t = 0.2
    crawler = Crawler(delta_t, data_folder)
    parser = Parser(data_folder)

    print('1. Crawling the places...')
    #crawler.crawl_all_places()

    print('2. Crawling the breweries from the places...')
    #crawler.crawl_breweries_from_places()

    print('3. Parsing the breweries from the places...')
    #parser.parse_breweries_from_places()

    print('4. Crawling the remaining pages from the breweries...')
    #crawler.crawl_all_breweries()

    print('5. Crawling the closed breweries...')
    #crawler.crawl_all_closed_breweries()

    print('6. Parsing the missing breweries...')
    #parser.parse_missing_breweries()

    print('7. Parsing the breweries files to get the number of beers...')
    #parser.parse_breweries_files_for_number()

    print('8. Parsing the breweries files to get the beers...')
    #parser.parse_breweries_files_for_beers()

    print('9. Crawling all the beers and their reviews...')
    #crawler.crawl_all_beers_and_reviews()

    print('10. Parsing all the beer files to update the beers.csv file...')
    #parser.parse_beer_files_for_information()

    print('11. Parsing all the beer files to get the reviews...')
    #parser.parse_beer_files_for_reviews()

    print('12. Getting the users from the ratings...')
    #parser.get_users_from_ratings()

    print('13. Crawling all the users...')
    #crawler.crawl_all_users()

    print('14. Parsing the users for some information...')
    parser.parse_all_users()

    print('15. Crawling user with the cookies... (On personal computer)')
    #crawler.crawl_users_with_cookies()

    print('16. Parsing the users that have been crawler with cookies...')
    parser.parse_users_crawler_with_cookies()

    stop = time.time()

    elapsed = str(datetime.timedelta(seconds=stop-start))

    print('Time to complete the crawling: {}'.format(elapsed))

if __name__ == "__main__":
    run()
