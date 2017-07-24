#! /usr/bin/env python
# coding=utf-8
#
# Copyright © 2017 Gael Lederrey <gael.lederrey@epfl.ch>
#
# Distributed under terms of the MIT license.

from classes.helpers import round_
import pandas as pd
import numpy as np
import requests
import time
import re
import os


class Crawler:
    """
    Crawler for BeerAdvocate website
    """

    def __init__(self, delta_t, data_folder=None):
        """
        Initialize the class.
        
        :param delta_t: Average time in seconds between two requests
        :param data_folder: Folder to save the data
        """

        if data_folder is None:
            self.data_folder = '../data/'
        else:
            self.data_folder = data_folder

        self.delta_t = delta_t

        self.special_places = ['Canada', 'United States', 'United Kingdom']

    ########################################################################################
    ##                                                                                    ##
    ##                               Crawl the places                                     ##
    ##                                                                                    ##
    ########################################################################################

    def crawl_all_places(self):
        """
        STEP 1

        Crawl all the places
        """

        url_countries = 'https://www.beeradvocate.com/place/directory/?show=all'

        # Create folder for all the HTML pages
        folder = self.data_folder + 'misc/'
        if not os.path.exists(folder):
            os.makedirs(folder)

        # Crawl the countries
        r = self.request_and_wait(url_countries)
        with open(folder + 'countries.html', 'wb') as output:
            output.write(r.content)

        # Create folder for all the places
        folder = self.data_folder + 'places/'
        if not os.path.exists(folder):
            os.makedirs(folder)

        # Parse the countries
        html = open(self.data_folder + 'misc/countries.html', 'rb').read().decode('utf8')

        str_ = '<a href="/place/directory/0/(.+?)/">(.+?)</a>'

        grp = re.finditer(str_, str(html))

        for g in grp:
            self.crawl_one_place(g.group(1), g.group(2))

    def crawl_one_place(self, grp1, grp2):
        """
        USED BY STEP 1

        Crawl one place and save it
        :param grp1: Results of REGEX 1 (Country Code)
        :param grp2: Results of REGEX 2 (Country name)
        """

        folder = self.data_folder + 'places/'

        # Get the country name
        country = grp2.replace('<b>', '').replace('</b>', '')
        nbr = country[::-1].find(' ') + 1
        country = country[:-nbr]

        # Check if it's special or not
        if country not in self.special_places:
            # Download the page with the number of breweries
            url = 'https://www.beeradvocate.com/place/directory/0/{}/'.format(grp1)
            r = self.request_and_wait(url)
            # Get the number of breweries
            str_ = 'Brewery \((\d+)\)'
            test = re.search(str_, str(r.content))
            # Check if it's more than 0
            if int(test.group(1)) > 0:
                # Save the first page in this case
                url = 'https://www.beeradvocate.com/place/list/?start=0&c_id={}&brewery=Y&sort=name'.format(grp1)
                r = self.request_and_wait(url)

                if not os.path.exists(folder + country):
                    os.makedirs(folder + country)

                with open(folder + country + '/0.html', 'wb') as output:
                    output.write(r.content)
        else:
            # Download the page with all the regions
            url = 'https://www.beeradvocate.com/place/directory/0/{}/'.format(grp1)
            r = self.request_and_wait(url)
            html_spec = r.content
            # Get all the regions
            str_spec = '<a href="/place/directory/0/{}/(.+?)/">(.+?)</a>'.format(grp1)
            grp_spec = re.finditer(str_spec, str(html_spec))
            for g_spec in grp_spec:
                if '#' not in g_spec.group(1):
                    # Get the name of the region
                    place = g_spec.group(2).replace('<b>', '').replace('</b>', '')
                    nbr = place[::-1].find(' ') + 1
                    place = place[:-nbr]
                    # Download the page with the number of breweries
                    url = 'https://www.beeradvocate.com/place/directory/0/{}/{}/'.format(grp1, g_spec.group(1))
                    r = self.request_and_wait(url)

                    # Get the number of breweries
                    str_ = 'Brewery \((\d+)\)'
                    test = re.search(str_, str(r.content))
                    # Check if it's more than 0
                    if int(test.group(1)) > 0:
                        # Save the first page in this case
                        url = 'https://www.beeradvocate.com/place/list/?start=0&c_id={}&s_id={}&brewery=Y&sort=name'.format(
                            grp1, g_spec.group(1))
                        r = self.request_and_wait(url)
                        name = country + '/' + place

                        if not os.path.exists(folder + name):
                            os.makedirs(folder + name)
                        with open(folder + name + '/0.html', 'wb') as output:
                            output.write(r.content)

    ########################################################################################
    ##                                                                                    ##
    ##               Crawl the pages with the breweries from the places                   ##
    ##                                                                                    ##
    ########################################################################################

    def crawl_breweries_from_places(self):
        """
        STEP 2

        Crawl the missing pages with the breweries from the different places
        """

        folder = self.data_folder + 'places/'

        list_ = os.listdir(folder)

        for place in list_:
            self.crawl_breweries_from_one_place(place)

    def crawl_breweries_from_one_place(self, dir_):
        """
        USED BY STEP 2

        Crawl the missing pages with the breweries from a place

        :param dir_: Name of the folder
        """
        step = 20

        folder = self.data_folder + 'places/'

        if dir_ not in self.special_places:
            html = open(folder + dir_ + '/0.html', 'rb').read().decode('utf8')

            # Get the code from the country
            str_ = 'c_id=(.+?)&'
            grp = re.search(str_, str(html))
            code = grp.group(1)

            # Get the number of breweries
            str_ = '\(out of (\d+)\)'
            grp = re.search(str_, str(html))
            nbr = round_(int(grp.group(1)) - 1, step)

            # Download the remaining breweries
            for i in range(1, int(nbr / step) + 1):
                start = i * step
                url = 'https://www.beeradvocate.com/place/list/?start={:d}&c_id={}&brewery=Y&sort=name'.format(start,
                                                                                                               code)
                r = self.request_and_wait(url)

                # Save it
                with open(folder + dir_ + '/{:d}.html'.format(start), 'wb') as output:
                    output.write(r.content)
        else:
            list_2 = os.listdir(folder + dir_)
            for dir_2 in list_2:
                html = open(folder + dir_ + '/' + dir_2 + '/0.html', 'rb').read().decode('utf8')

                # Get the code from the country
                str_ = 'c_id=(.+?)&'
                grp = re.search(str_, str(html))
                code = grp.group(1)

                # Get the code from the region
                str_ = 's_id=(.+?)&'
                grp = re.search(str_, str(html))
                code_region = grp.group(1)

                # Get the number of breweries
                str_ = '\(out of (\d+)\)'
                grp = re.search(str_, str(html))
                nbr = round_(int(grp.group(1)) - 1, step)

                # Download the remaining breweries
                for i in range(1, int(nbr / step) + 1):
                    start = i * step
                    url = 'https://www.beeradvocate.com/place/list/?start={:d}&c_id={}&s_id={}&brewery=Y&sort=name'.format(
                        start, code, code_region)
                    r = self.request_and_wait(url)

                    # Save it
                    with open(folder + dir_ + '/' + dir_2 + '/{:d}.html'.format(start), 'wb') as output:
                        output.write(r.content)

    ########################################################################################
    ##                                                                                    ##
    ##                       Crawl the pages for all the breweries                        ##
    ##                                                                                    ##
    ########################################################################################

    def crawl_all_breweries(self):
        """
        STEP 4

        Crawl all the breweries pages

        !!! Make sure step 3 was done with the parser !!!
        """

        df = pd.read_csv(self.data_folder + 'parsed/breweries.csv')

        folder = self.data_folder + 'breweries/'
        # Create folder for all the HTML pages
        if not os.path.exists(folder):
            os.makedirs(folder)

        for i in df.index:
            row = df.ix[i]
            self.crawl_one_brewery(row['id'])

    def crawl_one_brewery(self, id_):
        """
        USED BY STEP 4

        Crawl the HTML page of one brewery
        :param id_: ID of the brewery
        """

        folder = self.data_folder + 'breweries/'

        # Check if file already exists
        if not os.path.exists(folder + str(id_) + '.html'):

            # Get the HTML page
            url = 'https://www.beeradvocate.com/beer/profile/{:d}/?view=beers&show=all'.format(id_)
            r = self.request_and_wait(url)

            # Save it
            with open(folder + str(id_) + '.html', 'wb') as output:
                output.write(r.content)

    ########################################################################################
    ##                                                                                    ##
    ##                  Crawl the pages for all the missing breweries                     ##
    ##                                                                                    ##
    ########################################################################################

    def crawl_all_closed_breweries(self):
        """
        STEP 5

        Crawl the closed breweries (Not in the breweries.csv file)
        """

        df = pd.read_csv(self.data_folder + 'parsed/breweries.csv')

        df = df.sort_values(by='id')

        # Last id
        last = df['id'].ix[df.index[-1]]

        # All ids
        all_ids = list(range(1, last + 1))

        # The ids we already have
        got = list(df['id'])

        # The missing ones
        missing = list(set(all_ids) - set(got))

        for i in missing:
            self.crawl_one_closed_brewery(i)

    def crawl_one_closed_brewery(self, id_):
        """
        USED BY STEP 5

        Crawl the page of the brewery if it's one

        :param id_: ID for the place
        """

        folder = self.data_folder + 'breweries/'

        # Check if file already exists
        if not os.path.exists(folder + str(id_) + '.html'):
            url = 'https://www.beeradvocate.com/beer/profile/{:d}/?view=beers&show=all'.format(id_)

            r = self.request_and_wait(url)

            html = r.content

            # Search if it's a brewery
            str_ = '<b>Type:</b> (.+?)\\\\n\\\\t\\\\t<br>'
            grp = re.search(str_, str(html))
            types = grp.group(1).split(', ')

            if 'Brewery' in types:
                with open(folder + str(id_) + '.html', 'wb') as output:
                    output.write(r.content)

    ########################################################################################
    ##                                                                                    ##
    ##                              Crawl all the beers                                   ##
    ##                                                                                    ##
    ########################################################################################

    def crawl_all_beers_and_reviews(self):
        """
        STEP 9

        Crawl all the reviews from all the beers.

        !!! Make sure steps 6, 7 and 8 were done with the parser !!!
        """

        df = pd.read_csv(self.data_folder + 'parsed/beers.csv')

        folder = self.data_folder + 'beers/'
        # Create folder for all the HTML pages
        if not os.path.exists(folder):
            os.makedirs(folder)

        for i in df.index:
            row = df.ix[i]
            self.crawl_one_beer(row['brewery_id'], row['beer_id'])

    def crawl_one_beer(self, brewery_id, beer_id):
        """
        USED BY STEP 9

        Crawl all the reviews from one beer

        :param brewery_id: ID of the brewery
        :param beer_id: ID of the beer
        """

        step = 25

        try:
            # Create the folder
            folder = self.data_folder + 'beers/{:d}/{:d}/'.format(brewery_id, beer_id)
            if not os.path.exists(folder):
                os.makedirs(folder)

            if not os.path.exists(folder + '0.html'):

                # First URL
                url = 'https://www.beeradvocate.com/beer/profile/{:d}/{:d}'.format(brewery_id, beer_id)

                # Get it and write it
                r = self.request_and_wait(url)
                with open(folder + '0.html', 'wb') as output:
                    output.write(r.content)

                html = r.content
            else:
                html = open(folder + '0.html', 'rb').read().decode('utf8')

            # Parse it to get the number of Ratings
            str_ = '</i> Ratings: (.+?)</b>'
            grp = re.search(str_, str(html))

            if grp is not None:
                nbr = round_(int(grp.group(1).replace(',', '')) - 1, step)

                # Get all the pages with the reviews and ratings
                for i in range(1, int(nbr / step) + 1):
                    tmp = i * step

                    if not os.path.exists(folder + str(tmp) + '.html'):
                        url_tmp = url + '/?view=beer&sort=&start=' + str(tmp)

                        r = self.request_and_wait(url_tmp)

                        with open(folder + str(tmp) + '.html', 'wb') as output:
                            output.write(r.content)
        except Exception as e:
            print('---------------------------------------------------------------------')
            print('')
            print('ERROR WITH BREWERY_ID {} AND BEER_ID {}'.format(brewery_id, beer_id))
            print(e)
            print('---------------------------------------------------------------------')
            print('')

    ########################################################################################
    ##                                                                                    ##
    ##                                Other functions                                     ##
    ##                                                                                    ##
    ########################################################################################

    def request_and_wait(self, url):
        """
        Run the function get from the package requests, then wait a certain amount of time.

        :param url: url for the requests
        :return r: the request
        """

        # Get the time we want to wait before running the function again
        delta = np.abs(np.random.normal(self.delta_t, self.delta_t/2))

        start = time.time()

        # Run the function
        r = requests.get(url)

        elapsed = time.time()-start

        # If not enough time has been spend, sleep
        if elapsed < delta:
            time.sleep(delta-elapsed)

        # Return the result
        return r








