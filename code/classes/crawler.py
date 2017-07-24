#! /usr/bin/env python
# coding=utf-8
#
# Copyright © 2017 Gael Lederrey <gael.lederrey@epfl.ch>
#
# Distributed under terms of the MIT license.

from classes.helpers import round_
import multiprocessing as mp
import pandas as pd
import requests
import shutil
import json
import re
import os


class Crawler:
    """
    Crawler for BeerAdvocate website
    """

    def __init__(self, nbr_threads=None):
        """
        Initialize the class.
        
        :param nbr_threads: Number of threads you want to give. If not given, then it will use all the possible ones.
        """

        self.data_folder = '../data/'
        if nbr_threads is None:
            self.threads = mp.cpu_count()
        else:
            self.threads = nbr_threads

        self.specials_places = ['Canada', 'United States', 'United Kingdom']

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
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        # Crawl the countries
        r = requests.get(url_countries)
        with open(folder + 'countries.html', 'wb') as output:
            output.write(r.content)

        # Create folder for all the places
        folder = self.data_folder + 'places/'
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        # Parse the countries
        html = open(self.data_folder + 'misc/countries.html', 'rb').read().decode('utf8')

        str_ = '<a href="/place/directory/0/(.+?)/">(.+?)</a>'

        grp = re.finditer(str_, str(html))

        pool = mp.Pool(processes=self.threads)

        for g in grp:
            grp1 = g.group(1)
            grp2 = g.group(2)
            res = pool.apply_async(self.crawl_one_place, args=(grp1, grp2))
        res.get()
        pool.close()
        pool.join()

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
        if country not in self.specials_places:
            # Download the page with the number of breweries
            url = 'https://www.beeradvocate.com/place/directory/0/{}/'.format(grp1)
            r = requests.get(url)
            # Get the number of breweries
            str_ = 'Brewery \((\d+)\)'
            test = re.search(str_, str(r.content))
            # Check if it's more than 0
            if int(test.group(1)) > 0:
                # Save the first page in this case
                url = 'https://www.beeradvocate.com/place/list/?start=0&c_id={}&brewery=Y&sort=name'.format(grp1)
                r = requests.get(url)
                os.mkdir(folder + country)
                with open(folder + country + '/0.html', 'wb') as output:
                    output.write(r.content)
        else:
            # Download the page with all the regions
            url = 'https://www.beeradvocate.com/place/directory/0/{}/'.format(grp1)
            r = requests.get(url)
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
                    r = requests.get(url)

                    # Get the number of breweries
                    str_ = 'Brewery \((\d+)\)'
                    test = re.search(str_, str(r.content))
                    # Check if it's more than 0
                    if int(test.group(1)) > 0:
                        # Save the first page in this case
                        url = 'https://www.beeradvocate.com/place/list/?start=0&c_id={}&s_id={}&brewery=Y&sort=name'.format(
                            grp1, g_spec.group(1))
                        r = requests.get(url)
                        name = country + '/' + place
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

        pool = mp.Pool(processes=self.threads)

        for place in list_:
            res = pool.apply_async(self.crawl_breweries_from_one_place, args=(place,))
        res.get()
        pool.close()
        pool.join()

    def crawl_breweries_from_one_place(self, dir_):
        """
        USED BY STEP 2

        Crawl the missing pages with the breweries from a place

        :param dir_: Name of the folder
        """
        step = 20

        folder = self.data_folder + 'places/'

        if dir_ not in self.specials_places:
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
            for i in range(1, int((nbr) / step) + 1):
                start = i * step
                url = 'https://www.beeradvocate.com/place/list/?start={:d}&c_id={}&brewery=Y&sort=name'.format(start,
                                                                                                               code)
                r = requests.get(url)

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
                for i in range(1, int((nbr) / step) + 1):
                    start = i * step
                    url = 'https://www.beeradvocate.com/place/list/?start={:d}&c_id={}&s_id={}&brewery=Y&sort=name'.format(
                        start, code, code_region)
                    r = requests.get(url)

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
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        pool = mp.Pool(processes=self.threads)

        for i in df.index:
            row = df.ix[i]
            res = pool.apply_async(self.crawl_one_brewery, args=(row['id'],))
        res.get()
        pool.close()
        pool.join()

    def crawl_one_brewery(self, id_):
        """
        USED BY STEP 4

        Crawl the HTML page of one brewery
        :param id_: ID of the brewery
        """

        folder = self.data_folder + 'breweries/'

        # Get the HTML page
        url = 'https://www.beeradvocate.com/beer/profile/{:d}/?view=beers&show=all'.format(id_)
        r = requests.get(url)

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

        pool = mp.Pool(processes=self.threads)

        for i in missing:
            res = pool.apply_async(self.crawl_one_closed_brewery, args=(i,))
        res.get()
        pool.close()
        pool.join()

    def crawl_one_closed_brewery(self, id_):
        """
        USED BY STEP 5

        Crawl the page of the brewery if it's one

        :param id_: ID for the place
        """

        folder = self.data_folder + 'breweries/'

        url = 'https://www.beeradvocate.com/beer/profile/{:d}/?view=beers&show=all'.format(id_)

        r = requests.get(url)

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
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        pool = mp.Pool(processes=self.threads)

        for i in df.index:
            row = df.ix[i]
            res = pool.apply_async(self.crawl_one_beer, args=(row['brewery_id'], row['beer_id']))
        res.get()
        pool.close()
        pool.join()

    def crawl_one_beer(self, brewery_id, beer_id):
        """
        USED BY STEP 9

        Crawl all the reviews from one beer

        :param brewery_id: ID of the brewery
        :param beer_id: ID of the beer
        """

        try:
            # Create the folder
            folder = self.data_folder + 'beers/{:d}/{:d}/'.format(brewery_id, beer_id)
            os.makedirs(folder)

            # First URL
            url = 'https://www.beeradvocate.com/beer/profile/{:d}/{:d}'.format(brewery_id, beer_id)

            # Get it and write it
            r = requests.get(url)
            with open(folder + '0.html', 'wb') as output:
                output.write(r.content)

            # Parse it to get the number of Ratings
            html = r.content
            str_ = '</i> Ratings: (.+?)</b>'
            grp = re.search(str_, str(html))
            if grp is not None:
                nbr = int(grp.group(1).replace(',', ''))

                # Get all the pages with the reviews and ratings
                step = 25

                for i in range(1, int(round_(nbr, 25) / step) + 1):
                    tmp = i * step
                    url_tmp = url + '/?view=beer&sort=&start=' + str(tmp)

                    r = requests.get(url_tmp)

                    with open(folder + str(tmp) + '.html', 'wb') as output:
                        output.write(r.content)
        except Exception as e:
            print('---------------------------------------------------------------------')
            print('')
            print('ERROR WITH BREWERY_ID {} AND BEER_ID {}'.format(brewery_id, beer_id))
            print(e)
            print('---------------------------------------------------------------------')
            print('')








