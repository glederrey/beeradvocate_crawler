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

        # Different urls used to crawl the data
        # If you scrap the data again in the future,
        # you may have to change these links.

        self.url_styles = 'https://www.beeradvocate.com/beer/style/'

        # Other parameters
        self.step = 50

    ########################################################################################
    ##                                                                                    ##
    ##                                Crawl the beers                                     ##
    ##                                                                                    ##
    ########################################################################################

    def crawl_styles_page(self):
        """
        STEP 1
        
        Crawl the Styles page on BeerAdvocate
        """
        r = requests.get(self.url_styles)

        # Create folder for all the HTML pages
        folder = self.data_folder + 'misc/'
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        with open(folder + 'styles.html', 'wb') as output:
            output.write(r.content)

    def get_links_styles(self):
        """
        STEP 2
        
        Get the links to the beers styles from the Styles page
        """

        html = open(self.data_folder + 'misc/styles.html', 'rb').read().decode('utf8')

        # String for the REGEX
        str_ = '<br><a href="/beer/style/(.+?)/">(.+?)</a>'

        grp = re.finditer(str_, str(html))

        # Go through all findings
        links = {}
        for g in grp:
            links[g.group(2)] = 'https://www.beeradvocate.com/beer/style/' + g.group(1)

        with open(self.data_folder + 'misc/styles_links.json', 'w') as output:
            json.dump(links, output)

    def crawl_all_styles(self):
        """
        STEP 3
        
        Crawl all the styles pages 
        """

        # Load the JSON file with all the styles
        json_data = open(self.data_folder + 'misc/styles_links.json').read()
        data = json.loads(json_data)

        folder = self.data_folder + 'styles/'
        # Create folder for all the HTML pages
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        pool = mp.Pool(processes=self.threads)

        for key in data.keys():
            pool.apply_async(self.crawl_one_style, args=(key, data[key]))
        pool.close()
        pool.join()

    def crawl_one_style(self, style, link):
        """
        USED BY STEP 3
        
        Crawl all the pages from a given style
        :param style: String with the name of the style
        :param link: URL to the beers of the given style
        """

        # Transform the name with a more convenient syntax
        style = style.replace(' / ', '-').replace(' ', '_')

        folder = self.data_folder + 'styles/' + style + '/'

        # Create folder for all the HTML pages
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        # Get the link and get the first page
        r = requests.get(link)

        # Save it
        with open(folder + '0.html', 'wb') as output:
            output.write(r.content)

        # We also parse it to have the total number of beers for this style
        html = r.content

        # REGEX parsing
        str_ = '(out of (\d+))'
        grp = re.search(str_, str(html))
        # Number is rounded to closest lower *50 value
        nbr = round_(int(grp.group(2)))

        # Download all the pages with the beers for this style
        for i in range(1, int(nbr / self.step) + 1):
            val = self.step * i
            r = requests.get(link + '/?sort=revsD&start=' + str(val))
            with open(folder + str(val) + '.html', 'wb') as output:
                output.write(r.content)

    ########################################################################################
    ##                                                                                    ##
    ##                           Crawl the archived beers                                 ##
    ##                                                                                    ##
    ########################################################################################

    def crawl_all_breweries(self):
        """
        STEP 5

        Crawl all the breweries from the ones we have.
        (We are missing some breweries with only archived beers, but that's not a problem)

        !!! Make sure step 4 was done with the parser !!!
        """

        df = pd.read_csv(self.data_folder + 'parsed/beers.csv')

        folder = self.data_folder + 'breweries/'
        # Create folder for all the HTML pages
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        brewery_ids = list(df['brewery_id'].unique())

        pool = mp.Pool(processes=self.threads)

        for id_ in brewery_ids:
            res = pool.apply_async(self.crawl_one_brewery, args=(id_,))
        res.get()
        pool.close()
        pool.join()

    def crawl_one_brewery(self, brewery_id):
        """
        USED BY STEP 5

        Crawl the page of the brewery

        :param brewery_id: ID of the brewery
        """

        folder = self.data_folder + 'breweries/'

        # URL
        url = 'https://www.beeradvocate.com/beer/profile/{:d}/?view=beers&show=all'.format(brewery_id)

        # Get the HTML page
        r = requests.get(url)

        # Save it
        with open(folder + str(brewery_id) + '.html', 'wb') as output:
            output.write(r.content)

    def crawl_all_beers(self):
        """
        STEP 7

        Crawl all the reviews from all the beers.

        !!! Make sure step 6 was done with the parser !!!
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
            pool.apply_async(self.crawl_one_beer, args=(row['brewery_id'], row['beer_id']))
        pool.close()
        pool.join()

    def crawl_one_beer(self, brewery_id, beer_id):
        """
        USED BY STEP 7

        Crawl all the reviews from one beer

        :param brewery_id: ID of the brewery
        :param beer_id: ID of the beer
        """

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







