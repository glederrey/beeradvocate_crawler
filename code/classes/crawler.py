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
    ##                          Crawl the beers and the reviews                           ##
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








