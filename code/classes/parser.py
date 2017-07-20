#! /usr/bin/env python
# coding=utf-8
#
# Copyright © 2017 Gael Lederrey <gael.lederrey@epfl.ch>
#
# Distributed under terms of the MIT license.

import multiprocessing as mp
import pandas as pd
import shutil
import json
import re
import os


class Parser:
    """
    Parser for BeerAdvocate website
    """

    def __init__(self, nbr_threads=None):
        """
        Initialize the class
        
        :param nbr_threads: Number of threads you want to give. If not given, then it will use all the possible ones.
        """

        self.data_folder = '../data/'

        if nbr_threads is None:
            self.threads = mp.cpu_count()
        else:
            self.threads = nbr_threads

    ########################################################################################
    ##                                                                                    ##
    ##                      Parse the beers from the styles pages                         ##
    ##                                                                                    ##
    ########################################################################################

    def parse_beers_from_styles(self):
        """
        STEP 4

        Parse the beers from the styles pages. The resulting data are put into a pandas DF.
        
        !!! Make sure steps 1, 2 and 3 were done with the crawler !!!
        """

        # Get the styles
        json_data = open(self.data_folder + 'misc/styles_links.json').read()
        data = json.loads(json_data)

        styles = list(data.keys())

        # Transform the styles into folder names
        folders = {}
        for st in styles:
            folders[st] = st.replace(' / ', '-').replace(' ', '_')

        folder = self.data_folder + 'parsed/'
        # Create folder for the parsed CSV tables
        if os.path.exists(folder):
            shutil.rmtree(folder)

        os.mkdir(folder)

        # Multiprocess the parsing
        pool = mp.Pool(processes=self.threads)

        for style in styles:
            pool.apply_async(self.parse_one_style, args=(style, folders[style]))
        pool.close()
        pool.join()

        df_beers = pd.DataFrame()

        for style in styles:
            df = pd.read_csv(self.data_folder + 'styles/' + folders[style] + '/beers.csv')
            df_beers = df_beers.append(df, ignore_index=True)

        # Save the final DF
        df_beers.to_csv(self.data_folder + 'parsed/beers.csv', index=False)

    def parse_one_style(self, style, folder):
        """
        USED BY STEP 4

        Parse a style given the folder and add it to the df_beers
        :param style: Name of the style
        :param folder: folder where the HTML pages are
        """

        # Get the list of files inside the folder
        list_files = os.listdir(self.data_folder + 'styles/' + folder)

        # Prepare JSON for all the files
        json_beers = {'beer_name': [], 'brewery_name': [], 'beer_id': [], 'brewery_id': [], 'style': []}

        # Read the files
        for file_ in list_files:
            html = open(self.data_folder + 'styles/' + folder + '/' + file_, 'rb').read().decode('utf8')

            # REGEX
            str_ = '<a href="/beer/profile/(\d+)/(\d+)/"><b>(.+?)</b></a></td><td valign="top" ' \
                   'class="hr_bottom_light"><a href="/beer/profile/(\d+)/">(.+?)</a></td><td align="left" ' \
                   'valign="top" class="hr_bottom_light"><span style="color: #999999; font-weight: bold;">(.+?)' \
                   '</style></td><td align="left" valign="top" class="hr_bottom_light"><b>(.+?)</b></td><td ' \
                   'align="left" valign="top" class="hr_bottom_light"><b>(.+?)</b></td>'

            # Iterator
            grp = re.finditer(str_, str(html))

            # Add to the JSON
            for g in grp:
                json_beers['brewery_id'].append(int(g.group(1)))
                json_beers['beer_id'].append(int(g.group(2)))
                json_beers['beer_name'].append(g.group(3))
                json_beers['brewery_name'].append(g.group(5))
                json_beers['style'].append(style)

        # Create the pandas DF
        df = pd.DataFrame(json_beers)

        # Save to a CSV file
        df.to_csv(self.data_folder + 'styles/' + folder + '/beers.csv', index=False)

    ########################################################################################
    ##                                                                                    ##
    ##                        Parse the beers from the breweries                          ##
    ##                                                                                    ##
    ########################################################################################

    def parse_breweries(self):
        """
        STEP &

        Parse all the beers from the breweries and replace the beers.csv file

        !!! Make sure step 5 was done with the crawler !!!
        """

        # List the files in the breweries folder
        files = os.listdir(self.data_folder + 'breweries/')

        # Prepare the json for the DF
        json_beers = {'beer_name': [], 'brewery_name': [], 'beer_id': [], 'brewery_id': [], 'style': []}

        # Go through all files
        for file_ in files:
            # Open the HTML
            html = open(self.data_folder + 'breweries/' + file_, 'rb').read().decode('utf8')

            # Get the brewery name
            str_ = '<h1>(.+?)</h1>'
            grp = re.search(str_, str(html))
            brewery = grp.group(1)

            # Get all the other info
            str_ = '<a href="/beer/profile/(\d+)/(\d+)/"><b>(.+?)</b></a></td><td valign=top class="hr_bottom_light"><a href="/beer/style/(\d+)/">(.+?)</a></td><td align="left" valign="top" class="hr_bottom_light"><span style="color: #999999; font-weight: bold;">(.+?)</span></td><td align="left" valign="top" class="hr_bottom_light"><b>(.+?)</b></td><td align="left" valign="top" class="hr_bottom_light">(.+?)</td>'
            grp = re.finditer(str_, str(html))
            for g in grp:
                json_beers['beer_name'].append(g.group(3))
                json_beers['brewery_name'].append(brewery)
                json_beers['beer_id'].append(g.group(2))
                json_beers['brewery_id'].append(g.group(1))
                json_beers['style'].append(g.group(5))

        # Transform JSON in pandas DF
        df_beers = pd.DataFrame(json_beers)

        # Save to a CSV file
        df_beers.to_csv(self.data_folder + 'parsed/beers.csv', index=False)
