#! /usr/bin/env python
# coding=utf-8
#
# Copyright © 2017 Gael Lederrey <gael.lederrey@epfl.ch>
#
# Distributed under terms of the MIT license.

import pandas as pd
import numpy as np
import re
import os


class Parser:
    """
    Parser for BeerAdvocate website
    """

    def __init__(self, data_folder=None):
        """
        Initialize the class
        
        :param nbr_threads: Number of threads you want to give. If not given, then it will use all the possible ones.
        :param data_folder: Folder to save the data
        """

        if data_folder is None:
            self.data_folder = '../data/'
        else:
            self.data_folder = data_folder

        self.special_places = ['Canada', 'United States', 'United Kingdom']

        self.country_to_change = {'Korea (North)': 'North Korea',
                                  'Korea (South)': 'South Korea',
                                  'Fiji': 'Fiji Islands',
                                  'Bosnia & Herzegovina': 'Bosnia and Herzegovina',
                                  'Cape Verde': 'Cape Verde Islands',
                                  "Cote D'Ivoire (Ivory Coast)": 'Ivory Coast',
                                  'Croatia (Hrvatska)': 'Croatia',
                                  'New Zealand (Aotearoa)': 'New Zealand',
                                  'Russian Federation': 'Russia',
                                  'Saint Vincent & The Grenadines': 'Saint Vincent and The Grenadines',
                                  'Sao Tome & Principe': 'Sao Tome and Principe',
                                  'Turks & Caicos Islands': 'Turks and Caicos Islands',
                                  'Viet Nam': 'Vietnam'}

        self.day_to_nbr = {'Monday': 0,
                           'Tuesday': 1,
                           'Wednesday': 2,
                           'Thursday': 3,
                           'Friday': 4,
                           'Saturday': 5,
                           'Sunday': 6}

    ########################################################################################
    ##                                                                                    ##
    ##                       Parse the breweries from the places                          ##
    ##                                                                                    ##
    ########################################################################################

    def parse_breweries_from_places(self):
        """
        STEP 3

        Parse all the breweries name and ID from the places
        """

        folder = self.data_folder + 'parsed/'
        # Create folder for the parsed CSV tables
        if not os.path.exists(folder):
            os.makedirs(folder)

        folder = self.data_folder + 'places/'

        list_ = os.listdir(folder)

        json_brewery = {'name': [], 'id': [], 'location': []}

        # Go through all the countries
        for country in list_:
            # Check if the country is in the list of special countries
            if country not in self.special_places:
                # Get all the files
                files = os.listdir(folder + country)

                place = country
                # Change name of the country to a more convenient one
                if place in self.country_to_change:
                    place = self.country_to_change[place]

                # Go through all the files
                for file_ in files:
                    # Open them ...
                    html = open(folder + country + '/' + file_, 'rb').read().decode('utf8')

                    # ... and parse them
                    str_ = '<a href="/beer/profile/(\d+)/"><b>(.+?)</b>'

                    grp = re.finditer(str_, str(html))

                    # Put info in JSON
                    for g in grp:
                        json_brewery['id'].append(int(g.group(1)))
                        json_brewery['name'].append(g.group(2))
                        json_brewery['location'].append(place)

            else:
                # Get the list of regions
                list_2 = os.listdir(folder + country)
                # Go through all regions
                for region in list_2:
                    # Get all the files
                    files = os.listdir(folder + country + '/' + region)

                    # Go through all the files
                    for file_ in files:
                        # Open them ...
                        html = open(folder + country + '/' + region + '/' + file_, 'rb').read().decode('utf8')

                        # ... and parse them
                        str_ = '<a href="/beer/profile/(\d+)/"><b>(.+?)</b>'

                        grp = re.finditer(str_, str(html))

                        if country == 'United States':
                            place = country + ', ' + region
                            if place == 'United States, District of Columbia':
                                place = 'United States, New York'
                        elif country == 'Canada':
                            place = country
                        elif country == 'United Kingdom':
                            place = region

                        # Put info in JSON
                        for g in grp:
                            json_brewery['id'].append(int(g.group(1)))
                            json_brewery['name'].append(g.group(2))
                            json_brewery['location'].append(place)

        # Transform into pandas DF
        df = pd.DataFrame(json_brewery)

        # Save it
        df.to_csv(self.data_folder + 'parsed/breweries.csv', index=False)

    ########################################################################################
    ##                                                                                    ##
    ##                Parse the closed breweries to add them to the list                  ##
    ##                                                                                    ##
    ########################################################################################

    def parse_missing_breweries(self):
        """
        STEP 6

        Parse the closed breweries and add them to the list of breweries

        !!! Make sure steps 4 and 5 were done with the crawler !!!
        """

        df = pd.read_csv(self.data_folder + 'parsed/breweries.csv')

        folder = self.data_folder + 'breweries/'

        # Files inside the folder breweries
        list_ = os.listdir(folder)

        # Files already treated
        files = [str(df.ix[i]['id']) + '.html' for i in df.index]

        # Missing files
        missing = list(set(list_) - set(files))

        json_missing = {'name': [], 'id': [], 'place': []}
        for file_ in missing:
            html = open(folder + file_, 'rb').read().decode('utf8')

            # Find the name of the brewery
            str_ = '<h1>(.+?)</h1>'

            grp = re.search(str_, str(html))
            name = grp.group(1)

            try:
                # Find the country
                str_ = '<br><a href="/place/directory/(\d+)/(.+?)/">(.+?)</a>\s*<br><br>'

                grp = re.search(str_, str(html))
                place = grp.group(3)

                if place in self.special_places:
                    # Find the region
                    str_ = '<a href="/place/directory/(\d+)/(.+?)/">(.+?)</a>'

                    grp = re.search(str_, str(html))
                    region = grp.group(3)

                    place = place + ', ' + region
            except AttributeError:
                try:
                    str_ = ', <a href="/place/directory/(.+?)/">(.+?)</a>, <a href="/place/directory/(.+?)/">(.+?)</a>'
                    grp = re.search(str_, str(html))
                    region = grp.group(2)
                    place = grp.group(4) + ', ' + region
                except AttributeError:
                    try:
                        # Find the country
                        str_ = '<a href="/place/directory/(\d+)/(.+?)/">(.+?)</a>\s*<br><br>'

                        grp = re.search(str_, str(html))
                        place = grp.group(3)

                        if place in self.special_places:
                            # Find the region
                            str_ = '<a href="/place/directory/(\d+)/(.+?)/">(.+?)</a>'

                            grp = re.search(str_, str(html))
                            region = grp.group(3)

                            place = place + ', ' + region
                    except AttributeError:
                        place = 'UNKNOWN'

            # Change name of the country to a more convenient one
            if place in self.country_to_change:
                place = self.country_to_change[place]

            if place == 'United States, District of Columbia':
                place = 'United States, New York'

            json_missing['name'].append(name)
            json_missing['location'].append(place)
            json_missing['id'].append(int(file_.replace('.html', '')))

        # To pandas DF
        df_missing = pd.DataFrame(json_missing)

        # Append to the original one
        df = df.append(df_missing, ignore_index=True)

        # Save it
        df.to_csv(self.data_folder + 'parsed/breweries.csv', index=False)

    ########################################################################################
    ##                                                                                    ##
    ##               Parse the breweries files to get the number of beers                 ##
    ##                                                                                    ##
    ########################################################################################

    def parse_breweries_files_for_number(self):
        """
        STEP 7

        Parse the breweries files to get the number of beers from all the breweries
        """
        # Load the DF
        df = pd.read_csv(self.data_folder + 'parsed/breweries.csv')

        folder = self.data_folder + 'breweries/'

        nbr_beers = []
        # Go through all the breweries
        for i in df.index:
            id_ = df.ix[i]['id']
            html = open(folder + str(id_) + '.html', 'rb').read().decode('utf8')

            # Get current number of beers
            str1 = 'Current \((\d+)\)'
            grp = re.search(str1, str(html))
            nbr1 = int(grp.group(1))

            # Get archived
            str2 = 'Arch \((\d+)\)'
            grp = re.search(str2, str(html))
            nbr2 = int(grp.group(1))

            nbr_beers.append(nbr1 + nbr2)

        # Add to the DF
        df.loc[:, 'nbr_beers'] = nbr_beers

        # Save it again
        df.to_csv(self.data_folder + 'parsed/breweries.csv', index=False)

    ########################################################################################
    ##                                                                                    ##
    ##                    Parse the breweries files to get the beers                      ##
    ##                                                                                    ##
    ########################################################################################

    def parse_breweries_files_for_beers(self):
        """
        STEP 8

        Parse all the beers from the breweries and create the CSV file
        """

        df = pd.read_csv(self.data_folder + 'parsed/breweries.csv')

        # Only get the breweries with at least 1 beer
        df = df[df['nbr_beers'] > 0]

        folder = self.data_folder + 'breweries/'

        # Prepare the json for the DF
        json_beers = {'beer_name': [], 'brewery_name': [], 'beer_id': [], 'brewery_id': [], 'style': []}

        for i in df.index:
            row = df.ix[i]
            file_ = folder + str(row['id']) + '.html'
            # Open the HTML
            html = open(file_, 'rb').read().decode('utf8')

            # Get the brewery name
            str_ = '<h1>(.+?)</h1>'
            grp = re.search(str_, str(html))
            brewery = grp.group(1)

            # Get all the other info
            str_ = '<a href="/beer/profile/(\d+)/(\d+)/"><b>(.+?)</b></a></td><td valign=top class="hr_bottom_light">' \
                   '<a href="/beer/style/(\d+)/">(.+?)</a></td><td align="left" valign="top" class="hr_bottom_light">' \
                   '<span style="color: #999999; font-weight: bold;">(.+?)</span></td><td align="left" valign="top" ' \
                   'class="hr_bottom_light"><b>(.+?)</b></td><td align="left" valign="top" class="hr_bottom_' \
                   'light">(.+?)</td>'
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

    ########################################################################################
    ##                                                                                    ##
    ##                   Parse the beer files to get some information                     ##
    ##                                                                                    ##
    ########################################################################################

    def parse_beer_files_for_information(self):
        """
        STEP 10

        Parse the beer files to get some information on the beers

        !!! Make sure step 9 was done with the crawler !!!
        """

        # Load the DF
        df = pd.read_csv(self.data_folder + 'parsed/beers.csv')

        nbr_ratings = []
        nbr_reviews = []
        ba_score = []
        bros_score = []
        avg = []
        abv = []

        for i in df.index:
            row = df.ix[i]

            file = self.data_folder + 'beers/{}/{}/0.html'.format(row['brewery_id'], row['beer_id'])

            # Open the file
            html_txt = open(file, 'rb').read().decode('utf-8')

            # Find number of ratings
            str_ = '<dt>Ratings:</dt>\\n\\t\\t\\t\\t\\t<dd><span class="ba-ratings">(.+?)</span></dd>'

            grp = re.search(str_, html_txt)

            nbr_rat = int(grp.group(1).replace(',', ''))

            nbr_ratings.append(nbr_rat)

            # Find number of reviews
            str_ = '<dt>Reviews:</dt>\\n\\t\\t\\t\\t\\t<dd><span class="ba-reviews">(.+?)</span></dd>'

            grp = re.search(str_, html_txt)

            nbr_rev = int(grp.group(1).replace(',', ''))

            nbr_reviews.append(nbr_rev)

            # Find the average
            str_ = 'Avg:</dt>\\n\\t\\t\\t\\t\\t<dd><span class="ba-ravg">(.+?)</span></dd>'

            grp = re.search(str_, html_txt)

            avg_val = float(grp.group(1))

            if nbr_rat == 0:
                avg_val = np.nan

            avg.append(avg_val)

            # Find the BA Score
            str_ = '<b>BA SCORE</b>\\n\\t\\t\\t<br>\\n\\t\\t\\t<span class="BAscore_big ba-score">(.+?)</span>'

            grp = re.search(str_, html_txt)

            try:
                ba = float(grp.group(1))
            except ValueError:
                ba = np.nan

            ba_score.append(ba)

            # Find the Bros score
            str_ = '<b>THE BROS</b>\\n\\t\\t\\t<br>\\n\\t\\t\\t<span class="BAscore_big ba-bro_score">(.+?)</span>'

            grp = re.search(str_, html_txt)

            try:
                bros = float(grp.group(1))
            except ValueError:
                bros = np.nan

            bros_score.append(bros)

            # Find the ABV
            str_ = '<b>Alcohol by volume \(ABV\):</b> (.+?)\\n\\t\\t<br>'

            grp = re.search(str_, html_txt)

            try:
                abv_val = float(grp.group(1).replace('%', ''))
            except ValueError:
                abv_val = np.nan

            abv.append(abv_val)

        # Add the new columns
        df.loc[:, 'nbr_ratings'] = nbr_ratings
        df.loc[:, 'nbr_reviews'] = nbr_reviews
        df.loc[:, 'avg'] = avg
        df.loc[:, 'ba_score'] = ba_score
        df.loc[:, 'bros_score'] = bros_score
        df.loc[:, 'abv'] = abv

        # Save it again
        df.to_csv(self.data_folder + 'parsed/beers.csv', index=False)

    ########################################################################################
    ##                                                                                    ##
    ##                      Parse the beer files to get the reviews                       ##
    ##                                                                                    ##
    ########################################################################################

    def parse_beer_files_for_reviews(self):
        """
        STEP 11

        Parse the beer files to get some information on the beers

        !!! Make sure step 9 was done with the crawler !!!
        """