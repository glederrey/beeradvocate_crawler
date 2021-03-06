# beeradvocate_crawler

This project has the aim of crawling all the breweries, beers, reviews and users from the website 
[BeerAdvocate](http://wwww.beeradvocate.com). We do not guarantee that everything has been crawled. 
However, the majority of the website has been crawled. 

In the folder code, you will find a code called `run.py`. It is used to crawl everything. This file is
using two classes:
- `Crawler`: used to crawl the different HTML pages of this website
- `Parser`: used to parse the HTML files after they have been crawled

After running the code, you will get a folder called `data` with several subfolders:
- `misc` contains just a few miscalleneous files
- `places` contains the information about the places. Each place is represented by a folder with its name.
 Inside these folders, you will find the HTML pages with all the breweries from the given place.
- `breweries` contains all the HTML files of all the breweries. Inside these HTML files, the links
 to all the beers are written.
- `beers` contains all the pages of all the beers. Inside this folder, you will find all the folders
 from all the breweries. Inside the breweries folders, you will find the folders for all the beers 
 from this given brewery. Inside the beers folders, you will find the HTML pages with all the reviews.
- `users` contains all the pages of all the users. 
- `parsed` **contains all the parsed data**. In particular, it contains the files: 
 *breweries.csv*, *beers.csv*, *users.csv*, *ratings.txt.gz*, and *reviews.txt.gz*.

## Ratings

The collection of ratings are in the files *ratings.txt.gz* and *reviews.txt.gz* in the folder `parsed`. Two files were 
created according to the definitions of the reviews on BeerAdvocate website. Indeed, for them a review is a rating with 
a text of a least 150 characters. Therefore, the file *reviews.txt.gz* contains only the reviews while the file 
*ratings.txt.gz* contains all the ratings (with and without text). In the folder `code`, there is an 
example in python how to parse this file called [example_parser](./code/example_parser.py). The function parse (that you can reuse) is creating an iterator from the 
file. Then, you will go through each item (being a full rating). Each item can be treated as a dict or a JSON. Here is 
the list of key-value pairs with their type (that you have to change):

| Keys             | Type  | Description                           | **Warning**                                                                            |
| :--------------- | :---- | :------------------------------------ | :------------------------------------------------------------------------------------- |
| **beer_name**    | str   | Name of the beer                      |                                                                                        |
| **beer_id**      | int   | ID of the beer                        |                                                                                        |
| **brewery_name** | str   | Name of the brewery                   |                                                                                        |
| **brewery_id**   | int   | ID of the brewery                     |                                                                                        |
| **style**        | str   | Style of the beer                     |                                                                                        |
| **abv**          | float | ABV (Alcohol By Volume) in percentage |                                                                                        |
| **user_name**    | str   | Name of the user                      |                                                                                        |
| **user_id**      | str   | ID of the user                        |                                                                                        |
| **appearance**   | float | Rating for appearance                 | Not always available                                                                   |
| **aroma**        | float | Rating for aroma                      | Not always available                                                                   |
| **palate**       | float | Rating for palate                     | Not always available                                                                   |
| **taste**        | float | Rating for taste                      | Not always available                                                                   |
| **overall**      | float | Rating for overall                    | Not always available                                                                   |
| **rating**       | float | Final rating                          |                                                                                        |
| **text**         | str   | Text of the rating                    | Not always available in *ratings.txt.gz*. At least 150 characters in *reviews.txt.gz*. |
| **date**         | int   | Date of the review in UNIX Epoch      | No access to time of the day. => Time is always noon.                                  |
| **review**       | bool  | Boolean to say if it's a review       | Only available in the file *ratings.txt.gz* (String with 'True' and 'False'            |

## Crawled Data

Please contact directly [Robert West](mailto:robert.west@epfl.ch) and/or [Gael Lederrey](mailto:gael.lederrey@epfl.ch) to get the data. 

## Some Numbers

We give here the table with the different number of objects you can find in the parsed data, see folder `parsed`.

| Elements                  | Numbers   |
| :------------------------ | :-------- |
| Breweries                 | 16'758    |
| Beers                     | 280'823   |
| Beers (at least 1 rating) | 171'401   |
| Users                     | 153'704   |
| Ratings                   | 8'393'032 |
| Reviews (nbr chars > 150) | 2'589'586 |

## Procedure

1. **Crawl** all the places (countries and regions)
2. **Crawl** all the *open* breweries from these places
3. **Parse** the breweries from these places and create a CSV file (*breweries.csv*)
4. **Crawl** the remaining pages from the breweries (if more than 20 beers per brewery)
5. **Crawl** all the *closed* breweries
6. **Parse** all the missing breweries and add them to the CSV file (*breweries.csv*)
7. **Parse** all the breweries to add the number of beers to the CSV file (*breweries.csv*)
8. **Parse** all the breweries to get the beers and create a CSV file (*beers.csv*)
9. **Crawl** all the beers and their reviews
10. **Parse** all the beers to add some information in the CSV file (*beers.csv*)
11. **Parse** all the beers to get all the reviews and save them in two gzip files (*ratings.txt.gz* and *reviews.txt.gz*)
12. Get (**Parse**) the users from the file (*ratings.txt.gz*) and save them in the CSV file (*users.csv*)
13. **Crawl** all the users 
14. **Parse** all the users to get some information and update the CSV (*users.csv*)
15. **Crawl** the users who have put a restriction on their profile with the cookies of the connection with an account.
(!!! Needs to be done on a personal computer and you need an account on BeerAdvocate and get the cookies !!!)
16. **Parse** the users who have been crawled with the cookies and update the CSV (*users.csv*)

## Dates of crawling

The places, the breweries and the beers have been crawled between the 25th of July and the 1st of August 2017. 
The users have been crawled between the 2nd and the 4th of August 2017.

## Required packages

* `requests`
* `pandas`
* `numpy`
* `shutil`
* `json`
* `re`

This code has been developed on Linux (Linux Mint 18.1). Therefore, we do not guarantee that it works on another OS.


