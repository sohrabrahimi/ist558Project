# Yelp Scraper (ist558Project)

This project was conducted to fullfill a data mining course requirement at Penn State. The current repository includes the data collection process from Yelp. The code takes a U.S. zip code as an input and returns two separate CSV files. The first will include all the restaurant attributes in that zip codes, including their names, geographic coordinates, restaurant type, stars, and number of totl reviews. The second file includes the comments provided by reviewers for every restaurant. The two tables can be joined on a unique Business_ID. 


## Usage

`python yelp_scraper.py -z 16801, 16803`
