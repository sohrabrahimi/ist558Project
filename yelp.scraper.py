"""
Scrapping yelp reviews in zipcode.

ist558 project
"""

import urllib
import re
import time
import random
from bs4 import BeautifulSoup
import argparse


def get_yelp(zipcode, page_num):
    """get yelp address for given zipcode and page number."""
    return 'http://www.yelp.com/search?find_desc=&find_loc={0}' \
        '&ns=1#cflt=restaurants&start={1}'.format(zipcode, page_num)


def get_resturants(zipcode, page_num):
    """get resturant names and web page."""
    try:
        page_url = get_yelp(zipcode, page_num)
        soup = BeautifulSoup(
            urllib.request.urlopen(page_url).read(), "html5lib")
    except:
        print('failed')
        return []

    resturants = soup.findAll(
        'div',
        attrs={'class': re.compile(r'^search-result natural-search-result')})
    extracted = {}
    for resturant in resturants:
        name = resturant.find('a', {'class': 'biz-name'}).getText()
        address = 'https://www.yelp.com/' + resturant.find(
            'a', {'class': 'biz-name'})['href']
        extracted[name] = address

    return extracted, True


def get_review(address):
    """get yelp review from web page."""
    soup = BeautifulSoup(urllib.request.urlopen(address).read(), "html5lib")
    try:
        review = soup('script', {'type': "application/ld+json"})
        for tag in soup.find_all('meta'):
            try:
                if tag.attrs['name'] == 'yelp-biz-id':
                    biz_id = tag.attrs['content']
                    return biz_id, review
            except:
                pass
    except:
        print("failed to get reviews")
        return '', []


def get_attribute(address):
    """get resturant parameters."""
    soup = BeautifulSoup(urllib.request.urlopen(address).read(), "html5lib")

    my_dict = {}

    review_count = soup.find('span', {'itemprop': 'reviewCount'}).get_text()
    rating_value = soup.find('meta', {'itemprop':
                                      'ratingValue'}).attrs['content']
    my_dict['ratingValue'] = rating_value
    my_dict['ratingCount'] = review_count

    for tag in soup.find_all('script'):
        if tag.find(string=re.compile("longitude")):
            text = tag.contents[0].splitlines()[-3].strip()
            text = text[text.find('{') + 1:-3]
            for word in text.split(','):
                if len(word) == 1:
                    my_dict['state'] = word
                    pass
                if len(word.split(':')) == 2:

                    key, value = word.replace("\"", "").split(':')
                    my_dict[key] = value
    return my_dict


def crawl(zipcodes):
    """crawl through list of zipcodes."""
    page = 0
    flag = True
    header_flag = True
    for zipcode in zipcodes:
        while flag:
            resturants, flag = get_resturants(zipcode, page)
            for resturant in resturants:
                biz_id, review = get_review(resturants[resturant])
                attr_dict = get_attribute(resturants[resturant])
                attr_dict['zipcode'] = str(zipcode)
                with open(
                        str(zipcodes).replace(',', '_').replace(
                            '[', '').replace(']', '') + '_attributes.csv',
                        'a') as file:
                    if header_flag:
                        file.write(','.join(list(attr_dict.keys())) + '\n')
                        header_flag = False
                    file.write(','.join(list(attr_dict.values())) + '\n')
                reviews = review.pop()
                reviews = reviews.get_text().split("\"description\"")
                for review in reviews[1:]:
                    try:
                        words = review.split("\"author\"")[0].\
                            replace("\\n", '').lower()
                        words = re.sub("[^a-zA-Z]", " ", words).split(" ")
                        words = [word for word in words if len(word) > 1]
                        if biz_id == '':
                            words.insert(0, resturant)
                        else:
                            words.insert(0, biz_id)
                        words.append('\n')
                        words = ','.join(words)
                    except:
                        print('skip ' + resturant)
                    with open(
                            str(zipcodes).replace(',', '_').replace(
                                '[', '').replace(']', '') + '.csv',
                            'a') as file:
                        file.write(words)
            if not flag:
                print('Stopped')
                break
            page += 1
            time.sleep(random.randint(0, 1) * .931467298)
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Extracts all yelp restaurant \
    data from a specified zip code (or all American zip codes if nothing \
    is provided)')
    parser.add_argument(
        '-z',
        '--zipcode',
        nargs='+',
        type=int,
        help='Enter a zip code \
    you\'t like to extract from.')
    args = parser.parse_args()
    crawl(args.zipcode)
