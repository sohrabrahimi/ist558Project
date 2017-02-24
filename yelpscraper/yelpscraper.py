"""
Scrapping yelp reviews in zipcode.

ist558 project
"""
from urllib2 import urlopen
import re
import time
import random
import argparse
from bs4 import BeautifulSoup
from utils.networker import  *
from collections import deque

def get_yelp(zipcode, page_num):
    """get yelp address for given zipcode and page number."""
    return 'https://www.yelp.com/search?find_loc={}&start={}&cflt=restaurants'.format(zipcode, page_num)

def get_resturants(zipcode, page_num, tor=False):
    """get resturant names and web page."""
    if tor:
        try:
            page_url = get_yelp(zipcode, page_num)
            soup = BeautifulSoup(get(page_url).text, 'html.parser')
        except:
            print('tor, get_resturant failed')
            return [], False
    else:
        try:
            page_url = get_yelp(zipcode, page_num)
            soup = BeautifulSoup(urlopen(page_url).read(), 'html.parser')
        except:
            print('get_resturant failed')
            return [], False

    resturants = soup.findAll(
        'div',
        attrs={'class': re.compile(r'^search-result natural-search-result')})
    extracted = {}
    for resturant in resturants:
        name = resturant.find('a', {'class': 'biz-name'}).getText()
        address = 'https://www.yelp.com/' + resturant.find(
            'a', {'class': 'biz-name'})['href']
        extracted[name] = address

    return extracted, len(extracted) > 0


def get_review(address, tor=False):
    """get yelp review from web page."""
    if tor:
        soup = BeautifulSoup(get(address).text)
    else:
        soup = BeautifulSoup(urlopen(address).read(), 'html.parser')
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


def get_attribute(address, tor=False):
    """get resturant parameters."""
    try:
        if tor:
            soup = BeautifulSoup(get(address).text)
        else:
            soup = BeautifulSoup(urlopen(address).read(), 'html.parser')
    except:
        print('get_attribute failed')
    my_dict = {}

    try:
        review_count = soup.find('span', {'itemprop': 'reviewCount'}).get_text()
        rating_value = soup.find('meta', {'itemprop':
                                      'ratingValue'}).attrs['content']
    except:
        review_count = ''
        rating_value = ''
    my_dict['ratingValue'] = rating_value
    my_dict['ratingCount'] = review_count

    for tag in soup.find_all('script'):
        if tag.find(string=re.compile("longitude")):
            text = tag.contents[0].splitlines()[-3].strip()
            text = text[text.find('{') + 1:-3]
            for word in text.split(','):
                if len(word) == 1:
                    my_dict['state'] = word
                if len(word.split(':')) == 2:

                    key, value = word.replace("\"", "").split(':')
                    my_dict[key] = value
    return my_dict


def get_zipcode():
    """read zipcodes from file."""
    with open('./data/zipcodes.csv', 'r+') as file:
        zipcodes = [
            int(zipcode.strip()) for zipcode in file.read().split('\n')
            if zipcode.strip()
        ]
    return zipcodes


def crawl(zipcodes=None, tor=False, sleep_time=10):
    """crawl through list of zipcodes."""
    if tor:
        ip_address = get_current_ip()
        set_new_ip()
        used_ips = deque()
        used_ips.append(get_current_ip())
    else:
        ip_address = urlopen('https://icanhazip.com/').read().replace('\n', '')
    page = 0
    request_count = 0
    flag = True
    header_flag = True
    resturant_list = []
    zipcodes = [zipcodes] if zipcodes else get_zipcode()
    for zipcode in zipcodes:
        flag = True
        while flag:
            request_count += 1
            print('page {}, at zipcode {}'.format(page, zipcode))
            resturants, flag = get_resturants(zipcode, page, tor)
            for resturant in resturants:
                if request_count % 50 == 0 and tor:
                    ensure_new_ip(used_ips)
                    request_count = 0
                    print('reached 50 requests, change ip address')
                if request_count % 50 == 0 and not tor:
                    print('reached 50 request, going to sleep {} minutes'.format(sleep_time))
                    time.sleep(60 * sleep_time)
                    request_count = 0
                request_count += 1
                biz_id, review = get_review(resturants[resturant], tor)
                if biz_id in resturant_list:
                    print('repeated resturant:', resturant)
                    continue
                print('scraping returant:' + resturant +
                        ', at zipcode:' + str(zipcode) +
                        ', current IP address:' + ip_address)
                resturant_list.append(biz_id)
                request_count += 1
                attr_dict = get_attribute(resturants[resturant], tor)
                attr_dict['zipcode'] = str(zipcode)
                with open('./data/' + str(zipcode) +'_attributes.csv', 'a') as file:
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
                        print('skipped ' + resturant)
                    with open('./data/' + str(zipcode) +'_reviews.csv', 'a') as file:
                        file.write(words)

            page += 10
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
    parser.add_argument('-t',
                        '--tor',
                        action='store_true',
                        help='use tor to change ip address')
    args = parser.parse_args()
    crawl(zipcodes=args.zipcode, tor=args.tor, sleep_time=10)
