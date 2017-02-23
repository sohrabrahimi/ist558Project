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
    return 'http://www.yelp.com/search?find_desc=&find_loc={0}' \
        '&ns=1#cflt=restaurants&start={1}'.format(zipcode, page_num)


def get_resturants(zipcode, page_num):
    """get resturant names and web page."""
    try:
        page_url = get_yelp(zipcode, page_num)
        soup = BeautifulSoup(get(page_url).text, 'html.parser')

        if soup.find_all('title').get_text() == u'503 Service Unavailable':
            print('get_resturant: 503 error')
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

    return extracted, True


def get_review(address):
    """get yelp review from web page."""
    soup = BeautifulSoup(get(address).text)
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
    try:
        soup = BeautifulSoup(get(address).text)
    except:
        print('get_attribute failed')
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


def crawl(zipcodes=None, tor=False):
    """crawl through list of zipcodes."""
    if tor:
        set_new_ip()
        used_ips = deque()
        used_ips.append(get_current_ip())
    else:
        get = urlopen
    page = 0
    request_count = 0
    flag = True
    header_flag = True
    biz_list = []
    zipcodes = [zipcodes] if zipcodes else get_zipcode()
    for zipcode in zipcodes:
        if page != 0:
            time.sleep(random.random() * 1 + 1)
        while flag:
            request_count += 1
            resturants, flag = get_resturants(zipcode, page)
            for resturant in resturants:
                if request_count % 50 == 0 and tor:
                    ensure_new_ip(used_ips)
                    request_count = 0
                elif request_count % 50 == 0:
                    time.sleep(60 * 15)
                    request_count = 0
                request_count += 1
                biz_id, review = get_review(resturants[resturant])
                if biz_id in biz_list:
                    continue
                print('scraping returant {}, at zipcode {}, current IP address {}'.
                      format(biz_id, zipcode, get_current_ip()))
                biz_list.append(biz_id)
                request_count += 1
                attr_dict = get_attribute(resturants[resturant])
                attr_dict['zipcode'] = str(zipcode)
                with open('./data/' + str(zipcode) +'attributes.csv', 'a') as file:
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
                    with open('./data/' + str(zipcode) +'reviews.csv', 'a') as file:
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
    parser.add_argument('-t',
                        '--tor',
                        action='store_true',
                        help='use tor to change ip address')
    args = parser.parse_args()
    crawl(zipcodes=args.zipcode, tor=args.tor)
