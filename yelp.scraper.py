"""
Scrapping yelp reviews in zipcode.

ist558 project
"""

from bs4 import BeautifulSoup
import urllib
import re
import time
import random


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
        print('stopped')
        return []

    resturants = soup.findAll(
        'div',
        attrs={'class': re.compile(r'^search-result natural-search-result')})

    try:
        assert (len(resturants) == 10)
    except:
        print('end of zipcode')
        return [], False

    extracted = {}
    for r in resturants:
        name = r.find('a', {'class': 'biz-name'}).getText()
        address = 'https://www.yelp.com/' + r.find(
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


def crawl(zipcodes):
    """crawl through list of zipcodes."""
    page = 0
    flag = True
    for zipcode in zipcodes:
        while flag:
            resturants, flag = get_resturants(zipcode, page)
            for resturant in resturants:
                biz_id, review = get_review(resturants[resturant])
                reviews = review.pop()
                reviews = reviews.get_text().split("\"description\"")
                for review in reviews[1:]:
                    try:
                        "strip newline symbols"
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
                        pass
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
    data = crawl([16801])
