import bs4
from bs4 import BeautifulSoup
import urllib.request
from time import sleep
import json
from datetime import datetime
import re
import os
import pandas as pd
import uuid
from collections import deque
from pathlib import Path


REQUEST_DELAY_SECONDS = 10

MAX_CARS_HISTORY = 100_000

STORAGE_PATH = "data/autos/"
VISITED_PATH = "data/visited/"
FOLDERS = [STORAGE_PATH, VISITED_PATH]

PATH_TO_SAVED_CARS_IDS = Path(VISITED_PATH) / 'car_ids.json'


# countries = {"Germany": "D",
#              "Austria": "A",
#              "Belgium": "B",
#              "Spain": "E",
#              "France": "F",
#              "Italy": "I",
#              "Luxemburg": "L",
#              "Holland": "NL"}

COUNTRIES = {"Germany": "D"}

BASE_URL = 'https://www.autoscout24.de/lst?sort=age&desc=1&ustate=N%2CU&size=20&page='


for folder in FOLDERS:
    if not os.path.isdir(folder):
        os.mkdir(folder)
        print(folder, "created.")
    else:
        pass

if not os.path.isfile(PATH_TO_SAVED_CARS_IDS):
    with open(PATH_TO_SAVED_CARS_IDS, "w") as file:
        json.dump([], file)


def get_results_page(page_num, country='Deutschland'):
    try:
        url = f'{BASE_URL}{page_num}&cy={COUNTRIES[country]}&atype=C&'

        # print(url)
        # only_a_tags = SoupStrainer("a")

        result = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml')
    except Exception as e:
        print("Error: " + str(e) + " " * 50, end="\r")
        result = None
    return result


def full_parse_page_response(response):
    urls = extract_car_urls(response)

    # TODO: filter out already scrapped ones and record new ones

    cars = [scrape_car(url) for url in urls]
    return cars


def quick_parse_page_response(response):
    parsed_articles = []

    for article in response.findAll('article'):
        parsed_articles.append(parse_article(article))
    return parsed_articles


def scrape_car(url):
    car = BeautifulSoup(urllib.request.urlopen('https://www.autoscout24.de' + url).read(), 'lxml')
    return car


def extract_car_urls(response):
    car_urls = []
    for url_tag in response.findAll("a"):
        link = url_tag.get('href')
        if 'angebote' in link:
            car_urls.append(link)

    return car_urls


def generate_id_from_link(link):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, link))


def parse_article(article: bs4.Tag):
    attribute_names = [
        'data-vehicle-type',
        'data-price-label',
        'data-price',
        'data-make',
        'data-seller-type',
        'data-listing-zip-code',
        'data-mileage',
        'data-fuel-type',
        'data-model',
        'data-first-registration'
    ]
    attribute_dict = {a: article.get(a) for a in attribute_names}

    (header,) = article.select('div[class*="ListItem_header__"]')
    (listing,) = article.select('div[class*="ListItem_listing__"]')

    attribute_dict['link'] = header.find('a').get('href')

    (version_tag,) = header.select('span[class*="ListItem_version__"]')
    attribute_dict['short_description'] = version_tag.text

    details = listing.select('span[class*="VehicleDetailTable_item__"]')
    attribute_dict['engine_power'] = details[2].text
    attribute_dict['condition'] = details[3].text
    attribute_dict['num_owners'] = details[4].text
    attribute_dict['gearbox'] = details[5].text

    attribute_dict['uuid'] = generate_id_from_link(attribute_dict['link'])
    attribute_dict['processed_at'] = datetime.now().strftime('%d-%m-%yyyy')

    return attribute_dict


def scrape_offers(max_page=20):

    new_cars = []

    with open(PATH_TO_SAVED_CARS_IDS) as f:
        processed_ids = deque(list(json.load(f)), maxlen=MAX_CARS_HISTORY)

    for page in range(1, max_page+1):
        response = get_results_page(page, country='Germany')
        cars = quick_parse_page_response(response)

        page_new_cars = [c for c in cars if c['uuid'] not in processed_ids]

        # saving all, will filter out duplicates later
        # will help to track now long articles stay available
        new_cars.extend(cars)

        print(f'Page: {page} | new cars: {len(page_new_cars)} | total new cars: {len(new_cars)}')

        sleep(REQUEST_DELAY_SECONDS)

    archive_file_path = Path(STORAGE_PATH) / f"{datetime.now().strftime('%d%m%y_%H%M%S')}.csv"
    pd.DataFrame(new_cars).to_csv(archive_file_path, index=False)

    processed_ids.extend([c['uuid'] for c in new_cars])

    with open(PATH_TO_SAVED_CARS_IDS, "w") as f:
        json.dump(list(processed_ids), f)


if __name__ == '__main__':
    scrape_offers(1)