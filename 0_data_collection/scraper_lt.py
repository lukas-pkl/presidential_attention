#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper to collect data from delfi.lt

Created: 2021 12 22
Author: lukasp
"""

# Standard library
import requests
from datetime import datetime
from tqdm import tqdm
import random
import time

# Pypi libraries
from bs4 import BeautifulSoup
from pymongo import MongoClient

# Local
import constants
from utils import get_date, get_links, get_text, clean_link


def add_to_visited(link):
    last_char = link[-1]
    if last_char == "0":
        visited_0.add(link)
    elif last_char == "1":
        visited_1.add(link)
    elif last_char == "2":
        visited_2.add(link)
    elif last_char == "3":
        visited_3.add(link)
    elif last_char == "4":
        visited_4.add(link)
    elif last_char == "5":
        visited_5.add(link)
    elif last_char == "6":
        visited_6.add(link)
    elif last_char == "7":
        visited_7.add(link)
    elif last_char == "8":
        visited_8.add(link)
    elif last_char == "9":
        visited_9.add(link)
    else:
        visited_oth.add(link)


def check_visited(link):
    last_char = link[-1]

    if last_char == "0":
        return link in visited_0
    elif last_char == "1":
        return link in visited_1
    elif last_char == "2":
        return link in visited_2
    elif last_char == "3":
        return link in visited_3
    elif last_char == "4":
        return link in visited_4
    elif last_char == "5":
        return link in visited_5
    elif last_char == "6":
        return link in visited_6
    elif last_char == "7":
        return link in visited_7
    elif last_char == "8":
        return link in visited_8
    elif last_char == "9":
        return link in visited_9
    else:
        return link in visited_oth


visited_0 = set()
visited_1 = set()
visited_2 = set()
visited_3 = set()
visited_4 = set()
visited_5 = set()
visited_6 = set()
visited_7 = set()
visited_8 = set()
visited_9 = set()
visited_oth = set()

mongo = MongoClient(constants.mongo_conn_string)
mongo_col = mongo[constants.mongo_db][constants.mongo_lt_col]

# Step 1: Get visited links from the DB
print("Fetching previous links from mongo")
print(datetime.now())
print()

query = {}
projection = {"_id": 0, "source": 1}

for i in tqdm(mongo_col.find(query, projection)):
    add_to_visited(i["source"])

# Step 2: Scrape first 200 index pages to get new links

# 2.1 Initial page
print("Collecting links")
print(datetime.now())
print()

links = set()

res = requests.get(constants.lt_core_link)
add_to_visited(constants.lt_core_link)
if res.status_code == 200:
    soup = BeautifulSoup(res.text, "html5lib")
    site_links = get_links(soup)
    for link in site_links:
        if check_visited(link) == False:
            links.add(link)
else:
    pass

# 2.2 Numbered pages 2-200

pages = [i for i in range(2, 200)]

for i in tqdm(pages):
    res = requests.get(constants.lt_core_link_ext + str(i))
    if res.status_code == 200:
        soup = BeautifulSoup(res.text, "html5lib")
        site_links = get_links(soup)
        for link in site_links:
            if check_visited(link) == False:
                links.add(link)
    else:
        pass
    sleep_interval = (
        random.randint(100, 300) / 100
    )  # Sleep a random interval between 1 and 3 s
    time.sleep(sleep_interval)

print(f"Collected {len(links)} links")
print(datetime.now())
print()

# Step 3: Actual scraping
print("Scraping")
print(datetime.now())
print()

counter = 0
faulty_links = []
print(datetime.now(), " Started")
while True:
    if len(links) > 0:
        current_link = links.pop()
        add_to_visited(current_link)
        try:
            res = requests.get(current_link)
            if res.status_code == 200:
                try:
                    soup = BeautifulSoup(res.text, "html5lib")
                    date = get_date(soup)
                    site_links = get_links(soup)
                    text = get_text(soup)

                    doc = {"source": current_link, "text": text, "date": date}
                    mongo_col.insert_one(doc)
                    counter += 1

                    if counter % 1000 == 0:
                        print(datetime.now(), " Counter at ", counter)

                    for link in site_links:
                        if check_visited(link) == False:
                            links.add(link)

                except Exception as e:
                    print(datetime.now(), current_link, e)
                    faulty_links.append(current_link)
        except Exception as e:
            print(datetime.now(), " Faulty link ", current_link)

        pass
    else:
        if len(faulty_links) > 0:
            for link in faulty_links:
                links.add(link)
        else:
            break

print(f"{counter} new texts inserted to DB")
print(datetime.now())
print()
