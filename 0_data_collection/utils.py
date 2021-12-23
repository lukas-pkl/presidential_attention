#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
utility file to support the scraper

Created: 2021 12 22
Author: lukasp
"""

import constants
from datetime import datetime


def get_date(soup):
    """
    Extracts publish time from the page source
    
    PARAMS:
        soup:BeautifulSoup object - page source converted to BeautifulSoup object
    RETURNS:
        date:datetime - date when page was published
    """
    date_tag = soup.find("meta", {"name": "cXenseParse:recs:publishtime"})
    date = None
    if date_tag != None:
        if "content" in date_tag.attrs:
            date_s = date_tag["content"]
            date = datetime.strptime(date_s.split("+")[0], "%Y-%m-%dT%H:%M:%S")
    return date


def get_text(soup):
    """
    Extracts text from the page source
    
    PARAMS:
        soup:BeautifulSoup object - page source converted to BeautifulSoup object
    RETURNS:
        text:str - text contents of the page
    """

    text = ""
    lead = soup.find("div", {"class": "delfi-article-lead"}).text
    text += lead
    p_tags = soup.find_all("p")
    for p in p_tags:
        if not p.text.endswith("..."):
            text += p.text + "\n "
    return text


def clean_link(link: str) -> str:
    """
    Removes bad segments from the extratted links
    """
    bad_parts = [
        "https://www.facebook.com/sharer/sharer.php?u=",
        "https://www.facebook.com/dialog/send?app_id=1476286675946138&link=",
        "https://www.facebook.com/dialog/send?app_id=1476286675946138&link=",
        "&com=1",
    ]
    for bp in bad_parts:
        link = link.replace(bp, "")
    # link = link.split("&redirect_uri=")[0]
    link = link.split("&")[0]
    return link


def get_links(soup):
    """
    Extracts links to other relevant pages from the page source
    
    PARAMS:
        soup:BeautifulSoup object - page source converted to BeautifulSoup object
    RETURNS:
        rel_links2:set - a set of relevant links extracted from text
    """
    a_tags = soup.find_all("a")
    links = [i for i in a_tags if "href" in i.attrs]
    rel_links = set(
        [i["href"] for i in links if i["href"].startswith(constants.lt_core_link)]
    )
    rel_links2 = set([clean_link(i) for i in rel_links])
    return rel_links2
