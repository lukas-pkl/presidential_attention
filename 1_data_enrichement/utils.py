#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
utility file to support the annotator

Created: 2021 12 22
Author: lukasp
"""

from pydantic import BaseModel
from datetime import datetime


def resolve_cabinet(cabinet_data, datetime_obj):
    """
    Assembles list of cabinet member names for a particular datetime
    
    PARAMS:
        cabinet_data:list - a list of cabinet members, their positions and incumbency duration
        datetime_obj:datetime - date when a news article was published
    RETURNS:
        cabinet - a list of people who were ministers and the president for that time
    """

    cabinet = []

    for row in cabinet_data:
        if row["from"] <= datetime_obj <= row["to"]:
            cabinet.append(row)
    return cabinet


def cabinet_entities(datetime_obj, entities, cabinet_data):
    """
    Cross-references a list of entities extracted from text
    with a list of cabinet ministers in office at the time the article was published
    
    PARAMS:
        datetime_obj:datetime - datetime when the article was published
        entities:list - a list of entities extracted from text
        cabinet_data:list - a list of cabinet members, their positions and incumbency duration
    
    RETURNS:
        rel_mentions:list - a list of enities mentioned in text that were the cabinet 
                            members at the time and the president
    """
    entities = set(entities)
    relevant_cabinet = resolve_cabinet(cabinet_data, datetime_obj)
    rel_mentions = []
    for row in relevant_cabinet:
        if any(name in entities for name in row["names"]):
            plh = {
                "person_id": row["person_id"],
                "person_name": row["person_name"],
                "cabinet_ents": row["ministry"],
            }
            rel_mentions.append(plh)
    return rel_mentions


def prime_and_prez(cabinet: list):
    pm = ""
    cabinet_no = ""
    prezident = ""

    for item in cabinet:
        if item["ministry"] == "Premjeras":
            pm = item["person_name"]
            cabinet_no = str(item["cabinet_no"])

        if item["ministry"] == "Prezidentas":
            prezident = item["person_name"]
    return (pm, cabinet_no, prezident)


class LTDoc(BaseModel):

    para_id:str=""
    text: str = ""
    source: str = ""
    cabinet_ents: list = []
    date: datetime = datetime(1990, 3, 11)
    year: int = 1990
    month: int = 0
    day: int = 0
    cabinet: str = ""
    cabinet_no: str = ""
    president: str = ""
