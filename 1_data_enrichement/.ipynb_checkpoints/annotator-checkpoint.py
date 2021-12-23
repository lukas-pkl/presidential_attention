#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Annotator to add NER and time variables

Created: 2021 12 22
Author: lukasp
"""

from datetime import datetime
from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import spacy
import pickle

import utils 
import constants

# Load auxiliary file with cabinet member names
with open("./Cabinet_data_NER.pkl", "rb") as file:
    dfl = pickle.load(file)

mongo = MongoClient(constants.mongo_conn_string)
mongo_lt_source_col = mongo[constants.mongo_db][constants.mongo_lt_col]
mongo_lt_destination_col = mongo[constants.mongo_db][constants.mongo_lt_annotated_col]

nlp = spacy.load("lt_core_news_lg")

class LTAnnotator:
    """
    Annotates LT news texts to detect mentions of cabinet members and president
    """

    def __init__(self):
        #self. mongo_source = mongo_lt_source_col
        #self.mongo_destination = mongo_lt_destination_col
        self.nlp = nlp
        self.cabinet_data = dfl

    def detect_entities(self, input_data):
        """
        PARAMS: 
            input_data: list [{"_id":IDObject, "text":str, "date":datetime, "source":str}, 
                            {{"_id":IDObject, "text":str, "date":datetime, "source":str}}]
        RETURNS:
            annotate_data: pandas.DataFrame
        """

        annotated_raw = []

        for row in tqdm(input_data):
            doc = self.nlp(row["text"])
            entities = [i.text for i in doc.ents]
            cab_ents = utils.cabinet_entities(row["date"], entities, self.cabinet_data)
            if cab_ents != [] :
                for i in cab_ents:
                    d = {"_id": row["_id"], 
                        "date": row["date"], 
                        "text":row["text"],
                        "source": row["source"]}
                    d.update(i)
                    annotated_raw.append(d)

        df_ent = pd.DataFrame(annotated_raw)

        plh = df_ent.groupby(["_id"]).agg({'cabinet_ents':lambda x: list(x), "date":"first", "text":"first", "source":"first"})
        df_ent_g = pd.DataFrame()
        df_ent_g["_id"] = list(plh.index)
        df_ent_g["cabinet_ents"] = list(plh["cabinet_ents"])
        df_ent_g["date"] = list(plh["date"])
        df_ent_g["source"] = list(plh["source"])
        df_ent_g["text"] = list(plh["text"])


        return df_ent_g

    def extend_data_datetime_vars(self, interim_data):

        """
        Adds year, mothm day vars to the dataframe

        PARAMS:
            interim_data:DataFrame - a dataframe with `date` column
        RETURNS:
            interim_data:DataFrame
        """

        interim_data["year"] = interim_data.apply(lambda x : x["date"].year, axis = 1)
        interim_data["month"] = interim_data.apply(lambda x : x["date"].month, axis = 1)
        interim_data["day"] = interim_data.apply(lambda x : x["date"].day, axis = 1)
        interim_data = interim_data.sort_values(by=["date"])
        
        return interim_data

    def extend_data_cabinet_vars(self, interim_data):

        """
        Adds cabinet vars: cabinet, cabinet_number, president

        PARAMS:
            interim_data:DataFrame - a dataframe with `date` column
        RETURNS:
            interim_data:DataFrame
        """

        pm = []
        cabinet_no = []
        prezident = []

        for row in tqdm(interim_data.date):
            cabinet = utils.resolve_cabinet(self.cabinet_data, row)
            plh = utils.prime_and_prez(cabinet)
            pm.append(plh[0])
            cabinet_no.append(plh[1])
            prezident.append(plh[2])

        interim_data["cabinet"] = pm
        interim_data["cabinet_no"] = cabinet_no
        interim_data["president"] = prezident

        return interim_data