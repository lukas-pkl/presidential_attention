#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main script to annotate texts

Created: 2021 12 23
Author: lukasp
"""

from datetime import datetime

from pymongo import MongoClient

import constants
from annotator import LTAnnotator
from utils import LTDoc

mongo = MongoClient(constants.mongo_conn_string)
mongo_lt_source_col = mongo[constants.mongo_db][constants.mongo_lt_col]
mongo_lt_destination_col = mongo[constants.mongo_db][constants.mongo_lt_annotated_col]

anno = LTAnnotator()

begin = 1999
end = datetime.now().year+1

years = [i for i in range(begin,end)]

for y in years:
    print(y)
    query = {"date" : {"$gte": datetime(y, 1, 1), "$lt":datetime(y+1, 1, 1)}}
    cursor = mongo_lt_source_col.find(query)
    input_data = [i for i in cursor]
    print(f"Records for this year {len(input_data)}")
    if len(input_data)>0:

        df = anno.detect_entities(input_data)
        df2 = anno.extend_data_datetime_vars(df)
        df3 = anno.extend_data_cabinet_vars(df2)
        
        validated_data = [LTDoc(**i) for i in df3.to_dict(orient="records")] #Testing for schema compliance
        validated_data2 = [i.dict() for i in validated_data]
        
        mongo_lt_destination_col.insert_many(validated_data2)
