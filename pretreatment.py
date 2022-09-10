# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/9/7 
# @function: the script is used to do something.
# @version : V1.0 
#

import Levenshtein
from searchmanage import SpellCheck, SearchManage, Tools, DbpediaLookUp
import json
import pandas as pd
import spacy

JSON_FORMAT = {
    "isCompeted": bool,
    "row": int,
    "col": int,
    "keyColumnIndex": int,
    "columnsType": list,
    "data": list
}

JSON_VALUE = {
    "isNone": bool,
    "value": str,
    "correction": list,
    "QIDs": list,
    "Labels": list,
    "IRIs": list
}

JSON_COLUMN = {
    "canSearch": bool,
    "column": list,
    "type": str
}

LABEL_ = {'PERSON': 0, 'NORP': 0, 'FAC': 0, 'ORG': 0, 'GPE': 0, 'LOC': 0,
          'PRODUCT': 0, 'EVENT': 0, 'WORK_OF_ART': 0, 'LAW': 0, 'LANGUAGE': 0,
          'DATE': 0, 'TIME': 0, 'PERCENT': 0, 'MONEY': 0, 'QUANTITY': 0,
          'ORDINAL': 0, 'CARDINAL': 0}

REMOVE_CHAR = [" ", ",", ":", ".", "-", "million", "→", "[", "]", "(",
               ")", "%", "year", "years", "day", "days", "january", "jan",
               "february", "feb", "march", "mar", "april", "apr", "may",
               "june", "jun", "july", "jun", "august", "aug", "september", "sep",
               "october", "oct", "november", "nov", "december", "dec"]


class CSVPretreatment(object):
    """The class for manage of pretreatment.

    :ivar relative_path: the relative path of csv file
    :ivar csv_or_json_file: the name of csv file
    :ivar csv_data: data read from csv file
    :ivar frame_: the DataFrame of csv data
    :ivar json_: the result of json data
    :ivar search_index: the index of column for word correcting and search
    """

    def __init__(self, relative_path: str, csv_or_json_file: str):
        self.csv_data = None
        self.frame_ = None
        self.json_ = None
        self.search_index = None

        self.nlp = spacy.load("en_core_web_sm")
        self.relative_path = relative_path
        self.csv_or_json_file = csv_or_json_file

        if (csv_or_json_file.split("."))[1] == "csv":
            self.file_type = "csv"
            self.init_csv()
            self.init_json_process()
        else:
            self.file_type = "json"
            self.init_json()

    def init_csv(self):
        # self.frame_ = pd.read_csv(self.relative_path + self.csv_or_json_file)
        da, self.csv_data = Tools.read_csv(filename=self.relative_path + self.csv_or_json_file,
                                           is_header=True, out_data_t=True, is_print=True)
        del da
        self.json_ = {
            "isCompeted": False,
            "row": len(self.csv_data[0]),
            "col": len(self.csv_data),
            "keyColumnIndex": 0,
            "columnsType": list(),
            "data": list()
        }
        self.search_index = list()

    def init_json(self):
        pass

    def init_json_process(self):
        self.judge_columns_category()

    @staticmethod
    def is_no_search_entities(entities: list) -> bool:
        flag = 0
        pos: str
        for pos in entities:
            if pos is None or pos == "":
                flag += 1
                continue
            temp = pos.lower()
            for re_ in REMOVE_CHAR:
                temp = temp.replace(re_, "")
            if temp.isdigit():
                flag += 1
        if flag >= len(entities) / 2:
            return True
        else:
            return False

    def judge_columns_category(self):

        search_index = []
        # judge all columns type
        for col_index in range(len(self.csv_data)):
            label_ = LABEL_.copy()
            column_dict = {
                "canSearch": True,
                "type": str,
                "column": []
            }
            none_ = 0
            for cell in self.csv_data[col_index]:
                if cell is None or cell == "":
                    none_ += 1
                    continue
                doc = self.nlp(cell)
                for entity in doc.ents:
                    label_[entity.label_] += 1
            NE_type = label_['PERSON'] + label_['NORP'] + label_['FAC'] + label_['ORG'] \
                      + label_['GPE'] + label_['LOC'] + label_['PRODUCT'] + label_['EVENT'] \
                      + label_['WORK_OF_ART'] + label_['LAW'] + label_['LANGUAGE']
            literal_type = label_['DATE'] + label_['TIME'] + label_['PERCENT'] + label_['MONEY'] \
                           + label_['QUANTITY'] + label_['ORDINAL'] + label_['CARDINAL'] + none_
            type_ = max(label_, key=lambda k: label_[k])
            self.json_["columnsType"].append(type_)
            column_dict["type"] = type_
            if NE_type >= literal_type:
                search_index.append(col_index)
                for csv_ in self.csv_data[col_index]:
                    value_dict = {
                        "isNone": False,
                        "value": None,
                        "correction": None,
                        "QIDs": None,
                        "Labels": None,
                        "IRIs": None
                    }
                    # csv cell data is None
                    if csv_ is None or csv_ == "":
                        value_dict["isNone"] = True
                    # normal csv cell data
                    else:
                        value_dict["value"] = csv_
                    column_dict["column"].append(value_dict)

            else:
                column_dict["canSearch"] = False
                column_dict["column"] = self.csv_data[col_index]
            self.json_["data"].append(column_dict)

        # find key column
        key_select = search_index[0]
        key_select_score = []
        uniqueness_score = 0
        for i in search_index:
            only_col_list = []
            for cell in self.csv_data[i]:
                flag = True
                for only_cell in only_col_list:
                    if Levenshtein.distance(cell, only_cell) <= 2:
                        flag = False
                        break
                if flag:
                    only_col_list.append(cell)
            key_select_score.append(len(only_col_list))
            if len(only_col_list) > uniqueness_score:
                uniqueness_score = len(only_col_list)
                key_select = i
        self.json_["keyColumnIndex"] = key_select

    def correct_process(self):
        pass

    def search_process(self):
        pass

    def write_json(self):
        pass


class PretreatmentManage(object):
    pass


class JsonDataManage(object):
    pass


if __name__ == "__main__":
    c_ = CSVPretreatment(
        "D:\\王树鑫\\Learning\\Kcode实验室\\SemTab2022\\Code\\SEUTab\\data\\Round2\\ToughTablesR2-WD\\Test\\tables\\",
        "5HD27KI3.csv")
    print(c_.json_)
    # nlp = spacy.load('en_core_web_sm')
    # print(nlp.pipe_labels)
