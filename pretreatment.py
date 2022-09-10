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
# import pandas as pd
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


class JsonDataManage(object):
    """The class for manage of json data.

    :ivar self.json_: the json data for analysis
    """

    def __init__(self):
        self.json_ = None

    def set_json(self, json_: dict):
        """Set json data."""
        self.json_ = json_

    def get_json(self) -> dict:
        """Get json data."""
        return self.json_

    def init_json(self, json_file_path: str) -> dict:
        """Init the json data from s json file.

        :param json_file_path: the path of json file

        :raise ValueError: read json file failed

        :return: json data read from json file.
        """
        try:
            with open(json_file_path, mode="r+", encoding="utf-8") as f:
                self.json_ = json.load(f)
        except Exception as e:
            print(e)
            raise ValueError("Read json file %s failed." % json_file_path)
        return self.json_

    def write_json(self, json_file_pah: str):
        """write json data to a json file.

        :param json_file_pah: the json file path where json data will be written

        :raise ValueError: write json file failed
        """
        try:
            with open(json_file_pah, mode="w+", encoding="utf-8") as f:
                json.dump(self.json_, f)
        except Exception as e:
            print(e)
            raise ValueError("Write json file %s failed." % json_file_pah)

    def get_column_values(self, col_index: int) -> list:
        """Get a list original values from a column.

        :param col_index: the index of column data you want to get

        :raise IndexError: col_index is greater than number of json data column
        """
        if col_index >= self.json_["col"]:
            raise IndexError("Json data only has %d columns." % self.json_["col"])

        if self.json_["data"][col_index]["canSearch"]:
            re_ = []
            for da_ in self.json_["data"][col_index]["column"]:
                if da_["isNone"]:
                    re_.append("None")
                else:
                    re_.append(da_["value"])
        else:
            return self.json_["data"][col_index]["column"]

        return list()

    @property
    def shape(self) -> tuple:
        """Get the shape of the table."""
        return tuple([self.json_["row"], self.json_["col"]])

    @property
    def key_column_index(self) -> int:
        """Get the index of key column."""
        return self.json_["keyColumnIndex"]

    @property
    def column_types(self) -> list:
        """Get columns' type."""
        return self.json_["columnsType"]

    @property
    def data(self) -> list:
        """Get data details."""
        return self.json_["data"]


class CSVPretreatment(JsonDataManage):
    """The class for manage of csv file pretreatment.

    :ivar relative_path: the relative path of csv file
    :ivar csv_or_json_file: the name of csv file
    :ivar csv_data: data read from csv file
    :ivar frame_: the DataFrame of csv data
    :ivar json_: the result of json data
    :ivar search_index: the index of column for word correcting and search
    :ivar nlp: the nlp model loading from spacy
    """

    def __init__(self, relative_path: str, csv_or_json_file: str):
        super().__init__()
        self.csv_data = None
        self.frame_ = None
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
            self.init_json(self.relative_path + self.csv_or_json_file)

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

    def init_json_process(self):
        self.judge_columns_category()

    @staticmethod
    def is_no_search_entities(entities: list) -> bool:
        """Judge whether csv data columns can be searched."""
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
        """Judge the category of all tables columns."""
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


class PretreatmentManage(object):
    pass


if __name__ == "__main__":
    c_ = CSVPretreatment(
        "D:\\王树鑫\\Learning\\Kcode实验室\\SemTab2022\\Code\\SEUTab\\data\\Round2\\ToughTablesR2-WD\\Test\\tables\\",
        "5HD27KI3.csv")
    # print(c_.json_)
    # nlp = spacy.load('en_core_web_sm')
    # print(nlp.pipe_labels)
