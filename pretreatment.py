# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/9/7 
# @function: the script is used to do something.
# @version : V1.0 
#

from searchmanage import SpellCheck, SearchManage, Tools, DbpediaLookUp
import json
import pandas as pd

JSON_VALUE = {
    "isNone": False,
    "value": str,
    "correction": list,
    "QIDs": list,
    "Labels": list,
    "IRIs": list
}

JSON_COLUMN = {
    "canSearch": True,
    "column": list,
    "type": str
}

REMOVE_CHAR = [" ", ",", ":", ".", "-", "million", "→", "[", "]", "(",
               ")", "%", "year", "years", "day", "days", "january", "jan",
               "february", "feb", "march", "mar", "april", "apr", "may",
               "june", "jun", "july", "jun", "august", "aug", "september", "sep",
               "october", "oct", "november", "nov", "december", "dec"]

test_path = "data/Round1/HardTablesR1/Valid/tables/4F2U2YS9.csv"
test_path1 = "data/Round2/HardTablesR2/Valid/tables/CN14ET6Z.csv"
test_path2 = "data/Round2/ToughTablesR2-WD/Test/tables/5HD27KI3.csv"
test_path3 = "data/Round2/ToughTablesR2-WD/Test/tables/6JTF8CCT.csv"
test_path4 = "data/Round2/ToughTablesR2-WD/Test/tables/7QNYGYI7.csv"


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
        self.frame_ = pd.read_csv(self.relative_path + self.csv_or_json_file)
        da, self.csv_data = Tools.read_csv(filename=self.relative_path + self.csv_or_json_file,
                                           is_header=True, out_data_t=True, is_print=True)
        del da
        self.json_ = {
            "isCompeted": False,
            "row": len(self.frame_),
            "col": len(self.frame_.columns),
            "keyColumnIndex": 0,
            "data": list()
        }
        self.search_index = list()

    def init_json(self):
        pass

    def init_json_process(self):
        i = 0
        for c in self.frame_.columns.values:
            column_dict = {
                "canSearch": False,
                "type": str,
                "column": []
            }
            # number type
            if self.frame_[c].dtype != object:
                column_dict["type"] = "number"
                column_dict["column"] = self.csv_data[i]
            # object type
            else:
                # still no search entities
                if CSVPretreatment.is_no_search_entities(self.csv_data[i]):
                    column_dict["type"] = "no-search"
                    column_dict["column"] = self.csv_data[i]
                # search entities
                else:
                    column_dict["type"] = "string"
                    column_dict["canSearch"] = True
                    column_dict["column"] = []
                    for csv_ in self.csv_data[i]:
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
            self.json_["data"].append(column_dict)
            i += 1

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
    c_ = CSVPretreatment("data/Round2/ToughTablesR2-WD/Test/tables/", "5HD27KI3.csv")
    print(c_.json_)
