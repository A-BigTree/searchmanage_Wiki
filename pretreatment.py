# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/9/7 
# @function: the script is used to do something.
# @version : V1.0 
#
import time
import warnings
import Levenshtein
from typing import Union, Any, List
from searchmanage import SpellCheck, SearchManage, Tools, DbpediaLookUp
# from searchmanage import AnalysisTools
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
    "IRIs": list,
    "Descriptions": list,
    "Types": list,
    "Expansion": Any,
    "Target": Any
}

JSON_COLUMN = {
    "canSearch": bool,
    "PID": str,
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
        if len(json_file_path.split(".")) == 0:
            json_file_path = json_file_path + ".json"
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

    def can_column_search(self, col_index: int) -> bool:
        """Judge whether a column data can be queried.

        See also:
            - get_column_property()

        :param col_index:
            the index of column data you want to judge

        :raise IndexError: col_index is greater than number of json data column

        :return: whether data can be queried
        """
        return self.get_column_property(col_index, "canSearch")

    def get_column_data(self, col_index: int, key: str = "value") -> list:
        """Get a list of key word data from a column.

        :param col_index:
            the index of column data you want to get
        :param key:
            key word want to get. 'value', 'correction', 'QIDs',
            'Labels', 'IRIs', 'Descriptions', 'Types', 'Expansion'
            or 'Target'. Default: 'value'

        :raise IndexError:
            col_index is greater than number of json data column

        :return:
            the data of a column from its key word. If the column
            can not be queried, it will return the original data.
        """
        if col_index >= self.shape[1]:
            raise IndexError("Json data only has %d columns." % self.json_["col"])
        if key not in JSON_VALUE.keys():
            warnings.warn("No key = %s in json data." % key)
            key = "value"
        if self.can_column_search(col_index):
            re_ = []
            for da_ in self.json_["data"][col_index]["column"]:
                if da_["isNone"]:
                    re_.append("None")
                else:
                    re_.append(da_[key])
            return re_
        else:
            return self.json_["data"][col_index]["column"]

    def set_column_data(self, data: List[Any], col_index: int, key: str = "correction"):
        """Set data of key word to a column.

        :param data:
            a list of data you will set
        :param col_index:
            index of col_index
        :param key:
            key word want to get. 'value', 'correction', 'QIDs',
            'Labels', 'IRIs' , 'Descriptions', 'Types', 'Expansion'
            or 'Target'. Default: 'correction'

        :raise IndexError:
            col_index is greater than number of json data column
        """
        if col_index >= self.shape[1]:
            raise IndexError("Json data only has %d columns." % self.json_["col"])
        if key not in JSON_VALUE.keys():
            warnings.warn("No key = %s in json data." % key)
            key = "correction"
        if self.can_column_search(col_index):
            if len(data) != len(self.json_["data"][col_index]["column"]):
                warnings.warn("The length (%d) of data isn't equal to the length(%d) of column(%d)." %
                              (len(data), len(self.json_["data"][col_index]["column"]), col_index))
            for i in range(len(data)):
                self.json_["data"][col_index]["column"][i][key] = data[i]
        else:
            warnings.warn("The index %d of column can not be queried." % col_index)

    def get_column_property(self, col_index: int, key: str = "PID") -> Union[str, list, bool]:
        """Get column property in key word.

        :param col_index:
            the index of column data you want to judge
        :param key:
            key word want to get. 'canSearch', 'PID', 'type'
            or 'column'. Default: 'PID'.

        :raise IndexError: col_index is greater than number of json data column

        :return:
            the data from key word
        """
        if col_index >= self.json_["col"]:
            raise IndexError("Json data only has %d columns." % self.json_["col"])
        if key not in JSON_COLUMN.keys():
            warnings.warn("No key = %s in json column property." % key)
            key = "PID"
        return self.json_["data"][col_index][key]

    def set_column_property(self, data, col_index: int, key: str = 'PID'):
        """Set data of key word to a column property.

        :param data:
            the data you will set
        :param col_index:
            the index of column data you want to judge
        :param key:
            key word want to get. 'canSearch', 'PID', 'type'
            or 'column'. Default: 'PID'.
        """
        if col_index >= self.json_["col"]:
            raise IndexError("Json data only has %d columns." % self.json_["col"])
        if key not in JSON_COLUMN.keys():
            warnings.warn("No key = %s in json column property." % key)
            key = "PID"
        self.json_["data"][col_index][key] = data

    def get_cell_data(self, i: int, j: int, key: str = "value") -> Union[str, list]:
        """Get the date of key word from index(i, j).

        :param i:
            index of row
        :param j:
            index of column
        :param key:
            key word want to get. 'value', 'correction', 'QIDs',
            'Labels', 'IRIs' 'Types', 'Expansion' or 'Target'.
            Default: 'value'

        :raise IndexError:
            Index is greater than the number of row or column

        :return:
            the data of a cell from its key word. If the column
            can not be queried, it will return the original data.
        """
        if i >= self.shape[0] or j >= self.shape[1]:
            raise IndexError("Index is greater than the number of row or column.")
        if key not in JSON_VALUE.keys():
            warnings.warn("No key = %s in json data." % key)
            key = "value"
        if self.can_column_search(j):
            if self.json_["data"][j]["column"][i]["isNone"]:
                return "None"
            return self.json_["data"][j]["column"][i][key]
        return self.json_["data"][j]["column"][i]

    def set_cell_data(self, data: Any, i: int, j: int, key: str = "correction"):
        """Set a cell date in key word to index(i, j).

        :param data:
            the data you want to set
        :param i:
            index of row
        :param j:
            index of column
        :param key:
            key word want to get. 'value', 'correction', 'QIDs',
            'Labels', 'IRIs' 'Types', 'Expansion' or 'Target'.
            Default: 'correction'

        :raise IndexError:
            Index is greater than the number of row or column
        """
        if i >= self.shape[0] or j >= self.shape[1]:
            raise IndexError("Index is greater than the number of row or column.")
        if key not in JSON_VALUE.keys():
            warnings.warn("No key = %s in json data." % key)
            key = "correction"
        if self.can_column_search(j):
            self.json_["data"][j]["column"][i][key] = data
        else:
            warnings.warn("The index %d of column can not be queried." % j)

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
    def column_pids(self) -> list:
        """Get the columns' pid."""
        re_ = []
        for col_ in self.json_["data"]:
            re_.append(col_["PID"])
        return re_

    @property
    def data(self) -> list:
        """Get data details."""
        return self.json_["data"]

    def __str__(self):
        return "[row %d× col %d]\nKey column index: %d\nColumns category: " % (
            self.shape[0], self.shape[1], self.key_column_index) + self.column_types.__str__()


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
        print("Init json processing...")
        start = time.time()
        self.judge_columns_category()
        end = time.time()
        print("Cost time: %.3fs" % (end - start))

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
                "PID": str,
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
                        "IRIs": None,
                        "Types": None,
                        "Expansion": None,
                        "Target": None
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

    def correct_process(self, max_batch: int = 50, check_time: int = 10):
        """The process of word correction.

        :param max_batch:
            max number of entity in once batch
            for word correction. Default: 50
        :param check_time:
            check time. Default: 10
        """
        print("Spell check processing...")
        start = time.time()
        entities_search = []
        for i in range(self.shape[1]):
            if self.can_column_search(i):
                entities_search.append(self.get_column_data(i))
        entities_1d, index_ = Tools.list_level(entities_search)
        re_ = []
        none_ = []
        for i in range(len(entities_1d)):
            re_.append([])
            none_.append(i)
        sp = SpellCheck(m_num=max_batch)
        # sp_a = SpellCheck(url_="https://www.ask.com/web", m_num=max_batch)
        spell_ = entities_1d
        temp: list = []
        for i in range(check_time):
            temp = sp.search_run(spell_, timeout=1000, block_num=2)
            # temp = sp_a.search_run(spell_, timeout=1000, block_num=2, function_=AnalysisTools.ask_analysis)
            none_t = []
            spell_t = []
            t = 0
            for j in none_:
                if temp[t] == spell_[t]:
                    none_t.append(j)
                    spell_t.append(entities_1d[j])
                else:
                    re_[j].append(temp[t])
                t += 1
            spell_ = spell_t
            none_ = none_t
        for i in range(len(none_)):
            re_[none_[i]].append(temp[i])
        re_ = Tools.list_back(re_, index_)
        j = 0
        for i in range(self.shape[1]):
            if self.can_column_search(i):
                self.set_column_data(re_[j], i)
                j += 1
        end = time.time()
        print("Cost time: %.3fs" % (end - start))

    def wiki_search_process(self):
        """Search Process form text->IRIs using wikimedia API."""
        print("Wiki search processing...")
        start = time.time()
        entities_search = []
        for i in range(self.shape[1]):
            if self.can_column_search(i):
                entities_search.append(self.get_column_data(i, key="correction"))
        sm = SearchManage(m_num=50)
        re_ = sm.search_run(entities_search, keys="all", timeout=1000, block_num=3, limit=50)
        j = 0
        for i in range(self.shape[1]):
            if self.can_column_search(i):
                self.set_column_data(re_["id"][j], i, key="QIDs")
                self.set_column_data(re_["url"][j], i, key="IRIs")
                self.set_column_data(re_["label"][j], i, key="Labels")
                self.set_column_data(re_["description"][j], i, key="Descriptions")
            j += 1
        end = time.time()
        print("Cost time: %.3fs" % (end - start))


class PretreatmentManage(object):
    pass


if __name__ == "__main__":
    # c_ = CSVPretreatment(
    # "D:\\王树鑫\\Learning\\Kcode实验室\\SemTab2022\\Code\\SEUTab\\data\\Round2\\ToughTablesR2-WD\\Test\\tables\\",
    # "5HD27KI3.csv")
    # print(c_.json_)
    # nlp = spacy.load('en_core_web_sm')
    # print(nlp.pipe_labels)
    print([] + [4, 5, 6])
