# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/9/7 
# @function: the script is used to do something.
# @version : V1.0 
#
import os
import re
import threading
import time
import warnings
from pathlib import Path
from queue import Queue
import Levenshtein
from typing import Union, Any, List, Tuple, Pattern
from searchmanage import SpellCheck, SearchManage, Tools, AnalysisTools
# from searchmanage import DbpediaLookUp
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

REMOVE_WORDS = [":", ",", ".", "-", "|", "…", "(", ")", "&", "：", "[", "]", "{", "}", "!", "@", "?",
                "/", "\\", "#", "'", "*", "^", "%", "+", ";", "`"]


class JsonDataManage(object):
    """The class for manage of json data.

    :ivar self.json_: the json data for analysis
    """

    def __init__(self, json_file_path: Union[os.PathLike, str] = None):
        self.json_ = None
        if json_file_path:
            self.init_json(json_file_path)

    def set_json(self, json_: dict):
        """Set json data."""
        self.json_ = json_

    def get_json(self) -> dict:
        """Get json data."""
        return self.json_

    def init_json(self, json_file_path: Union[os.PathLike, str]) -> dict:
        """Init the json data from s json file.

        :param json_file_path: the path of json file

        :raise ValueError: read json file failed

        :return: json data read from json file.
        """
        json_file_path = Path(json_file_path).with_suffix(".json")
        try:
            with open(json_file_path, mode="r+", encoding="utf-8") as f:
                self.json_ = json.load(f)
        except Exception as e:
            print(e)
            raise ValueError(f"Read json file {json_file_path} failed.")
        return self.json_

    def write_json(self, json_file_path: Union[os.PathLike, str]):
        """write json data to a json file.

        :param json_file_path: the json file path where json data will be written

        :raise ValueError: write json file failed
        """
        try:
            with open(json_file_path, mode="w+", encoding="utf-8") as f:
                json.dump(self.json_, f, ensure_ascii=False)
        except Exception as e:
            print(e)
            raise ValueError(f"Write json file {json_file_path} failed.")

    def can_column_search(self, col_index: int) -> bool:
        """Judge whether a column data can be queried.

        See also:
            - get_column_property()

        :param col_index:
            the index of column data you want to judge

        :raise IndexError:
            col_index is greater than number of json data column

        :return:
            whether data can be queried
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
                    if key == "value":
                        re_.append("None")
                    else:
                        re_.append(["None"])
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

    def get_cell_data(self, i: int, j: int, key: str = "value") -> Union[str, dict, list]:
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
    def shape(self) -> Tuple[int, int]:
        """Get the shape of the table."""
        return self.json_["row"], self.json_["col"]

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

    def __init__(self, relative_path: str, csv_or_json_file: str = None):
        super().__init__()
        self.csv_data = None
        self.frame_ = None
        self.search_index = None
        self.file_type = None

        self.nlp = None
        self.relative_path = relative_path
        self.csv_or_json_file = None

        if csv_or_json_file is not None:
            self.init_file(csv_or_json_file)

    def init_file(self, file_name: str):
        self.csv_or_json_file = file_name
        if (self.csv_or_json_file.split("."))[1] == "csv":
            self.nlp = spacy.load("en_core_web_lg")
            self.file_type = "csv"
            self.init_csv()
            self.init_json_process()
        else:
            self.file_type = "json"
            self.init_json(self.relative_path + self.csv_or_json_file)

    def init_csv(self):
        self.frame_ = pd.read_csv(self.relative_path + self.csv_or_json_file)
        da, self.csv_data = Tools.read_csv(filename=self.relative_path + self.csv_or_json_file,
                                           is_header=True, out_data_t=True, is_print=True)
        del da
        self.json_ = {
            "isCompleted": False,
            "row": len(self.csv_data[0]),
            "col": len(self.csv_data),
            "keyColumnIndex": 0,
            "columnsType": [],
            "data": []
        }
        self.search_index = list()

    def init_json_process(self):
        print("Init json processing...")
        start = time.time()
        self.judge_columns_category()
        end = time.time()
        print("Cost time: %.3fs\n" % (end - start))

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
                "type": "",
                "PID": "",
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
            if NE_type + 1 >= literal_type and self.frame_[self.frame_.columns[col_index]].dtype == object:
                search_index.append(col_index)
                for csv_ in self.csv_data[col_index]:
                    value_dict = {
                        "isNone": False,
                        "value": None,
                        "correction": None,
                        "QIDs": None,
                        "Labels": None,
                        "IRIs": None,
                        "Descriptions": None,
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
        if len(search_index) == 0:
            self.json_["keyColumnIndex"] = 0
            return
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
        re_ = CSVPretreatment.spell_check_process(entities_1d, max_batch, check_time)
        re_ = Tools.list_back(re_, index_)
        # print(re_)
        j = 0
        for i in range(self.shape[1]):
            if self.can_column_search(i):
                self.set_column_data(re_[j], i)
                j += 1
        end = time.time()
        print("Cost time: %.3fs.\n" % (end - start))

    @staticmethod
    def spell_check_process(entities: list, max_batch: int = 50, check_time: int = 10) -> list:
        entities_pre1 = []
        entities_pre = []
        for entity in entities:
            temp_s = entity
            for rw in REMOVE_WORDS:
                temp_s = temp_s.replace(rw, " ")
            entities_pre1.append(temp_s)
        reg_ = re.compile(r"[A-Z][^A-Z 0-9]+")
        i = 0
        for entity in entities_pre1:
            i += 1
            entities_pre.append(CSVPretreatment.word_pretreatment(entity, reg_))
        for entity in entities_pre:
            if entity == " " or entity == "":
                entity = entities[i]
        # print(entities)
        # print(entities_pre)
        sp = SpellCheck(m_num=max_batch)
        # sp_a = SpellCheck(url_="https://www.ask.com/web", m_num=max_batch)
        # SpellCheck->"Bing"
        re_bing = CSVPretreatment.page_request(entities_pre, sp, check_time, mode="bing")
        # print(re_bing)
        # SpellCheck->"Ask"
        # re_ask = CSVPretreatment.page_request(entities_pre, sp_a, check_time, mode="ask")
        corrections_ = []
        # print(re_bing)
        reg_1 = re.compile(" - Wikipedia")
        reg_2 = re.compile(" - Wikidata")
        reg_3 = re.compile(" - Wikimedia Commons")
        for i in range(len(re_bing)):
            if len(re_bing[i]) == 0:
                corrections_.append([entities_pre[i], entities[i]])
                continue
            res_ = []
            for bing in re_bing[i]:
                bing = str(bing)
                if reg_1.findall(bing):
                    res_.append(bing.replace(" - Wikipedia", ""))
                if reg_2.findall(bing):
                    res_.append(bing.replace(" - Wikidata", ""))
                if reg_3.findall(bing):
                    res_.append(bing.replace(" - Wikimedia Commons", ""))
            re_bing[i], index_ = Tools.list_level(re_bing[i])
            del index_
            temp = ""
            for pos in re_bing[i]:
                t = pos
                for rw in REMOVE_WORDS:
                    t = t.replace(rw, " ")
                temp += (" " + t.lower() + " ")
            re_ = []
            for pos in temp.split(" "):
                if pos != "" and pos not in re_:
                    re_.append(pos)
            match_ = []
            for pos in entities_pre[i].split(" "):
                if pos != "":
                    match_.append(pos.lower())
            # print(re_)
            # print(match_)
            r_ = []
            for m in match_:
                r_t = []
                dis = []
                for r in re_:
                    dis.append(Levenshtein.distance(m, r))
                dis_sort_index = sorted(range(len(dis)), key=lambda k: dis[k])
                r_t.append(re_[dis_sort_index[0]])
                for in_ in dis_sort_index[1::]:
                    if dis[in_] - dis[dis_sort_index[0]] < 2:
                        r_t.append(re_[in_])
                    else:
                        break
                if len(r_t) > 10:
                    r_t = r_t[:10]
                r_.append(r_t)
            # print(r_)

            CSVPretreatment.words_combined(r_, res_, "", 0)
            if len(res_) > 50:
                res_ = res_[:50]
            res_.append(entities[i])
            res_.append(entities_pre[i])
            corrections_.append(res_)
        return corrections_

    @staticmethod
    def word_pretreatment(entity: str, reg_: Pattern) -> str:
        a = reg_.split(entity)
        a_ = reg_.findall(entity)
        temp = ""
        i = 0
        j = 0
        while i < len(a) and j < len(a_):
            if a[i] == "":
                temp += (a_[j] + " ")
            else:
                temp += (a[i] + a_[j] + " ")
            i += 1
            j += 1
        while i < len(a):
            temp += (a[i] + " ")
            i += 1
        while j < len(a_):
            temp += (a_[j] + " ")
            j += 1
        return temp.strip()

    @staticmethod
    def page_request(entities: list, sp: SpellCheck, check_time: int, mode: str = "bing") -> list:
        re_ = []
        none_ = []
        for i in range(len(entities)):
            re_.append([])
            none_.append(i)
        spell_ = entities
        for i in range(check_time):
            if mode == "bing":
                temp = sp.search_run(spell_, timeout=1000, block_num=2, function_=AnalysisTools.bing_page)
            elif mode == "ask":
                temp = sp.search_run(spell_, timeout=1000, block_num=2, function_=AnalysisTools.ask_page)
            else:
                raise KeyError("No mode = %s." % mode)
            none_t = []
            spell_t = []
            t = 0
            for j in none_:
                if len(temp[t]) == 0:
                    none_t.append(j)
                    spell_t.append(entities[j])
                else:
                    re_[j] = temp[t]
                t += 1
            spell_ = spell_t
            none_ = none_t
            if len(none_) == 0:
                break
            else:
                if i == check_time // 2:
                    print("Request too many times. Waiting for 10.0s.")
                    time.sleep(10.0)
        return re_

    @staticmethod
    def words_combined(t_: List[list], re_: list, s: str, index_: int):
        if index_ == len(t_):
            re_.append(s.strip())
            return
        for pos in t_[index_]:
            CSVPretreatment.words_combined(t_, re_, s + pos + " ", index_ + 1)
        return

    def wiki_search_process(self):
        """Search Process form text->IRIs using wikimedia API."""
        print("Wiki search processing...")
        start = time.time()
        entities_search = []
        entities_index = []
        for i in range(self.shape[1]):
            if self.can_column_search(i):
                entities_search.append(self.get_column_data(i, key="correction"))
                entities_index.append(i)

        sm = SearchManage(m_num=200)
        re_ = sm.search_run(entities_search, keys="all", timeout=1000, block_num=3, limit=50)
        res__ = []
        for i in range(len(entities_index)):
            # Set QIDs
            res_ = []
            for col in re_['id'][i]:
                r_ = []
                for da in col:
                    r_ += da
                r_ = CSVPretreatment.remove_repeat_entities(r_)
                res_.append(r_)
            self.set_column_data(res_, entities_index[i], key="QIDs")
            # Set IRIs
            res_iri = []
            for col in re_['url'][i]:
                r_iri = []
                for da in col:
                    r_iri += da
                r_iri = CSVPretreatment.remove_repeat_entities(r_iri)
                res_iri.append(r_iri)
            self.set_column_data(res_iri, entities_index[i], key="IRIs")
            res__.append(res_)
        end = time.time()
        print("Cost time: %.3fs.\n" % (end - start))

    def wiki_claims_process(self):
        start = time.time()
        print("Querying properties process...")
        ids_ = []
        ids_index = []
        key_col_index_ = 0
        j = 0
        for i in range(self.shape[1]):
            if self.can_column_search(i):
                ids_.append(self.get_column_data(i, key="QIDs"))
                ids_index.append(i)
                if i == self.key_column_index:
                    key_col_index_ = j
                j += 1
        sc = SearchManage(key="ids", m_num=200)
        # print(ids_)
        try:
            re_ = sc.search_run(ids_, timeout=1000, block_num=3,
                                keys=["properties", 'labels/en', 'descriptions/en'])
        except Exception as e:
            print(e)
            return
        # Set none-key columns Expansion and can search column labels& description
        for i in range(len(ids_index)):
            if i != key_col_index_:
                res = []
                for da in re_["properties"][i]:
                    dict_ = {
                        "Properties": da
                    }
                    res.append(dict_)
                self.set_column_data(res, ids_index[i], key="Expansion")
            self.set_column_data(re_['labels/en'][i], ids_index[i], key="Labels")
            self.set_column_data(re_['descriptions/en'][i], ids_index[i], key="Descriptions")
        # Set key column Expansion
        re_, index_ = Tools.list_level(re_["properties"][key_col_index_])
        ids_ = []
        ids_index_ = []
        res__ = []
        for r_ in re_:
            dict_ = dict()
            ids_index = []
            id_ = []
            for k_, v_ in r_.items():
                dict_[k_] = []
                for i in range(len(v_)):
                    dict_[k_].append(list(v_[i]))
                    if v_[i][0] is not None:
                        if v_[i][0] == "wikibase-entityid":
                            id_.append(v_[i][1])
                            ids_index.append(tuple([k_, i]))
            ids_.append(id_)
            ids_index_.append(ids_index)
            res__.append(dict_)
        s_ = sc.search_run(ids_, timeout=1000, block_num=2, keys="labels/en")
        for i in range(len(ids_index_)):
            for j in range(len(ids_index_[i])):
                res__[i][ids_index_[i][j][0]][ids_index_[i][j][1]].append(s_["labels/en"][i][j])
        res__ = Tools.list_back(res__, index_)
        res_ = []
        for da_ in res__:
            dict_ = {
                "Properties": da_,
                "AvgSimilarity": None,
                "Similarity": None,
                "PIDSimilarity": None,
                "QIDSimilarity": None,
                "LabelSimilarity": None
            }
            res_.append(dict_)
        self.set_column_data(res_, self.key_column_index, key="Expansion")
        self.json_["isCompleted"] = True
        end = time.time()
        print("Querying properties process successfully.\nCost time: %.3fs.\n" % (end - start))

    def csv_write_json(self):
        self.write_json(self.relative_path + (self.csv_or_json_file.split("."))[0] + ".json")

    @staticmethod
    def remove_repeat_entities(points_t: list) -> list:
        """Remove repeat entities.

        :param points_t:
            the list of entities which may exist repeat element

        :return:
            the list of entities without repeat entities
        """
        entities_ = []
        for entity in points_t:
            if entity not in entities_ and entity is not None:
                entities_.append(entity)
        return entities_


class PretreatmentManage(object):
    """The class is to manage CSV file to Json using multiprocess.

    :ivar files_queue: the queue of all files
    :ivar files_path: the path of all csv files

    :param file_path: the path of csv or json files
    """

    def __init__(self, file_path: str = None):
        self.files_queue = Queue()
        self.files_path = None
        if file_path is not None:
            self.init_queue(file_path)

    def init_queue(self, files_path: str):
        self.files_path = files_path
        while not self.files_queue.empty():
            self.files_queue.get()
        for home, dirs, files in os.walk(files_path):
            for file in files:
                if file.split(".")[1] == "csv":
                    self.files_queue.put(file)

    def init_and_bing_process(self, queue_f: Queue, queue_j: Queue, queue_c: Queue):
        js = JsonDataManage()
        csv = CSVPretreatment(self.files_path + "\\")
        while not queue_f.empty():
            cache = True
            f_ = queue_f.get()
            try:
                js.init_json(relative_ + "\\" + f_.split(".")[0] + ".json")
                if js.json_["isCompleted"]:
                    continue
            except ValueError:
                pass
            try:
                csv.init_file(f_)
                csv.correct_process(max_batch=50, check_time=10)
                csv.wiki_search_process()
            except Exception as e1:
                print(e1)
                cache = False
            finally:
                csv.write_json(self.files_path + "\\" + f_.split(".")[0] + ".json")
            if not cache:
                queue_f.put(f_)
            else:
                # print(csv.get_json())
                queue_j.put(csv.get_json())
                queue_c.put(f_)
        queue_f.task_done()

    def properties_process(self, queue_f: Queue, queue_j: Queue, queue_c: Queue):
        csv = CSVPretreatment(self.files_path + "\\")
        while True:
            j_ = queue_j.get()
            c_ = queue_c.get()
            # print(j_)
            csv.set_json(j_)
            cache = True
            try:
                csv.wiki_claims_process()
            except Exception as e1:
                print(e1)
                cache = False
            finally:
                csv.write_json(self.files_path + "\\" + c_.split(".")[0] + ".json")
            if not cache:
                queue_f.put(c_)
            else:
                # print(csv.get_json())
                print("------------------------------------")
                print("Remained entities: %d." % queue_f.qsize())
                print("------------------------------------")

    def run(self):
        queue_j = Queue()
        queue_c = Queue()
        p = threading.Thread(target=self.init_and_bing_process, args=(self.files_queue, queue_j, queue_c,))
        p.start()
        c = threading.Thread(target=self.properties_process, args=(self.files_queue, queue_j, queue_c,))
        c.start()
        p.join()
        c.join()


if __name__ == "__main__":
    _s_ = time.time()
    relative_ = "...\\tables"
    PM = PretreatmentManage(relative_)
    print(PM.files_queue.qsize())
    PM.run()
    _e_ = time.time()
    print("Cost time: %.3f" % (_e_ - _s_))
