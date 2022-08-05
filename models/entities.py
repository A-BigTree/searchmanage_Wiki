# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/7/20
# @function: the class for json data of analysis and a single entity
# @version : V0.4.5
#

from typing import Union, List
import random
from warnings import warn
import requests
from requests import ReadTimeout, Response
from ..tools import AnalysisTools
from ..tools import Tools
from ..tools.Tools import AGENTS_


class RequestAnalysis(object):
    """class of request data or a string and its analysis data.

    Use you own analysis function to analysis json data. Of course, you can
    use some simple function to analysis json data from wikidata API in
    searchmanage.tools.AnalysisTools.

    :ivar __request(Response): request data
    :ivar __ready_(bool): record whether json data is ready
    :ivar __analysis(Union[dict, list, str,None]): result of analysis for json data
    """

    def __init__(self):
        self.__request = None
        self.__ready_ = False
        self.__analysis = None

    def set_request(self, request_: Union[Response, str, dict] = None):
        """Set request data. And if it's set successfully, then turn ready_ True.

        :param request_: request data you want to set
        :return: None
        """
        if request_ is not None:
            self.__ready_ = True
        self.__request = request_

    def set_analysis(self, analysis_: Union[None, dict, list]):
        """Set analysis data you get.

        :param analysis_: analysis data you want to set
        :return: None
        """
        self.__analysis = analysis_

    def run_analysis(self, function, *args, **kwargs) -> Union[list, dict, None]:
        """Analysis the request data or a str using your own function.

        :param function: your own analysis function
        :param args: you own function's parameters which format is tuple
        :param kwargs: you own function's parameters which format is dict
        :return: result of analysis json data or a string
        """
        if self.__ready_ is not True:
            raise ValueError("Request data is not ready.")
        if self.__analysis is not None:
            self.__analysis = None
        try:
            self.__analysis = function(self.__request, *args, **kwargs)
        except ValueError:
            raise ValueError("Analysis of Response error.")
        except Exception as e:
            print(e)
        return self.__analysis

    @property
    def get_request(self) -> Union[Response, str, dict]:
        """Get json data or a string.

        :return: json data or a string
        """
        return self.__request

    @property
    def get_analysis(self) -> Union[None, list, dict]:
        """Get analysis result.

        :return: analysis result
        """
        return self.__analysis

    @property
    def ready(self) -> bool:
        """Get whether json data is ready.

        :return: whether json data is ready
        """
        return self.__ready_

    def clear(self):
        """Init the object of this class.

        :return: None
        """
        self.__request = None
        self.__analysis = None
        self.__ready_ = False


class Entities(RequestAnalysis):
    """Class of a single entity as an element in next querying using multithread.

        Store entity's based information like its index, entities text or ids in
        searching, and  search parameters.

        :ivar __index(int):
            store entity's index for restoring the
            correct location after multithreading
        :ivar __entities(list):
            entities text or ids into searching
        :ivar __params(Union[dict, str]):
            search parameters in querying
        :ivar __Request(Union[dict, str, None]):
            json data
        :ivar __ready_(bool):
            record whether json data is ready
        :ivar __analysis(Union[dict, list, str,None]):
            result of analysis for json data
    """

    def __init__(self):
        super().__init__()
        self.__index = 0
        self.__entities = None
        self.__params = None

    def set_entities(self, index: int, entities: Union[list, None]):
        """Set entity's index and entities.

        :param index: entity's index
        :param entities: a list of entities text or ids in querying
        :return: None
        """
        self.__index = index
        self.__entities = entities

    def init_params(self, search_entities: str, key: str, **kwargs):
        """Init parameters in searching according to Wikimedia API format.

        :param search_entities: a string of entities text or ids
        :param key:
            a string of using 'search' in querying entities
            text, or 'ids' in querying entities ids
        :param kwargs:
            set query parameters you want, you can see more
            details in ReadMe.md which explains different
            keywords you can reset in two different key.
        :return: None

        See more details at:
            - https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities
            - https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
        """
        if key == 'search':
            self.__params = {
                'search': search_entities,
                'action': 'wbsearchentities',
                'format': 'json',
                'language': 'en',
                'type': 'item',
                'limit': 10,
                'strictlanguage': None,
                'continue': None,
                'props': None
            }
        elif key == 'ids':
            self.__params = {
                'ids': search_entities,
                'action': 'wbgetentities',
                'format': 'json',
                'languages': 'en',
                'redirects': None,
                'sites': None,
                'title': None,
                'props': None,
                'languagefallback': None,
                'normalize': None,
                'sitefilter': None
            }
        else:
            raise ValueError("No key = %s" % key)

        if kwargs is not None:
            for k_, v_ in kwargs.items():
                try:
                    self.__params[k_] = v_
                except KeyError:
                    warn("Search __params has not key = %s" % k_)

    def set_params(self, params: Union[dict, str]):
        """Set any querying parameters you want.

        You can use it when overriding method.

        :param params: a parameter you want
        :return: None
        """
        self.__params = params

    def set_search_params(self, **kwargs):
        """Reset parameters in searching according to Wikimedia API format.

        :param kwargs:
            set query parameters you want, you can see more
            details in ReadMe.md which explains different
            keywords you can reset in two different key.
        :return: None

        See more details at:
            - https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities
            - https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
        """
        for k_, v_ in kwargs.items():
            try:
                self.__params[k_] = v_
            except KeyError:
                warn("Search params has not key = %s" % k_)

    def entity_get_wiki(self, url: str, timeout: float):
        """Sent request to url gave for **json** data.

        :param url:
            a string of url for sending request
        :param timeout:
            when request is (timeout) seconds overtime, it will raise ValueError
        :return: None
        :raise ValueError: any exception
        """
        self.clear()

        try:
            get_ = requests.get(url=url,
                                params=self.__params,
                                headers={'User-Agent': random.choice(AGENTS_)},
                                timeout=timeout)
        except ReadTimeout:
            raise ValueError("Request time is over %fs." % timeout)
        except Exception as e:
            print(e)
            raise ValueError

        self.set_request(get_)

    def correct_id_repeat(self):
        """Correct duplicate entity errors.

        Notes:
            It was used after analysis.

        See also:
            - Tools.repeat_entities: Record repeating entities in list.
            - Tools.repeat_entities_back: Restore duplicate entity mapping.

        :return: None
        """
        if not self.ready:
            warn("Search data is not ready")
        index = Tools.repeat_entities(self.__entities)
        if len(index) == 0 or self.get_analysis is None:
            return
        self.set_analysis(Tools.repeat_entities_back(self.get_analysis, index))

    def wiki_json_analysis(self, key: str, keys: Union[str, List[str], None]) -> Union[list, dict, None]:
        """Analysis json data from Wikidata API.

        :param key: 'search' or 'ids'
        :param keys: analysis keys, more details seeing in ReadMe.md
        :return:
            If key is 'search', it will return a dict like {'ids':
            ['Q3551770', 'Q4430597']}. If key is 'ids', it will
            return a list[dict] like [{'P31':['Q20643955']}, {
            'P31': ['Q5']}].

        See also:
            - AnalysisTools.search_analysis
            - AnalysisTools.entities_analysis
            - correct_id_repeat
        """
        if not self.ready:
            return None
        if key == 'search':
            self.run_analysis(function=AnalysisTools.search_analysis, keys=keys)
        elif key == 'ids':
            self.run_analysis(function=AnalysisTools.entities_analysis, keys=keys)
            self.correct_id_repeat()

        else:
            warn("No action:%s" % key)
        return self.get_analysis

    def request_analysis(self, key: str, function, *args, **kwargs):
        """Analysis json data from Wikidata API using your own function.

        :param key: 'search' or 'ids'
        :param function: your own analysis function
        :param args: you own function's parameters which format is tuple
        :param kwargs: you own function's parameters which format is dict
        :return: analysis result

        See also:
            - JsonAnalysis().run_analysis
            - correct_id_repeat
        """
        if not self.ready:
            return None
        if key == 'ids':
            self.run_analysis(function=function, *args, **kwargs)
            self.correct_id_repeat()
        else:
            self.run_analysis(function=function, *args, **kwargs)
        return self.get_analysis

    @property
    def get_index(self) -> int:
        """Get entity's index."""
        return self.__index

    @property
    def get_entities(self) -> Union[list, None]:
        """Get entity's list of test or ids."""
        return self.__entities

    @property
    def get_params(self) -> Union[dict, str, None]:
        """Get entity's querying parameter."""
        return self.__params
