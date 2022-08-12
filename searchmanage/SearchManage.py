# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/7/20
# @function: the class of using API
# @version : V0.4.5
#

import re
import sys
from typing import Union, List
from warnings import warn
from queue import Queue
from .models import EntitiesSearch, Entities
from .tools import Tools, AnalysisTools
from SPARQLWrapper import SPARQLWrapper
import wikipedia

SEARCH_WIKI = {
    'search': None,
    'action': 'wbsearchentities',
    'format': 'json',
    'language': 'en',
    'type': 'item',
    'limit': 10,
    'strictlanguage': None,
    'continue': None,
    'props': None
}
"""The format of parameter when key is 'search'."""

IDS_WIKI = {
    'ids': None,
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
"""The format of parameter when key is 'ids'."""

URL_WIKI = "https://www.wikidata.org/w/api.php"
"""Url of wikidata API."""

URL_BING = "https://www.bing.com/search"
"""Url of Bing Search."""

PARAM_BING_QUERY = {
    "q": "%s site:wikidata.org",
    "setlang": "en-us"
}
"""The format of parameter using in Bing Search."""

SPARQL_WIKI = """
SELECT ?item ?itemLabel 
WHERE 
{
?item wdt:P31 wd:%s.
SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""
"""Default sparql format string."""

URL_SPARQL_WIKI = "https://query.wikidata.org/sparql"
"""Default SPARQL endpoint's URI."""

AGENT_SPARQL_WIKI = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
"""Agent in SPARQL."""

PARAM_BING_SPELL_CHECK = {
    "q": None
}
"""Parameter format in Bing Spell Check."""

URL_DBPEDIA_SEARCH = "https://lookup.dbpedia.org/api/search"
"""URL of Dbpedia search API."""

URL_DBPEDIA_PREFIX = "https://lookup.dbpedia.org/api/prefix"
"""URL of Dbpedia prefix search API."""

PARAM_DBPEDIA_QUERY = {
    "query": None,
    "label": None,
    "comment": None,
    "category": None,
    "typeName": None,
    "maxResults": 10,
    "format": "json",
    "minRelevance": None
}
"""Parameters using in Dbpedia look up."""


class SearchManage(EntitiesSearch):
    """Class of querying with Wikidata API using multithread.

    In this class, you can set the number of threading for querying.
    And you can put N-dimensional list of entities' text or ids in
    it, so you can query a number of entity in one time. What's more,
    each entity can get results at right position and right dimension
    in the list.

    Example
    -------
        >>> search_m1 = SearchManage(key='search', m_num=10)
        >>> points1 = [['SEU'], ['computer', 'game', 'computer games'], ['paper', 'comic', 'comic books']]
        >>> print(points1)
        [['SEU'], ['computer', 'game', 'computer games'], ['paper', 'comic', 'comic books']]

        >>> re1 = search_m1.search_run(points=points1, keys='id')
        Entities:7.Threading number:10.
        Querying successfully.
        Cost time:0.889532s.

        >>> print(re1)
        {'id': [[['Q3551770', 'Q4430597', 'Q1476733', 'Q405915', 'Q7455033', 'Q69513094', 'Q37481985',
        'Q29834566', 'Q98770548', 'Q23665157']], [['Q68', 'Q5157408', 'Q11202952', 'Q74058411', 'Q444
        195', 'Q7397', 'Q7889', 'Q21198', 'Q250', 'Q32566'], ['Q7889', 'Q11410', 'Q13406554', 'Q223930'
        , 'Q189936', 'Q16510064', 'Q723187', 'Q55524865', 'Q1493033', 'Q19862498'], ['Q4485157', 'Q2990
        909', 'Q5157439', 'Q44377201', 'Q57549504', 'Q868628', 'Q52199645', 'Q39736420', 'Q72344998',
        'Q47931363']], [['Q11472', 'Q13442814', 'Q1747211', 'Q410088', 'Q7132584', 'Q1402686', 'Q375703
        64', 'Q7132583', 'Q106499074', 'Q3894714'], ['Q245068', 'Q58209506', 'Q1004', 'Q3684337', 'Q772
        6945', 'Q108434880', 'Q838795', 'Q725377', 'Q1760610', 'Q715301'], ['Q43414701', 'Q30331748', '
        Q58572832', 'Q58572652', 'Q61938130', 'Q58572642', 'Q58446169', 'Q58447417', 'Q39870027', 'Q584
        99624']]]}

        >>> search_m2 = SearchManage(key='ids', m_num=10)
        >>> points2 = re1['id'][1]
        >>> print(points2)
        [['Q68', 'Q5157408', 'Q11202952', 'Q74058411', 'Q444195', 'Q7397', 'Q7889', 'Q21198', 'Q250', '
        Q32566'], ['Q7889', 'Q11410', 'Q13406554', 'Q223930', 'Q189936', 'Q16510064', 'Q723187', 'Q55524
        865', 'Q1493033', 'Q19862498'], ['Q4485157', 'Q2990909', 'Q5157439', 'Q44377201', 'Q57549504', '
        Q868628', 'Q52199645', 'Q39736420', 'Q72344998', 'Q47931363']]

        >>> re2 = search_m2.search_run(points=points2, keys=['claims/P31/value', 'labels/en'])
        Entities:30.Threading number:10.
        Querying successfully.
        Cost time:1.420619s.

        >>> print(re2)
        {'claims/P31/value': [[[], ['Q5633421'], ['Q28640', 'Q11488158', 'Q4164871'], [], ['Q4167410'], [],
        ['Q56055944'], ['Q11862829', 'Q4671286'], [], ['Q55215251']], [['Q56055944'], [], [],[], ['Q5'], [],
        ['Q11424'], ['Q431603', 'Q2178147'], ['Q4167410'], []], [[], ['Q482994'], ['Q134556'], ['Q4167410'],
        ['Q13442814'], ['Q41298'], ['Q13442814'], ['Q13442814'], ['Q13442814'], ['Q13442814']]],
        'labels/en': [['computer', 'Computer', 'human computer', 'computer voice', 'Computer', 'software', 'video
        game', 'computer science', 'computer keyboard', 'computed tomography'], ['video game', 'game', 'sports
        competition', 'game', 'The Game', 'sporting event', 'The Game', 'game', 'Game', 'game meat'], ['PC game',
        'Computer Games', 'Computer Games', 'Computer Games','Computer Games', 'Computer Games Magazine', 'Computer
        games and information-processing skills.','Computer games to teach hygiene: an evaluation of the e-Bug
        junior game.', 'Computer games as a means of movement rehabilitation', 'Computer games supporting cognitive
        behaviour therapy in children.']]}

    :ivar __url:
        a sting of domain name address
    :ivar entities_num(int):
        the number of querying entities
    :ivar key(string):
        a string reflecting its feature
    :ivar keys(Union[list, str, None]):
        a list or a string reflecting its keywords
    :ivar m_num(int):
        the number of thread in querying
    :ivar search_queue(Queue):
        consumer queue for init multiple entities(<class,'Entities'>)
    :ivar re_queue(Queue):
        producer queue for storing results of multiple entities(<class,'Entities'>)
    :ivar re_list(list):
        list of results of multiple entities(<class,'Entities'>)after querying
        for conveniently getting entities(<class,'Entities'>)
    :ivar paramFormat(Union[dict,str]):
        the format of parameters in querying
    :ivar index_(list):
        location index getting from expanse of N-dimensional list

    :param url_api: a sting of domain name address. Default: "https://www.wikidata.org/w/api.php"
    :param key: a string of query patten. 'search' or 'ids'. Default: "search"
    :param m_num: the number of thread in querying. Default: 10

    """

    def __init__(self, url_api: str = URL_WIKI, key: str = 'search', m_num: int = 10):
        super().__init__(key=key, m_num=m_num)
        self.__url = url_api

    def __set_search_entities(self, points: list, **kwargs):
        """Init search entities when key is 'search'.

        :param points: a 1D list of searching text
        :return: None

        See also:
            - Entities.set_entities
            - Entities.init_params
        """
        self.entities_num = len(points)
        for i in range(len(points)):
            entities = Entities()
            entities.set_entities(i, [points[i]])
            entities.init_params(points[i], key='search', **kwargs)
            self.search_queue.put(entities)

    def __set_get_entities(self, points: list, **kwargs):
        """Init search entities when key is 'ids'.

        :param points: a 1D list of searching text
        :return: None

        See also:
            - Entities.set_entities
            - Entities.init_params
            - Tools.threads_allocation

        """
        m_num = self.m_num
        if m_num > len(points):
            m_num = len(points)
        self.entities_num = len(points)
        search_en, re_en = Tools.threads_allocation(points, m_num)
        for i in range(len(search_en)):
            entities = Entities()
            entities.set_entities(i, re_en[i])
            entities.init_params(search_en[i], key='ids', **kwargs)
            self.search_queue.put(entities)

    def init_queue(self, points: list, **kwargs):
        """Init queue of search entities.

        :param points:
            a 1D list of querying entities txt or ids
        :param kwargs:
            for function expansion
        :return: None

        See more details:
            - https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities
            - https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
        """
        while not self.search_queue.empty():
            self.search_queue.get()
        points, self.index_ = Tools.list_level(points)
        if self.key == 'search':
            self.__set_search_entities(points, **kwargs)
        elif self.key == 'ids':
            self.__set_get_entities(points, **kwargs)
        else:
            raise ValueError('class "EntitiesSearch".key does\'t have %s.' % self.key)

    def set_entities_params(self, **kwargs):
        """Reset the queue of parameters in searching except entities text or ids.

        :param kwargs:
            set query parameters you want, you can see more details
            in ReadMe.md which explains different keywords you can
            reset in two different key.
        :return: None

        See more details:
            - https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities
            - https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities

        See also:
            - Entities.set_search_params
        """
        if self.search_queue.empty():
            warn("Search queue is empty. Can not change parameters of entities.")
            return
        queue_t = Queue()
        while not self.search_queue.empty():
            entities: Entities = self.search_queue.get()
            entities.set_search_params(**kwargs)
            queue_t.put(entities)
        self.search_queue = queue_t

    def analysis_to_dict(self) -> dict:
        """Turn result list of analysis from list[dict] into dict[list].

        :return: data of dict[list] from list[dict]
        """
        re_an = dict()
        if self.key == 'search':
            for key, value in self.re_list[0].get_analysis.items():
                re_an[key] = [value]
            da_: Entities
            for da_ in self.re_list[1::]:
                for key, value in da_.get_analysis.items():
                    re_an[key].append(value)
        elif self.key == 'ids':
            for k in self.keys:
                re_an[k] = []
            da_: Entities
            for da_ in self.re_list:
                for da_t in da_.get_analysis:
                    for key, value in da_t.items():
                        re_an[key].append(value)
        return re_an

    def analysis_entities(self, keys: Union[str, List[str]] = None) -> dict:
        """Analysis the list of entities' json data.

        :param keys:
            analysis keys, more details seeing in ReadMe.md
        :return:
            If key is 'search', it will return a dict like
            {'ids': ['Q3551770', 'Q4430597']}. If key is 'ids',
            it will return a list[dict] like [{'P31':['Q206439
            55']}, { 'P31': ['Q5']}].

        See also:
            - Entities.wiki_json_analysis
            - analysis_to_dict
        """
        da_: Entities
        for da_ in self.re_list:
            da_.wiki_json_analysis(key=self.key, keys=keys)
        return self.analysis_to_dict()

    def analysis_json(self, function_, *args, **kwargs) -> dict:
        """Analysis json data in the list of entities using your own function.

        :param function_: your own analysis function
        :param args: your own analysis function
        :param kwargs: your own function's parameters which format is dict
        :return: analysis result

        See also:
            - Entities.request_analysis
            - analysis_to_dict
        """
        da_: Entities
        for da_ in self.re_list:
            da_.request_analysis(key=self.key, function=function_, *args, **kwargs)
        return self.analysis_to_dict()

    def search_run(self, points: list, keys: Union[str, List[str]] = None,
                   timeout: float = 30.0, time_stop: float = 30.0, block_num: int = 10,
                   function_=None, args: tuple = (), **kwargs) -> dict:
        """Run querying using multithread.

        :param points:
            N-dimensional list of entities' text or ids
        :param keys:
            keywords you want to analysis form. Default: None
        :param timeout:
            when request is (timeout) seconds overtime, it will raise ValueError. Default: 30.0
        :param time_stop:
            blocking time when entities in queue raise exception. Default: 30.0
        :param block_num:
            maximum number of repeated running. Default: 10
        :param function_:
            your own analysis function. Default: None
        :param args:
            the parameters which format is tuple. Default: None
        :param kwargs:
            set query parameters you want, you can see more
            details in ReadMe.md which explains different
            keywords you can reset in two different key.
        :return:
            If key is 'search', it will return a dict like
            {'ids': ['Q3551770', 'Q4430597']}. If key is 'ids',
            it will return a dict[list] like {'claims/P31/va
            lue':['Q20643955']}. And the result will be at
            right position and right dimension in the list.
        """
        self.init_queue(points, **kwargs)

        if self.key == 'search':
            print(f'Entities:{self.entities_num}(type:text).Threading number:{self.m_num}.')
        else:
            print(f'Entities:{self.entities_num}(type:wiki\'s entities id).Threading number:{self.m_num}.')

        try:
            self.multithread_get_(timeout=timeout, time_stop=time_stop, block_num=block_num,
                                  url=self.__url, keys=keys, function_=function_, args=args)
        except RuntimeError:
            warn("Run time error.")
            return {}

        dict_t = dict()
        for k_, v_ in self.analysis_to_dict().items():
            dict_t[k_] = Tools.list_back(v_, self.index_)
        return dict_t


class Wikipedia(EntitiesSearch):
    """Class of querying with function in *wikipedia* using multithread.

        In this class, you can set the number of threading for querying.
        And you can put N-dimensional list of variable parameters in
        it, so you can query a number of entity in one time. What's more,
        each entity can get results at right position and right dimension
        in the list.

        Example
        -------
            >>> w1 = Wikipedia(m_num= 10)
            >>> pass

        :ivar entities_num(int):
            the number of querying entities
        :ivar key(string):
            a string reflecting its feature
        :ivar keys(Union[list, str, None]):
            a list or a string reflecting its keywords
        :ivar m_num(int):
            the number of thread in querying
        :ivar search_queue(Queue):
            consumer queue for init multiple entities(<class,'Entities'>)
        :ivar re_queue(Queue):
            producer queue for storing results of multiple entities(<class,'Entities'>)
        :ivar re_list(list):
            list of results of multiple entities(<class,'Entities'>)after querying
            for conveniently getting entities(<class,'Entities'>)
        :ivar paramFormat(Union[dict,str]):
            the format of parameters in querying
        :ivar index_(list):
            location index getting from expanse of N-dimensional list

        :param m_num: the number of thread in querying. Default: 10

        """

    def __init__(self, m_num=10):
        super().__init__(key='wikipedia', m_num=m_num, paramFormat="%s")

    def __function__(self, cache_: Queue, url: str = None, keys: Union[str, List[str]] = None, timeout: float = 5,
                     function_=None, args: tuple = None):
        while not self.search_queue.empty():
            entities: Entities = self.search_queue.get()
            try:
                if entities.get_params == "" or entities.get_params is None:
                    entities.set_request(None)
                else:
                    if function_ is None:
                        entities.set_request(wikipedia.suggest(entities.get_params))
                    else:
                        entities.set_request(function_(entities.get_params, *args))
                self.re_queue.put(entities)
            except Exception as e:
                print(e)
                cache_.put(entities)
        self.search_queue.task_done()

    def search_run(self, points: list, time_stop: float = 30.0, block_num: int = 10,
                   function_=None, args: tuple = (), **kwargs) -> list:
        """Run querying using multithread.

        :param points:
            N-dimensional list of variable parameters
        :param time_stop:
            blocking time when entities in queue raise exception. Default: 30.0
        :param block_num:
            maximum number of repeated running. Default: 10
        :param function_:
            chose your own function you want. If it is None, it will run function
            <wikipedia.suggest()>. Default: None
        :param args:
            parameters in tuple except var parameter in your own function.
            Default: None
        :param kwargs:
            for function expansion
        :return:
            a list of result at right position and right dimension
        """
        self.init_queue(points, **kwargs)
        print(f'Entities:{self.entities_num}(type:wikipedia).Threading number:{self.m_num}.')

        try:
            self.multithread_get_(timeout=30.0, time_stop=time_stop, block_num=block_num,
                                  url=None, keys=None, function_=function_, args=args)
        except RuntimeError:
            warn("Run time error.")
            return []

        return self.request_list


class SparqlQuery(EntitiesSearch):
    """Class of querying with Sparql using multithread.

    In this class, you can set the number of threading for querying.
    And you can put N-dimensional list of variable parameters in
    sparql element, so you can query a number of entity in one time.
    What's more, each entity can get results at right position and
    right dimension in the list.

    Example
    -------
     >>> sql = SparqlQuery(m_num = 10, format_ = 'json')
     >>> pass

    :ivar __returnFormat:
        Set the return format. If the one set is not
        an allowed value, the setting is ignored
    :ivar __url_:
        SPARQL endpoint's URI
    :ivar entities_num(int):
        the number of querying entities
    :ivar key(string):
        a string reflecting its feature
    :ivar keys(Union[list, str, None]):
        a list or a string reflecting its keywords
    :ivar m_num(int):
        the number of thread in querying
    :ivar search_queue(Queue):
        consumer queue for init multiple entities(<class,'Entities'>)
    :ivar re_queue(Queue):
        producer queue for storing results of multiple entities(<class,'Entities'>)
    :ivar re_list(list):
        list of results of multiple entities(<class,'Entities'>)after querying
        for conveniently getting entities(<class,'Entities'>)
    :ivar paramFormat(Union[dict,str]):
        the format of parameters in querying
    :ivar index_(list):
        location index getting from expanse of N-dimensional list

    :param m_num:
        the number of thread in querying. Default: 10
    :param format_:
        Set the return format.Possible values are : `json`,
        `xml`, `turtle`, `n3`, `rdf`, `rdfxml`, `csv`, `tsv`,
        `jsonld` (constants in this module). All other cases
        are ignored. Default: 'json'
    :param url_:
        SPARQL endpoint's URI. Default: URL_SPARQL_WIKI = "https://query.wikidata.org/sparql"
    :param sparql_:
        the SPARQL query format string. Default: SparqlQuery. SPARQL_WIKI
    """

    def __init__(self, m_num: int = 10, format_: str = 'json', url_: str = URL_SPARQL_WIKI, sparql_: str = SPARQL_WIKI):
        super().__init__(key='sparql', m_num=m_num, paramFormat=sparql_)
        self.__returnFormat = format_
        self.__url_ = url_

    def __function__(self, cache_: Queue, url: str = None, keys: Union[str, List[str]] = None, timeout: float = 5,
                     function_=None, args: tuple = None):
        sparql = SPARQLWrapper(endpoint=self.__url_, agent=AGENT_SPARQL_WIKI)
        sparql.setReturnFormat(self.__returnFormat)
        sparql.setTimeout(int(timeout))
        while not self.search_queue.empty():
            entities: Entities = self.search_queue.get()
            try:
                sparql.setQuery(entities.get_params)
                entities.set_request(sparql.query().convert())
            except Exception as e:
                print(e)
                cache_.put(entities)
                continue
            try:
                if function_ is None:
                    entities.set_analysis(AnalysisTools.sparql_analysis(entities.get_request))
                else:
                    entities.run_analysis(function=function_, *args)
            except Exception as e:
                print(e)
            self.re_queue.put(entities)
        self.search_queue.task_done()

    def set_keys(self):
        reg = re.compile("SELECT.*?WHERE")
        k_ = reg.search(self.paramFormat.replace("\n", "").replace(" ", ""))
        if k_ is None:
            raise ValueError("Sparql Error")
        keys = k_.group().replace("SELECT", "").replace("WHERE", "")
        self.keys = keys[1::].split("?")

    def analysis_to_dict(self) -> dict:
        re_an = dict()
        for key, value in self.re_list[0].get_analysis.items():
            re_an[key] = [value]
        da_: Entities
        for da_ in self.re_list[1::]:
            for key, value in da_.get_analysis.items():
                re_an[key].append(value)
        return re_an

    def search_run(self, points: list, timeout: float = 30.0, time_stop: float = 30.0,
                   block_num: int = 10, function_=None, args: tuple = (), **kwargs) -> dict:
        """Run querying using multithread.

        :param points:
            N-dimensional list of variable parameters in sparql element
        :param timeout:
            the timeout (in seconds) to use for querying the endpoint. Default: 30.0
        :param time_stop:
            blocking time when entities in queue raise exception. Default: 30.0
        :param block_num:
            maximum number of repeated running. Default: 10
        :param function_:
            your own analysis function. Default: None
        :param args:
            the parameters which format is tuple. Default: None
        :param kwargs:
            for function expansion
        :return:
            the result which format is dict will be at right
            position and right dimension in the list
        """
        self.init_queue(points, **kwargs)
        self.set_keys()
        print(f'Entities:{self.entities_num}(type:sparql).Threading number:{self.m_num}.')
        try:
            self.multithread_get_(timeout=timeout, time_stop=time_stop, block_num=block_num,
                                  url=None, keys=None, function_=function_, args=args)
        except RuntimeError:
            warn("Run time error.")
            return {}
        dict_t = dict()
        for k_, v_ in self.analysis_to_dict().items():
            dict_t[k_] = Tools.list_back(v_, self.index_)
        return dict_t


class BingQuery(EntitiesSearch):
    """Query in Bing Search using multithread.

    :ivar entities_num(int):
        the number of querying entities
    :ivar key(string):
        a string reflecting its feature
    :ivar keys(Union[list, str, None]):
        a list or a string reflecting its keywords
    :ivar m_num(int):
        the number of thread in querying
    :ivar search_queue(Queue):
        consumer queue for init multiple entities(<class,'Entities'>)
    :ivar re_queue(Queue):
        producer queue for storing results of multiple entities(<class,'Entities'>)
    :ivar re_list(list):
        list of results of multiple entities(<class,'Entities'>)after querying
        for conveniently getting entities(<class,'Entities'>)
    :ivar paramFormat(Union[dict,str]):
        the format of parameters in querying
    :ivar index_(list):
        location index getting from expanse of N-dimensional list
    """

    def __init__(self, url_: str = URL_BING, m_num: int = 10):
        super().__init__(key="q", m_num=m_num, paramFormat=PARAM_BING_QUERY)
        self.__url_ = url_

    def search_run(self, points: list, timeout: float = 30.0, time_stop: float = 30.0,
                   block_num: int = 10, function_=None, args: tuple = (), **kwargs) -> list:
        """Run querying using multithread.

        :param points:
            N-dimensional list of variable parameters in sparql element
        :param timeout:
            the timeout (in seconds) to use for querying the endpoint. Default: 30.0
        :param time_stop:
            blocking time when entities in queue raise exception. Default: 30.0
        :param block_num:
            maximum number of repeated running. Default: 10
        :param function_:
            your own analysis function. Default: None
        :param args:
            the parameters which format is tuple. Default: None
        :param kwargs:
            for function expansion
        :return:
            the result will be at right position and right dimension in the list
        """

        self.init_queue(points, **kwargs)
        if function_ is None:
            function_ = AnalysisTools.bing_search_analysis
        print(f'Entities:{self.entities_num}(type:bingQuery).Threading number:{self.m_num}.')
        try:
            self.multithread_get_(timeout=timeout, time_stop=time_stop, block_num=block_num,
                                  url=self.__url_, keys=None, function_=function_, args=args)
        except RuntimeError:
            warn("Run time error.")
            return []

        return self.analysis_list


class SpellCheck(EntitiesSearch):
    """Spell check in Bing Search using multithread.

    :ivar entities_num(int):
        the number of querying entities
    :ivar key(string):
        a string reflecting its feature
    :ivar keys(Union[list, str, None]):
        a list or a string reflecting its keywords
    :ivar m_num(int):
        the number of thread in querying
    :ivar search_queue(Queue):
        consumer queue for init multiple entities(<class,'Entities'>)
    :ivar re_queue(Queue):
        producer queue for storing results of multiple entities(<class,'Entities'>)
    :ivar re_list(list):
        list of results of multiple entities(<class,'Entities'>)after querying
        for conveniently getting entities(<class,'Entities'>)
    :ivar paramFormat(Union[dict,str]):
        the format of parameters in querying
    :ivar index_(list):
        location index getting from expanse of N-dimensional list
    """

    def __init__(self, url_: str = URL_BING, m_num: int = 10):
        super().__init__(key="q", m_num=m_num, paramFormat=PARAM_BING_SPELL_CHECK)
        self.__url_ = url_

    def __remove_none(self):
        p = self.param_list_1d
        a = self.analysis_list_1d
        for i in range(self.entities_num):
            if a[i] is None:
                self.re_list[i].set_analysis(p[i][self.key])

    def search_run(self, points: list, timeout: float = 30.0, time_stop: float = 30.0,
                   block_num: int = 10, function_=None, args: tuple = (), **kwargs) -> list:
        self.init_queue(points, **kwargs)
        print(f'Entities:{self.entities_num}(type: SpellCheck<{self.__url_}>). Threading number:{self.m_num}.')
        if function_ is None:
            function_ = AnalysisTools.spell_check_bing
        try:
            self.multithread_get_(timeout=timeout, time_stop=time_stop, block_num=block_num,
                                  url=self.__url_, keys=None, function_=function_, args=args)
        except RuntimeError:
            warn("Run time error.")
            return []
        self.__remove_none()
        return self.analysis_list


class DbpediaLookUp(EntitiesSearch):
    """Querying keywords using Dbpedia look up tools with multithread.

    :ivar entities_num(int):
        the number of querying entities
    :ivar key(string):
        a string reflecting its feature
    :ivar keys(Union[list, str, None]):
        a list or a string reflecting its keywords
    :ivar m_num(int):
        the number of thread in querying
    :ivar search_queue(Queue):
        consumer queue for init multiple entities(<class,'Entities'>)
    :ivar re_queue(Queue):
        producer queue for storing results of multiple entities(<class,'Entities'>)
    :ivar re_list(list):
        list of results of multiple entities(<class,'Entities'>)after querying
        for conveniently getting entities(<class,'Entities'>)
    :ivar paramFormat(Union[dict,str]):
        the format of parameters in querying
    :ivar index_(list):
        location index getting from expanse of N-dimensional list
    :param key:
        the key where parameters will be set, choose "query",
        "label", "comment" or "category". Default: "query"
    :param m_num:
        the number of thread you want to set. Default: 5
    """

    def __init__(self, key: str = "query", m_num: int = 5):
        super().__init__(key=key, m_num=m_num, paramFormat=PARAM_DBPEDIA_QUERY)

    def analysis_to_dict(self) -> dict:
        """Turn result list of analysis from list[dict] into dict[list].

        :return: data of dict[list] from list[dict]
        """
        re_an = dict()
        for k, v in self.re_list[0].get_analysis.items():
            re_an[k] = [v]
        da_: Entities
        for da_ in self.re_list[1::]:
            for key, value in da_.get_analysis.items():
                re_an[key].append(value)
        return re_an

    def search_run(self, points: list, patten: str = "search", is_all: bool = False, timeout: float = 30.0,
                   time_stop: float = 30.0, block_num: int = 10, function_=None, args: tuple = (), **kwargs) -> dict:
        """Run querying using multithread.
        :param points:
            N-dimensional list of variable parameters in sparql element
        :param patten:
            the patten of querying, choose 'prefix' or 'search'. Default: 'search'
        :param is_all:
            return all analysis result or not. Default: False
        :param timeout:
            the timeout (in seconds) to use for querying the endpoint. Default: 30.0
        :param time_stop:
            blocking time when entities in queue raise exception. Default: 30.0
        :param block_num:
            maximum number of repeated running. Default: 10
        :param function_:
            your own analysis function. Default: None
        :param args:
            the parameters which format is tuple. Default: None
        :param kwargs:
            for function expansion
        :return:
            a dict of the analysis result
        """
        self.init_queue(points, **kwargs)
        if patten == 'search':
            url_ = URL_DBPEDIA_SEARCH
        else:
            url_ = URL_DBPEDIA_PREFIX
        if function_ is None:
            function_ = AnalysisTools.dbpedia_analysis
        print(f'Entities:{self.entities_num}(type:DBpediaLookup&{patten}).Threading number:{self.m_num}.')
        try:
            self.multithread_get_(timeout=timeout, time_stop=time_stop, block_num=block_num,
                                  url=url_, keys=None, function_=function_, args=args)
        except RuntimeError:
            warn("Run time error.")
            return {}

        dict_t = dict()
        for k_, v_ in self.analysis_to_dict().items():
            dict_t[k_] = Tools.list_back(v_, self.index_)
        if not is_all:
            return {"label": dict_t["label"], "resource": dict_t["resource"],
                    "typeName": dict_t["typeName"], "type": dict_t["type"]}
        return dict_t
