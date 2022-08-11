# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/7/20 
# @function: the static method in class <AnalysisTools>
# @version : V0.4.1
#

from typing import Union, List
from warnings import warn
from bs4 import BeautifulSoup
from requests import Response
import re

DATA_TYPE = {
    'commonsMedia': 'string',
    'globe-coordinate': 'globecoordinate',
    'wikibase-item': 'wikibase-entityid',
    'wikibase-property': 'wikibase-entityid',
    'string': 'string',
    'monolingualtext': 'monolingualtext',
    'external-id': 'string',
    'quantity': 'quantity',
    'time': 'time',
    'url': 'string',
    'math': 'string',
    'geo-shape': 'string',
    'musical-notation': 'string',
    'tabular-data': 'string',
    'wikibase-lexeme': 'wikibase-entityid',
    'wikibase-form': 'wikibase-entityid',
    'wikibase-sense': 'wikibase-entityid'
}
"""Wikidata Data-type"""

VALUE_TYPE = {
    'wikibase-entityid': [1, 'id', 'entity-type', 'numeric-id'],
    'globecoordinate': [2, 'latitude', 'longitude', 'precision', 'globe'],
    'time': [1, 'time', 'precision', 'before', 'after', 'timezone'],
    'string': None,
    'monolingualtext': [2, 'text', 'language'],
    'quantity': [1, 'amount', 'lowerBound', 'upperBound']
}
"""Wikidata Value-type"""

REG = re.compile("/")
"""Regular expression"""

PATTEN1 = ['labels', 'descriptions', 'aliases']
"""Analysis keys in patten1"""

DBPEDIA_KEYS = [(1, 'label'), (1, 'resource'), (2, 'typeName'), (2, 'type'), (1, 'score'),
                (1, 'refCount'), (1, 'comment'), (2, 'redirectlabel'), (2, 'category')]
"""Analysis keys-1 using in Dbpedia look up json data."""


class AnalysisTools:
    """Class of providing some simple analysis functions.

    In this class, you can call its static methods you want."""

    @staticmethod
    def search_analysis(json_: Response, keys: Union[str, list] = None) -> dict:
        """Analysis function used when key = 'search' in SearchManage.

        :param json_: json data
        :param keys: analysis keys. you can use 'all', 'id', 'url', 'label', or 'description'
        :return: result of analysis which format is dict
        """

        try:
            json_ = json_.json()
        except ValueError:
            raise ValueError("Json data error.")
        id_ = []
        url_ = []
        label_ = []
        describe_ = []
        match_ = []

        try:
            if json_['success'] == 1:
                for da_ in json_['search']:
                    try:
                        id_.append(da_['id'])
                    except KeyError:
                        pass
                    try:
                        url_.append(da_['url'])
                    except KeyError:
                        pass
                    try:
                        label_.append(da_['label'])
                    except KeyError:
                        pass
                    try:
                        describe_.append(da_['description'])
                    except KeyError:
                        pass
                    try:
                        match_.append(da_['match']['type'])
                    except KeyError:
                        pass
        except KeyError:
            pass

        re_dict = {'id': id_, 'url': url_, 'label': label_, 'description': describe_, 'match': match_}
        if keys == 'all' or type(keys) == list:
            return re_dict
        try:
            return {keys: re_dict[keys]}
        except KeyError:
            print(f'No keys:{keys}')
            return re_dict

    @staticmethod
    def keys_regular(keys: List[str]) -> List[Union[dict, None]]:
        """Get features of analysis keys.

        :param keys: analysis keys in a list
        :return: return features of keys which format is dict

        See also:
            - entities_analysis: analysis function used when key = 'ids' in SearchManage.
        """
        re_ = []
        for key in keys:
            re_dict = {
                'key': key,
                'correct': 1,
                'root': None,
                'identity': None,
                'patten': None,
                'error': None
            }
            kl = REG.split(key)
            # labels,description,aliases
            if kl[0] in PATTEN1:
                re_dict['root'] = kl[0]
                if len(kl) == 1:
                    re_dict['patten'] = 0
                elif len(kl) == 2:
                    if kl[1] == '':
                        re_dict['patten'] = 0
                    else:
                        re_dict['patten'] = 1
                        re_dict['identity'] = kl[1]
                elif len(kl) == 3:
                    if kl[1] == '' and kl[2] == '':
                        re_dict['patten'] = 0
                    else:
                        re_dict['error'] = 'format'
                        re_dict['correct'] = 0
                else:
                    re_dict['error'] = 'tooLong'
                    re_dict['correct'] = 0

            # claims
            elif kl[0] == 'claims':
                re_dict['root'] = kl[0]
                if len(kl) == 1:
                    re_dict['patten'] = 0
                elif len(kl) == 2:
                    if kl[1] == '':
                        re_dict['patten'] = 0
                    elif (kl[1].lower())[0] == 'p':
                        re_dict['patten'] = 0
                        re_dict['identity'] = kl[1]
                    else:
                        re_dict['error'] = 'format'
                        re_dict['correct'] = 0
                elif len(kl) == 3:
                    if kl[1] == '' and kl[2] == '':
                        re_dict['patten'] = 0
                    elif (kl[1].lower())[0] == 'p':
                        re_dict['identity'] = kl[1]
                        if kl[2] == '':
                            re_dict['patten'] = 0
                        elif kl[2] == 'value':
                            re_dict['patten'] = 1
                        elif kl[2] == 'qualifiers-order':
                            re_dict['patten'] = 2
                        elif kl[2] == 'qualifiers':
                            re_dict['patten'] = 3
                        elif kl[2] == 'references':
                            re_dict['patten'] = 4
                        else:
                            re_dict['error'] = 'format'
                            re_dict['correct'] = 0
                    else:
                        re_dict['error'] = 'format'
                        re_dict['correct'] = 0
                elif len(kl) == 4:
                    if (kl[1].lower())[0] == 'p':
                        re_dict['identity'] = kl[1]
                        if kl[2] == '' and kl[3] == '':
                            re_dict['patten'] = 0
                        elif kl[2] == 'qualifiers' and kl[3] == '':
                            re_dict['patten'] = 3
                        elif kl[2] == 'references' and kl[3] == '':
                            re_dict['patten'] = 4
                        else:
                            re_dict['error'] = 'format'
                            re_dict['correct'] = 0
                    else:
                        re_dict['error'] = 'format'
                        re_dict['correct'] = 0
                elif len(kl) == 5:
                    if (kl[1].lower())[0] == 'p':
                        re_dict['identity'] = kl[1]

                        if kl[2] == 'qualifiers' and kl[3] == '' and kl[4] == '':
                            re_dict['patten'] = 3
                        elif kl[2] == 'references' and kl[3] == '' and kl[4] == '':
                            re_dict['patten'] = 4
                        else:
                            re_dict['error'] = 'format'
                            re_dict['correct'] = 0
                    else:
                        re_dict['error'] = 'format'
                        re_dict['correct'] = 0
                else:
                    re_dict['error'] = 'tooLong'
                    re_dict['correct'] = 0

            # sitelinks
            elif kl[0] == 'sitelinks':
                re_dict['root'] = kl[0]
                if len(kl) == 1:
                    re_dict['patten'] = 0
                elif len(kl) == 2:
                    if kl[1] == '':
                        re_dict['patten'] = 0
                    else:
                        re_dict['error'] = 'format'
                        re_dict['correct'] = 0
                elif len(kl) == 3:
                    if kl[1] == '' and kl[2] == '':
                        re_dict['patten'] = 0
                    else:
                        re_dict['error'] = 'format'
                        re_dict['correct'] = 0
                else:
                    re_dict['error'] = 'tooLong'
                    re_dict['correct'] = 0
            else:
                re_dict['error'] = 'root'
                re_dict['correct'] = 0
            re_.append(re_dict)
        return re_

    @staticmethod
    def value_analysis(da_: dict) -> Union[tuple, float, str, int, None]:
        """Get analysis result of different data type.

        :param da_: mainsnak in claims or value in qualifiers
        :return: analysis result which format is tuple including of its value type and main value

        See also:
            - claims_analysis: get analysis result when key is including of 'claims'
        """
        try:
            # da = da_['mainsnak']
            da_t = da_['datatype']
            va_t = DATA_TYPE[da_t]
            value_l = VALUE_TYPE[va_t]
        except KeyError:
            return tuple([None, None])
        if va_t == 'string':
            try:
                return tuple([va_t, da_['datavalue']['value']])
            except KeyError:
                return tuple([va_t, None])
        else:
            num = value_l[0]
            if num == 1:
                try:
                    return tuple([va_t, da_['datavalue']['value'][value_l[1]]])
                except KeyError:
                    return tuple([va_t, None])
            else:
                tuple_l = [va_t]
                for i in range(num):
                    try:
                        tuple_l.append(da_['datavalue']['value'][value_l[i + 1]])
                    except KeyError:
                        tuple_l.append(None)
                tuple_ = tuple(tuple_l)
                return tuple_

    @staticmethod
    def patten1_analysis(json_: dict, key: dict) -> Union[list, str, dict, None]:
        """Get analysis result when key is in PATTEN1.

        :param json_: json data
        :param key: analysis key
        :return: result of analysis

        See also:
            - entities_analysis: analysis function used when key = 'ids' in SearchManage.
        """
        if key['patten'] == 0:
            try:
                return json_[key['root']]
            except KeyError:
                return {}
        else:
            try:
                da_ = json_[key['root']][key['identity']]
            except KeyError:
                if key['root'] == 'aliases':
                    return []
                else:
                    return None
            if key['root'] == 'aliases':
                re_ = []
                for da in da_:
                    try:
                        re_.append(da['value'])
                    except KeyError:
                        pass
                return re_
            else:
                try:
                    return da_['value']
                except KeyError:
                    return None

    @staticmethod
    def claims_analysis(json_: dict, key: dict) -> Union[list, dict]:
        """Get analysis result when key is including of 'claims'.

        :param json_: json data
        :param key: analysis key's feature
        :return: analysis data

        See also:
            - entities_analysis: analysis function used when key = 'ids' in SearchManage.
        """
        if key['patten'] == 0:
            if key['identity'] is None:
                try:
                    return json_['claims']
                except KeyError:
                    return {}
            elif key['identity'].lower() == 'p':
                re_ = []
                try:
                    for k_, v_ in json_['claims'].items():
                        re_.append(k_)
                except KeyError:
                    pass
                return re_
            else:
                try:
                    return json_['claims'][key['identity']]
                except KeyError:
                    return []

        elif key['patten'] == 1:
            if key['identity'].lower() == 'p':
                try:
                    da__: dict = json_['claims']
                except KeyError:
                    return {}
                re_ = dict()
                for k_, v__ in da__.items():
                    re_[k_] = []
                    for v_ in v__:
                        try:
                            v = v_['mainsnak']
                            re__ = AnalysisTools.value_analysis(v)
                            if re__[1] is not None:
                                re_[k_].append(re__)
                        except KeyError:
                            pass

                return re_
            else:
                try:
                    da_ = json_['claims'][key['identity']]
                except KeyError:
                    return []
                re_ = []
                for da in da_:
                    try:
                        d = da['mainsnak']
                        re__ = AnalysisTools.value_analysis(d)
                        if re__[1] is not None:
                            re_.append(re__)
                    except KeyError:
                        pass
                return re_

        elif key['patten'] == 2:
            re_ = []
            try:
                for da_ in json_[key['root']][key['identity']]:
                    try:
                        re_.append(da_['qualifiers-order'])
                    except KeyError:
                        re_.append([])
            except KeyError:
                pass
            return re_
        elif key['patten'] == 3:
            re_ = []
            try:
                for da_ in json_[key['root']][key['identity']]:
                    try:
                        re_.append(da_['qualifiers'])
                    except KeyError:
                        re_.append({})
            except KeyError:
                pass
            return re_
        elif key['patten'] == 4:
            re_ = []
            try:
                for da_ in json_[key['root']][key['identity']]:
                    try:
                        re_.append(da_['references'])
                    except KeyError:
                        re_.append({})
            except KeyError:
                pass
            return re_

    @staticmethod
    def sitelinks_analysis(json_: Response, key: dict) -> dict:
        """Get result in sitelinks.

        :param json_: json data
        :param key: analysis key's features
        :return: result of analysis

        See also:
         - entities_analysis: analysis function used when key = 'ids' in SearchManage.
        """
        try:
            json_ = json_.json()
        except ValueError:
            raise ValueError("Json data error.")
        try:
            return json_[key['root']]
        except KeyError:
            return {}

    @staticmethod
    def entities_analysis(json_: Response, keys: Union[str, List[str]] = None) -> List[dict]:
        """Analysis function used when key = 'ids' in SearchManage.

        :param json_: json data
        :param keys: analysis keys, more details seeing in ReadMe.md
        :return: result of analysis which format is list with dict

        See also:
         - keys_regular
         - patten1_analysis
         - claims_analysis
         - sitelinks_analysis
        """
        try:
            json_ = json_.json()
        except ValueError:
            raise ValueError("Json data error.")
        if type(keys) is str:
            keys = [keys]
        keys_dict = AnalysisTools.keys_regular(keys)
        re_list = []
        try:
            if json_['success'] == 1:
                for key_, value_ in json_['entities'].items():
                    re_dict = dict()

                    for k in keys_dict:
                        re_dict[k['key']] = None
                        if k['correct'] == 1:
                            if k['root'] in PATTEN1:
                                re_dict[k['key']] = AnalysisTools.patten1_analysis(value_, k)
                            elif k['root'] == 'claims':
                                re_dict[k['key']] = AnalysisTools.claims_analysis(value_, k)
                            elif k['root'] == 'sitelinks':
                                re_dict[k['key']] = AnalysisTools.sitelinks_analysis(value_, k)

                    re_list.append(re_dict)
        except KeyError:
            pass

        return re_list

    @staticmethod
    def sparql_analysis(json_: dict) -> dict:
        """Get analysis result using in <SparqlQuery>.

        :param json_: json data
        :return: result of analysis which format is dict
        """
        try:
            keys: list = json_['head']['vars']
        except KeyError:
            warn("Analysis filed. Please check Sparql command.")
            return {}

        re_an = dict()
        for k_ in keys:
            re_an[k_] = []

        try:
            da_l: list = json_['results']['bindings']
            for da_ in da_l:
                for k_ in keys:
                    try:
                        re_an[k_].append(da_[k_]['value'])
                    except KeyError:
                        re_an[k_].append(da_[k_])
        except KeyError:
            pass

        return re_an

    @staticmethod
    def bing_search_analysis(request_: Response):
        REG_BING_SEARCH = re.compile(r"https://www.wikidata.org/wiki/Q\d+")
        try:
            result = str(request_.content)
        except ValueError:
            raise ValueError

        return REG_BING_SEARCH.findall(result)

    @staticmethod
    def spell_check_bing(request: Response):
        result = request.text
        res = None
        try:
            soup = BeautifulSoup(result, 'lxml')
            if soup.find('div', id='sp_requery'):
                res = soup.find('div', id='sp_requery').a.text
        except Exception as e:
            print(e)
        return res

    @staticmethod
    def dbpedia_analysis(request_: Response) -> dict:
        try:
            json_ = request_.json()
        except ValueError:
            raise ValueError("Json data error.")
        res = dict()
        for da in DBPEDIA_KEYS:
            res[da[1]] = []
        try:
            docs_ = json_['docs']
            for element in docs_:
                for da in DBPEDIA_KEYS:
                    if da[0] == 1:
                        try:
                            res[da[1]].append(element[da[1]][0])
                        except KeyError or IndexError:
                            res[da[1]].append(None)
                    else:
                        try:
                            res[da[1]].append(element[da[1]])
                        except KeyError or IndexError:
                            res[da[1]].append([])
        except KeyError:
            pass
        return res
