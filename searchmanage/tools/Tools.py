# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/5/20
# @function: the static method in class Tools
# @version : V0.4.0
#

import csv
import numpy as np
from typing import List, Any, Union, Tuple

# Browser proxies list
AGENTS_ = [
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0; Baiduspider-ads) Gecko/17.0 Firefox/17.0",
    "Mozilla/5.0 (Linux; U; Android 2.3.6; en-us; Nexus S Build/GRK39F) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Avant Browser/1.2.789rel1 (http://www.avantbrowser.com)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/12.0.712.0 Safari/534.27",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.24 Safari/535.1",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0 x64; en-US; rv:1.9pre) Gecko/2008072421 Minefield/3.0.2pre",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9b4) Gecko/2008030317 Firefox/3.0b4",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-GB; rv:1.9.0.11) Gecko/2009060215 Firefox/3.0.11 (.NET CLR 3.5.30729)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 GTB5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; tr; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 ( .NET CLR 3.5.30729; .NET4.0E)",
    "Mozilla/5.0 (Windows; U; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; BIDUBrowser 7.6)",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0",
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64; Trident/7.0; Touch; LCJB; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0a2) Gecko/20110622 Firefox/6.0a2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101 Firefox/7.0.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b4pre) Gecko/20100815 Minefield/4.0b4pre",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0 )",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
    "Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a",
    "Mozilla/2.02E (Win95; U)",
    "Mozilla/3.01Gold (Win95; I)",
    "Mozilla/4.8 [en] (Windows NT 5.1; U)",
    "Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)",
    "HTC_Dream Mozilla/5.0 (Linux; U; Android 1.5; en-ca; Build/CUPCAKE) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.2; U; de-DE) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/234.40.1 Safari/534.6 TouchPad/1.0",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-us; sdk Build/CUPCAKE) AppleWebkit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.1; en-us; Nexus One Build/ERD62) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-us; htc_bahamas Build/CRB17) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.1-update1; de-de; HTC Desire 1.19.161.5 Build/ERE27) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Sprint APA9292KT Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 1.5; de-ch; HTC Hero Build/CUPCAKE) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; ADR6300 Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.1; en-us; HTC Legend Build/cupcake) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 1.5; de-de; HTC Magic Build/PLAT-RC33) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1 FirePHP/0.3",
    "Mozilla/5.0 (Linux; U; Android 1.6; en-us; HTC_TATTOO_A3288 Build/DRC79) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 1.0; en-us; dream) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-us; T-Mobile G1 Build/CRB43) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari 525.20.1",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-gb; T-Mobile_G2_Touch Build/CUPCAKE) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.0; en-us; Droid Build/ESD20) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Droid Build/FRG22D) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.0; en-us; Milestone Build/ SHOLS_U2_01.03.1) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.0.1; de-de; Milestone Build/SHOLS_U2_01.14.0) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 0.5; en-us) AppleWebKit/522  (KHTML, like Gecko) Safari/419.3",
    "Mozilla/5.0 (Linux; U; Android 1.1; en-gb; dream) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 2.0; en-us; Droid Build/ESD20) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.1; en-us; Nexus One Build/ERD62) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Sprint APA9292KT Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; ADR6300 Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-ca; GT-P1000M Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 3.0.1; fr-fr; A500 Build/HRI66) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 1.6; es-es; SonyEricssonX10i Build/R1FA016) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 1.6; en-us; SonyEricssonX10i Build/R1AA056) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1"
]
"""User-Agents you can choose"""


class Tools:
    """Class of providing some functions you may use.

    In this class, you can call its static methods you want."""

    @staticmethod
    # Read 2D data from csv
    def read_csv(filename: str, is_header: bool = True, out_data_t: bool = True, is_print: bool = True):
        """Read 2D data from csv file.

        Parameters
        ----------
        filename : int
            csv file path
        is_header : bool
            if you want to skip the header(the first line) of csv file. default: True
        out_data_t :bool
            if return transformation of 2D data. default: True
        is_print :bool
            if print information of reading. default: True

        Returns
        -------
        out:
            2D table list, and if out_data_t = True, it will return transform of table list as well.
        """
        data_list = []
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                if is_header:
                    next(f)
                for data in reader:
                    data_list.append(data)
        except Exception as e:
            print(e)
            data_list.append(['None'])
        if is_print:
            print(f'File:{filename} Read Successfully!')
        if out_data_t:
            data_arr = np.array(data_list)
            data_list_t = (data_arr.transpose()).tolist()
            return data_list, data_list_t
        else:
            return data_list

    @staticmethod
    # Write 2D data to csv file
    def data_write_to_csv(filename: str, data: List[List[Any]], headers: List[str] = None, isHeaders: bool = False):
        """Write 2D data to csv file.

        Parameters
        ----------
        filename:str
            csv file path
        data:list
            2D data
        headers:list
            table headers. default:None
        isHeaders:bool
            if you want to write headers. default:False

        Returns
        -------
        out:
            None
        """
        if headers is None:
            headers = []
        try:
            with open(filename, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                if isHeaders:
                    writer.writerow(headers)
                for i in range(len(data)):
                    writer.writerow(data[i])
            print(f'File:{filename},Write Successfully！')
        except Exception as e:
            print(e)
            print(f'File:{filename},Write Failed！')
        print()

    @staticmethod
    # hierarchical structure of data
    def hierarchical_structure(temp_other: Any, hie_level: int = 0, max_level: int = None):
        """Print hierarchical structure of data.

        Parameters
        ------
        temp_other:Any
            data you want to print
        hie_level:int
            the first level you want to print. default: 0
        max_level:int
            the max level you want to print. default: None

        Returns
        -------
        out:
            None

        Example
        -------
        >>> from tools import Tools
        >>> data = [{'a':{'a1':[1,2],'a2':[3,4]},'b':{'b1':[1,2],'b2':[3,4]}}]
        >>> Tools.hierarchical_structure(data)
         0 <class 'list'>
         Length:1
             1 <class 'dict'>
             Length:2
             1.1 a
                 2 <class 'dict'>
                 Length:2
                 2.1 a1
                     3 <class 'list'>
                     Length:2
                         4 <class 'int'>
                 2.2 a2
                     3 <class 'list'>
                     Length:2
                         4 <class 'int'>
             1.2 b
                 2 <class 'dict'>
                 Length:2
                 2.1 b1
                     3 <class 'list'>
                     Length:2
                         4 <class 'int'>
                 2.2 b2
                     3 <class 'list'>
                     Length:2
                         4 <class 'int'>
        >>> Tools.hierarchical_structure(data,max_level = 2)
         0 <class 'list'>
         Length:1
             1 <class 'dict'>
             Length:2
             1.1 a
                 2 <class 'dict'>
             1.2 b
                 2 <class 'dict'>
        """
        type_temp = type(temp_other)
        level_num = hie_level
        print('\t' * level_num, level_num, type_temp)
        if max_level is not None:
            if hie_level == max_level:
                return
        if type_temp == dict:
            print('\t' * level_num, f'Length:{len(temp_other)}')
            if len(temp_other) == 0:
                print('\t' * level_num, 'None')
            else:
                num = 1
                for keys, values in temp_other.items():
                    print('\t' * level_num, f'{level_num}.{num}', keys)
                    num += 1
                    Tools.hierarchical_structure(values, hie_level=level_num + 1, max_level=max_level)
        elif type_temp == list:
            print('\t' * level_num, f'Length:{len(temp_other)}')
            if len(temp_other) == 0:
                print('\t' * level_num, 'None')
            else:
                Tools.hierarchical_structure(temp_other[0], hie_level=level_num + 1, max_level=max_level)

    @staticmethod
    # N-dimensional list expansion
    def list_level(list_: Union[List[Any], Any]) -> Union[Tuple[list, int], Tuple[list, list], Tuple[Any, None]]:
        """N-dimensional list expanses to 1D list and returns location __index

        Parameters
        ----------
        list_:List[Any]
            N-dimensional list

        Returns
        --------
        out:
            1D list and location __index

        See also
        --------
        list_back_find:Recursive function, map restore using location __index
        list_back: Map restore using location __index
        """
        if type(list_) == list:
            if len(list_) == 0:
                return list_, 0
            if type(list_[0]) == list:
                index_list = []
                da_temp = []
                for da in list_:
                    next_da, next_in = Tools.list_level(da)
                    da_temp = da_temp + next_da
                    index_list.append(next_in)
                return da_temp, index_list
            else:
                return list_, len(list_)
        else:
            return list_, None

    @staticmethod
    # Map restore using location __index
    def list_back_find(da_1d: List[Any], index_li: Union[int, List[Any]], start_in: int = 0):
        """Recursive function, map restore using location __index

        Parameters
        ----------
        da_1d:list
            list data
        index_li:list
            location __index
        start_in:int
            the first __index in process of restoring

        Returns
        -------
        out:
            the data in the process and the information in the next process

        See also
        --------
        list_level:N-dimensional list expanses to 1D list and returns location __index
        list_back: Map restore using location __index
        """
        if type(da_1d) != list:
            return da_1d
        if type(index_li) == list:
            da_t = []
            in_t = start_in
            for ind in index_li:
                da_t1, in_t = Tools.list_back_find(da_1d, ind, in_t)
                da_t.append(da_t1)
            return da_t, in_t
        else:
            return da_1d[start_in:start_in + index_li], start_in + index_li

    @staticmethod
    # Map restore using location __index
    def list_back(da_1d: List[Any], index_li: Union[int, List[Any]]) -> Union[int, List[Any]]:
        """Map restore using location __index

        Parameters
        ----------
        da_1d:list
            list data
        index_li:list
            location __index

        Returns
        -------
        out:
            list after restoring process

        See also
        --------
        list_level:N-dimensional list expanses to 1D list and returns location __index
        list_back_find:Recursive function, map restore using location __index

        Example
        -------
        >>> from tools import Tools
        >>> a=[[1,1],[2,2],[3],[4,4,4],[],[6]]
        >>> print(a)
        [[1, 1], [2, 2], [3], [4, 4, 4], [], [6]]
        >>> a_1d,index1 = Tools.list_level(a)
        >>> print(a_1d)
        [1, 1, 2, 2, 3, 4, 4, 4, 6]
        >>> print(index1)
        [2, 2, 1, 3, 0, 1]
        >>> a_t1 = [['1','1'],['1'],['2'],['2','2'],[],['4'],['4','4'],['4'],['6']]
        >>> re1 = Tools.list_back(a_t1,index1)
        >>> print(re1)
        [[['1', '1'], ['1']], [['2'], ['2', '2']], [[]], [['4'], ['4', '4'], ['4']], [[None]], [['6']]]
        >>> a_t2 = ['1','1','2','2','3','4','4','4','6']
        >>> re2 = Tools.list_back(a_t2,index1,is_deep_list=False)
        >>> print(re2)
        [['1', '1'], ['2', '2'], ['3'], ['4', '4', '4'], [], ['6']]
        """

        re_t = Tools.list_back_find(da_1d, index_li)
        return re_t[0]

    @staticmethod
    # Record repeating __entities in list
    def repeat_entities(points_t: list) -> list:
        """
        Record repeating __entities in list.

        When we query multiple __entities ids at the same time using Wiki-Api: 'wbgetentities',
        duplicate entity will only return one json data. So we need to ensure that the position
        of subsequent parsing is correct. This function is to record duplicate entity __index.

        Parameters
        ----------
        points_t:list
            1D __entities ids list

        Returns
        --------
        out:list
            duplicate __entities __index

        See also
        --------
        repeat_entities_back:
            Restore duplicate entity mapping
        """
        new_points = []
        index_ = []
        for i in range(len(points_t)):
            if points_t[i] in new_points:
                index_.append((i, new_points.index(points_t[i])))
            else:
                new_points.append(points_t[i])
        return index_

    @staticmethod
    # Restore duplicate entity mapping
    def repeat_entities_back(new_p: list, index_: list) -> List[Any]:
        """Restore duplicate entity mapping

        Parameters
        ----------
        new_p:list
            non duplicate entity mapping
        index_:list
            duplicate __entities __index

        Returns
        --------
        out:list
            restored duplicate entity mapping

        See also
        ---------
        repeat_entities:
            Record repeating __entities __index in list

        Example
        -------
        >>> a = ['Q1','Q2','Q1','Q3','Q2','Q1','Q3']
        >>>print(a)
        ['Q1', 'Q2', 'Q1', 'Q3', 'Q2', 'Q1', 'Q3']
        >>> index1 = Tools.repeat_entities(a)
        >>> print(index1)
        [(2, 0), (4, 1), (5, 0), (6, 2)]
        >>> after_search = [{'Q1':1},{'Q2':2},{'Q3':3}]
        >>> print(after_search)
        [{'Q1': 1}, {'Q2': 2}, {'Q3': 3}]
        >>> restored_ = Tools.repeat_entities_back(after_search,index1)
        >>> print(restored_)
        [{'Q1': 1}, {'Q2': 2}, {'Q1': 1}, {'Q3': 3}, {'Q2': 2}, {'Q1': 1}, {'Q3': 3}]
        """
        points_ = []
        in1_ = 0
        in2_ = 0
        for i in range(len(index_) + len(new_p)):
            if in2_ < len(index_):
                if i == index_[in2_][0]:
                    points_.append(new_p[index_[in2_][1]])
                    in2_ += 1
                else:
                    points_.append(new_p[in1_])
                    in1_ += 1
            else:
                points_.append(new_p[in1_])
                in1_ += 1
        return points_

    @staticmethod
    # Allocate the number of simultaneous query __entities
    def threads_allocation(points: list, thread_num: int) -> Tuple[list, List[Any]]:
        """Allocate the number of simultaneous query __entities according to the number of threads

        We can query multiple __entities ids at the same time using Wiki-Api: 'wbgetentities'.
        So when using multiple threads to get data, we can allocate the number of simultaneous
        query __entities according to the number of threads to improve data acquisition speed.

        Parameters
        ----------
        points:list
            __entities ids list
        thread_num:int
            the number of threads

        Returns
        -------
        out:Tuple[list, List[Any]]
                __entities that have allocated with '|', and 2D __entities list that have allocated

        Example
        -------
        >>> a = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q10', 'Q11', 'Q12']
        >>> print(a)
        ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q10', 'Q11', 'Q12']
        >>> a_re, at = Tools.threads_allocation(a, thread_num=5)
        >>> print(a_re)
        ['Q1|Q2', 'Q3|Q4', 'Q5|Q6', 'Q7|Q10', 'Q11|Q12']
        >>> print(at)
        [['Q1', 'Q2'], ['Q3', 'Q4'], ['Q5', 'Q6'], ['Q7', 'Q10'], ['Q11', 'Q12']]
        """
        thread_n = thread_num
        if len(points) // thread_num > 49:
            thread_n = len(points) // 50 + 1
        p_length = len(points)
        num = p_length // thread_n
        re_points = []
        remainder = p_length % thread_n
        count_num = []
        for i in range(thread_n):
            count_num.append(num)
        for i in range(remainder):
            count_num[i] += 1
        start = 0
        entities = []
        for i in range(thread_n):
            entities.append(points[start:start + count_num[i]])
            points_t = points[start]
            for j in range(start + 1, start + count_num[i]):
                points_t = points_t + "|" + points[j]
            re_points.append(points_t)
            start += count_num[i]
        return re_points, entities
