# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/9/12 
# @function: the script is used to do something.
# @version : V1.0 
#
import Levenshtein
from requests import Response
from searchmanage import SpellCheck
import lxml.etree

DATA = ["Neochmia uuficauda"]


def bing_page(request_: Response):
    res = None
    # print(request_.text)
    try:
        xpath_data = lxml.etree.HTML(request_.text)
        res = xpath_data.xpath("/html/body/div[1]/main/ol//h2//text()")
        print(res)
    except Exception as e:
        print(e)
    return res


def ask_page(request_: Response):
    res = None
    # print(request_.url)
    try:
        xpath_data = lxml.etree.HTML(request_.text)
        res = xpath_data.xpath("/html/body/div[2]/div/main/div/div[1]/section/div/div[4]/div/div[2]//a//text()")
        print(res)
    except Exception as e:
        print(e)
    return res


PAGE_BING = ['ADW: ', 'Neochmia', ' ruficauda: IN…', 'ADW: ', 'Neochmia',
             ' ruficauda: INFORMATION - Animal Diversity Web', 'Neochmia', ' ruficauda (Gould, 1837) - GBIF',
             'Star Finch (', 'Neochmia', ' ruficauda) :: xeno-canto', 'Neochmia ruficauda',
             ' - Wikispecies - Wikimedia', 'Star finch (', 'Neochmia',
             ' ruficauda) longevity, ageing, and life history', '(', 'Neochmia', ' ruficauda) - Wikit.wiki',
             'Star finches ', 'Neochmia', ' ruficauda have a visual preference for white dot ...', 'ADW：', 'Neochmia',
             ' Ruficauda：信息 - 德赢平台入口', 'Star finches ', 'Neochmia',
             ' ruficauda have a visual preference for ... - PubMed', 'neochmia', ' ruficauda | BirdForum']

PAGE_ASK = ['Neochmia - Wikipedia', 'Star Finch (Neochmia ruficauda) - Feathers on featherbase.info',
            'The star finch (Neochmia ruficauda) is a species of ... - Pinterest',
            'Star Finch (Neochmia ruficauda) - BirdLife species factsheet']

REMOVE_WORDS = [":", ",", ".", "-", "|", "…", "(", ")", "|", "：", "[", "]", "{", "}", "!"]

if __name__ == "__main__":
    # sp = SpellCheck(m_num=20)
    # sp_a = SpellCheck(url_="https://www.ask.com/web", m_num=20)
    # sp.search_run(DATA, function_=bing_page)
    # sp_a.search_run(DATA, function_=ask_page)
    temp = ""
    for pos in PAGE_BING + PAGE_ASK:
        t = pos
        for r in REMOVE_WORDS:
            t = t.replace(r, " ")
        temp += (" " + t.lower() + " ")
    re_ = []
    for pos in temp.split(" "):
        if pos != "" and pos not in re_:
            re_.append(pos)
    match_ = []
    t = DATA[0]
    temp = ""
    for r in REMOVE_WORDS:
        t = t.replace(r, " ")
    temp += (" " + t + " ")
    for pos in temp.split(" "):
        if pos != "":
            match_.append(pos.lower())
    print(match_)
    print(re_)
    r_ = []
    for m in match_:
        r_t = []
        dis = []
        for r in re_:
            dis.append(Levenshtein.distance(m, r))
        print(dis)
        dis_sort_index = sorted(range(len(dis)), key=lambda k: dis[k])
        print(dis_sort_index)

