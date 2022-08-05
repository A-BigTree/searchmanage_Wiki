# -*- coding:utf-8 -*-
# @author  : Shuxin_Wang
# @email   : 213202122@seu.edu.cn
# @time    : 2022/7/21
# @function: test for searchmanage
# @version : V0.4.5
#

from SearchManage import SearchManage, Wikipedia, SparqlQuery, BingQuery, SpellCheck

if __name__ == "__main__":

    # Example data
    p1 = [['SEU', 'Chain', 'English'], ['computer', 'games', 'computer game'], ['graph', 'wikipedia'],
          ['SEU', 'Chain', 'English'], ['computer', 'games', 'computer game'], ['graph', 'wikipedia'],
          ['SEU', 'Chain', 'English'], ['computer', 'games', 'computer game'], ['graph', 'wikipedia']]
    p2 = ["Q3918", "Q355304", "Q106589826", "Q3918", "Q355304", "Q106589826", "Q3918", "Q355304",
          "Q106589826", "Q3918", "Q355304", "Q106589826"]

    # SearchManage: key = 'search'
    s1 = SearchManage(key='search', m_num=10)
    r1 = s1.search_run(p1, keys='all', limit=20)
    # print(r1['label'])

    # SearchManage: key = 'ids'
    s2 = SearchManage(key='ids', m_num=20)
    r2 = s2.search_run(r1['id'], keys=['labels/en', 'claims/P//'])
    # print(r2)

    # Wikipedia: <wikipedia.suggest()>
    w1 = Wikipedia(m_num=10)
    # r3 = w1.search_run(p1)
    # print(r3)

    # SparqlQuery: <sparql_ = SparqlQuery.SPARQL_>
    sql1 = SparqlQuery(m_num=10, format_='json')
    # r4 = sql1.search_run(p2[0:3], timeout=60)
    # print(r4)

    # BingQuery: <url_ = BingQuery.URL_>
    b1 = BingQuery(m_num=24)
    r5 = b1.search_run(p1[0], timeout=100)
    print(r5)

    p3 = [["elgant palm trre garden", "elgant palm trre", "the southeast university"],
          ["elgant", "elgant trre", "the sotheast univrssity"],
          ["elgat palm trre garden", "elgat palm trre", "the sothaast univrsity"]]
    # SpellCheck <url_ = "https://www.bing.com/search">
    sc = SpellCheck(m_num=12)
    r6 = sc.search_run(p3, timeout=60)
    print(r6)
