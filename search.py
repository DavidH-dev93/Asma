from iteration_utilities import unique_everseen
from billiard import Pool, Manager
import threading, math, os
import global_ as g

SEARCH_MAX_COUNT = 1000
es = g.es 
manager = Manager()

def getIndices(): 
    tmp = es.indices.get_alias("*")
    return [i for i in tmp if i[0] != '.']

def search(index,body):
    hits = []
    body = buildQuery(body)
    res = es.search(index=index,body=body,scroll='1m')
    for hit in res['hits']['hits']:
        hits.append(hit['_source'])
    hits = [i for n, i in enumerate(hits) if i not in hits[n + 1:]]
    return len(hits), hits

def indexSearch(index,body,override=True):
    global hitsFromSlice
    if override:
        hitsFromSlice = manager.list()
    if index == 'all':
        return globalSearch(body,SEARCH_MAX_COUNT)
    slicedScroll(index,SEARCH_MAX_COUNT,os.cpu_count()-1,body)
    return len(hitsFromSlice) , hitsFromSlice

def generalSearch(index, body, page):
    if index == 'all':
        index = None
    result  = manager.list()
    query = generalQueryForSearch(index,body, page)
    hits = []
    res = es.search(index=index,body=query)
    hits.extend(processHits(res['hits']['hits']))
    result.extend(hits)
    total_result = res['hits']['total']['value']
    return len(result), result, total_result

def processHits(hits):
    final = [] 
    for hit in hits:
        final.append(hit['_source'])
    return final    

hitsFromSlice = manager.list()

def indexListAll(index,override=True):
    global hitsFromSlice
    if override:
        hitsFromSlice = manager.list()
    slicedScroll(index,SEARCH_MAX_COUNT,os.cpu_count() - 1)
    return len(hitsFromSlice), hitsFromSlice

def slicedScroll(index,size,maxt,query=None):
    pool = Pool(maxt)
    for i in range(maxt):
        if query == None:
            pool.apply_async(sliceSearch, (index,i,maxt,size))
        else:
            pool.apply_async(sliceSearch, (index,i,maxt,size,query))
    pool.close()
    pool.join()       

def sliceSearch(index,id,maxt,size,query=None):
    global hitsFromSlice
    if query != None:
        query = buildSlicedQueryForSearch(id,maxt,query)
    else:
        query = buildSlicedQuery(id,maxt)
    hits = []
    res = es.search(index=index,body=query,size=size,scroll='5m')
    sid = res['_scroll_id']
    scrollSize = len(res['hits']['hits'])
    while scrollSize:
        hits.extend(processHits(res['hits']['hits']))
        res = es.scroll(scroll_id=sid, scroll='5m')
        sid = res['_scroll_id']
        scrollSize = len(res['hits']['hits'])
    hitsFromSlice.extend(hits)    

def globalSearch(body,size=SEARCH_MAX_COUNT):
    global hitsFromSlice
    hitsFromSlice = manager.list()
    if body == 'FETCH':
        body = buildFetchQuery()
    indices = getIndices()
    t = 0
    h = []
    for i in indices:
        if body == buildFetchQuery():
            print('fetching...' + i)
            tmp , htmp = indexListAll(i,override=False)
            print(tmp)
        else:
            tmp , htmp = indexSearch(i,body,override=False)
            t += tmp
    return len(hitsFromSlice), hitsFromSlice

def buildFetchQuery():
    return {
        "query": {
            "match_all": {}
        }
    }

def buildQuery(body):
    return {
        "query": {
            "bool": {
                "must": body
            },
        },
    }

def buildSlicedQueryForSearch(id,maxt,body):

    return {
        "slice": {
            "field": "facebook_UID",
            "id": id, 
            "max": maxt 
        },       

        "query": {
            "bool": {
                "must": body
            },            
        },
    }    

def buildSlicedQuery(id,maxt):
    return {
        "slice": {
            "field": "facebook_UID",
            "id": id, 
            "max": maxt 
        },
        "query": {
            "match_all": {}
        }
    }

def generalQueryForSearch(index, body, page):
    return {
        "from": (page-1)*100,
        "size": 100,
        "profile": True,
        "query": {
            "bool": {
                "must": body
            },            
        },
    }