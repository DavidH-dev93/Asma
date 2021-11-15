from elasticsearch import Elasticsearch, helpers
import csv,sys,math,json,time,os,threading
from multiprocessing import Pool
import pathlib

PATH = '/home/goodluck/work/test.csv'
DIR = '/home/goodluck/work/asma/static/uploads'
es = Elasticsearch(host = "localhost", port = 9200)
csv.field_size_limit(sys.maxsize)

def ingestFile(file,i):
    print(file)
    with open(file, encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for success, info in helpers.parallel_bulk(es, actions=reader, index=i,thread_count=1,chunk_size=1000):
            pass

def ingest(index, filename, existant):
    start_time = time.time()
    shards = 1
    if index not in existant:            
        es.indices.create(index=index, body={
            'settings' : {
                'index' : {
                    'number_of_shards': shards
                }
            }
        })
    ingestFile(DIR+"/"+filename, index)
    es.indices.put_settings(index=index,
                            body= {"index" : {
                                    "max_result_window" : 100000
                             }})
    print("--- %s seconds ---" % (time.time() - start_time))
    return
