from elasticsearch import Elasticsearch, helpers
import csv,sys,math,json
import pandas as pd
from server import db
from models import Fields, FieldsMap, Keys, Tasks, LastField
import time,os, random, subprocess
import bulk as bk
from multiprocessing import Pool, Manager
from datetime import datetime
from celery import task, current_app, group
from celery.signals import task_success
from celery.result import AsyncResult
from celery.task.control import revoke
from server import celery
import update as u
import global_ as g
import search as s

ALLOWED_EXTENSIONS = {'csv'}
csv.field_size_limit(sys.maxsize)

es = g.es
manager = Manager()
result_ = ''

"""def getIndices():
    tmp = es.indices.get_alias("*")
    return list(set([i.split('_')[0] for i in tmp if i[0] != '.']))"""

def getIndexCount(index):
    indices = [i for i in getLetterIndices() if index + '_' in i]
    count = 0
    for i in indices:
        count += int(es.cat.count(i , params={"format": "json"})[0]['count'])
    print(count)    

def getIndices(): 
    tmp = es.indices.get_alias("*")
    return [i for i in tmp if i[0] != '.']

def getIndexDate(index):
    s = es.cat.indices(index=index,h = ("creation.date.string"),s="creation.date")
    return datetime.strptime(s,'%Y-%m-%dT%H:%M:%S.%fZ\n')

def getFieldsForView():
    fields = FieldsMap.query.all()
    for f in fields[:]:
        fields[fields.index(f)] = {'field' : f.new, 'old': f.old, 'priority': f.priority} 
    """for f in fields:
        for h in [x for x in fields if x != f]:
            if h['field'] == f['field']:
                f['old'].extend(h['old'])
                fields.remove(h)"""

    return list(sorted(fields, key = lambda i: i['priority'], reverse=True))
    #return [f.new for f in fields]

def getFields():
    fields = Fields.query.all()
    return [f.name for f in fields]

def convertFields():
    fmap = {}
    f = FieldsMap.query.all()
    for field in f:
        fmap[field.old] = field.new    
    return fmap

def setPriorities(plist):
    for p in plist:
        for k,v in p.items():
            print(k)
            f = FieldsMap.query.filter_by(old=k).first()
            f.priority = v
            db.session.commit()
            break


def setKeys(array):
    Keys.query.filter_by(isKey=1).delete()
    db.session.commit()
    for k in array:
        if Keys.query.filter_by(key=k).first():
            Keys.query.filter_by(key=k).delete()
        tmp = Keys(key=k,isKey=1)
        db.session.add(tmp)
        db.session.commit()

def getKeys():
    keys = Keys.query.filter_by(isKey=1).all()
    return [k.key for k in keys]        

def setSecKeys(array):
    Keys.query.filter_by(isKey=0).delete()
    db.session.commit()
    for k in array:
        if Keys.query.filter_by(key=k).first():
            Keys.query.filter_by(key=k).delete()
        tmp = Keys(key=k,isKey=0)
        db.session.add(tmp)
        db.session.commit()

def getSecKeys():
    keys = Keys.query.filter_by(isKey=0).all()
    return [k.key for k in keys]         

def stdFields():
    ret = []
    res = getInitialData(1)
    for k,v in res[0].items():
        ret.append(k)
    return ret

def deleteIndex(index):
    es.indices.delete(index=index, ignore=[404,400])
    """count = Tasks.query.filter_by(index=index).count()
    for i in range(count):
        revoke(index+str(i), terminate=True)"""
    while Tasks.query.filter_by(index=index).first():
        Tasks.query.filter_by(index=index).delete()
        db.session.commit()


def checkFile(path,index):
    with open(path,encoding="utf-8-sig") as f:
        exists = checkExistant(reader=csv.DictReader(f),index=index)
        return exists

def addTask(index,number,lines):
    Tasks.query.filter_by(index=index,number=number).delete()
    db.session.commit()
    stat = Tasks(index=index,number=number,lines=lines)
    db.session.add(stat)
    db.session.commit()

@task
def checkIndexTask(index, lines):
    print("Checking Index task")
    checkIndex(index, lines)
    return

def checkIndex(index,lines):
    print('update------------checkIndex----------')
    if lines == None:
        lines = es.cat.count(index, params={"format": "json"})
        lines = lines[0]['count']
    print('------------lines------------', lines)
    total, index_data = s.generalIndexListAll(index)
    print('--------index----------', index_data, total)
    print('index_data result--------', len(index_data))
    result = updatePart(index,index_data)
    return True

def updatePart(index,index_data):
    print('part')
    exists = checkExistant(reader=index_data,index=index)
    return True

@task()
def updateAll(dir,f,index):
    checkFile(dir+f,index)
    return True

@task_success.connect()
def task_succeeded(result, sender=None,**kwargs):
    print("result: " + str(result))
    return True

def getStatus():
    tasks = Tasks.query.all()
    db.session.commit()
    status = []
    for i in getIndices():
        s = {i:'PENDING'}
        for j in tasks:
            if j.index == i:
                s['total'] = j.lines
                if j.number == 0:
                    s[i] = 'PENDING'
                elif j.number == 1:
                    s[i] = 'SUCCESS'    
                """if celery.AsyncResult(i+str(j.number)).state != 'SUCCESS':
                    s[i] = False
                    break"""
        if s[i] != True:
            status.append(s)

    return status

def getCpus(files):
    if files > os.cpu_count():
        return os.cpu_count()
    else:
         return files    
    
def upload(rootPath,filePath,index, update, filename):
    print('start upload')
    lines = 0
    print("Processing started")
    processUploaded.apply_async((index, lines, update, filename), countdown=1)
    return

@task
def processUploaded(index, lines, update, filename):
    print("start processUploaded")
    bk.ingest(index, filename, getIndices())
    if update:
        print('updating...')
        result = checkIndex(index,lines)
    return

def checkHeader(file):
    last = LastField.query.order_by(-LastField.id).first()
    with open(file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
    found = False    
    for h in headers: 
        if h in getFields():
            found = True
    print(found)        
    if not found:
        headerList = [last.last + i for i in range(len(headers))]
        for h in headerList:
            addField(str(h))
        last.last = len(headers)
        db.session.commit()
        headerList = ','.join(str(e) for e in headerList)
        cmd = 'sed -i 1i' + '"'+ headerList +'"' + ' ' + file
        print(cmd)
        try:
            os.system(cmd)
        except:
            pass    

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def addField(name):
    try:
        f = Fields(name=name)
        fmap = FieldsMap(old=name,new=name,priority=0)
        db.session.add(f)
        db.session.add(fmap)
        db.session.commit()
        return True
    except:
        return False

def deleteField(f):
    try:
        #this has to be changed to new after modification of getFields
        targetInMap = FieldsMap.query.filter_by(new=f).first()
        Fields.query.filter_by(name=targetInMap.old).delete()
        FieldsMap.query.filter_by(new=f).delete()
        Keys.query.filter_by(key=f).delete()
        db.session.commit()
        return True
    except Exception as e:
        print(e)
        return False          

def modifyField(old,new):
    try:
        f = FieldsMap.query.filter_by(new=old).first()
        f.new = new
        db.session.commit()
        return True
    except:
        return False

def checkExistant(reader,index='all'):
    try:
        for row in reader:
            must = []
            should = []
            for r in row:
                if row[r] == '' or row[r] == None or row[r] == 'None':
                    pass
                elif r in getSecKeys(): 
                    should.append({'term' : {r:row[r].lower()}})
            try:
                ilist = [i for i in getIndices() if i != index]    
                indices, total, data = searchData(must=must,should=should,indexList=ilist)
                if len(data) > len([]) and len(indices) > len([]) and indices != [index]:
                    for d in data:
                        if 'null' in d.keys():
                            del d["null"]
                        new = u.replaceExistant(d,row,indices,index=index,sk=getSecKeys(),es=es)
                else:
                    continue
            except Exception as e:
                print(e)
                print('index' + str(i))
                print(indices)
                print(total)
                print(data)
                continue        
        return True
    except Exception as e:
        print(str(e))
        return False

def searchData(must={},should={},indexList=[]):
    query = {
            "query": {
                "bool": {
                    "should": should
                },
            },
    }
    hits = []
    indices_exist = []
    indices = indexList
    try:
        total = 0
        if len(indexList) != len([]):
            for i in indexList:
                res = ''
                try:
                    res = es.search(index=i,size=10000, body=query)
                except:
                    continue
                if res['hits']['total']['value'] != 0:
                    indices_exist.append(i)     
                total +=  res['hits']['total']['value']
                for hit in res['hits']['hits']:
                    hits.append(hit["_source"])

        hits = [i for n, i in enumerate(hits) if i not in hits[n + 1:]]
        return indices_exist, total, hits
    except Exception as e:
        print('here' + str(e))
        return [],0, []



def getInitialData(size):
    hits = []
    res = ''
    body = {
        "from" : 0, "size": size,
        "query": {
            "match_all": {}
        },
    }
    indices = getIndices()
    i = random.choice(indices)
    try:
        res = es.search(index=i, body=body)
    except Exception as e:
        print(e)
    for hit in res['hits']['hits']:
        hits.append(hit['_source'])
    return [i for n, i in enumerate(hits) if i not in hits[n + 1:]][0:size]

exportList = manager.list()
root_ = ''

def export(data,root):
    global root_
    root_ = root
    try:
        # try:
            # os.system('rm ' + root + '/exports/*')
            # os.system('touch ' + root + '/exports/exported.csv')
        # except:
        #     pass 
        nproc = 1
        pool_ = Pool(nproc)
        chunk = math.ceil(len(data)/nproc)
        last = (len(data)//nproc)
        for i in range(nproc):
            if i == nproc - 1:
                pool_.apply_async(removeNullElems, (i,data[i*chunk:],root))
            else:
                pool_.apply_async(removeNullElems, (i,data[i*chunk:i*chunk + chunk], root))
        pool_.close()
        pool_.join()
        os.system('cat ' + root + '/exports/* > ' + root +'/exported.csv')
        return True
    except Exception as e:
        print(e)
        return False

def removeNullElems(i,dataSlice, root):
    global root_
    for d in dataSlice[:]:
        if 'null' in d.keys():
            del dataSlice[dataSlice.index(d)]['null']
    header = dataSlice[0].keys()
    with open(root + '/exports/exported.csv', 'w+') as f:
        writer = csv.writer(f, delimiter=',')
        if i == 0:
            writer.writerow(header)
        for d in dataSlice:
            writer.writerow(d.values())
    # exportList.extend(dataSlice)

def makeLoadable(d):
    d = d.replace(" \'", " \"")
    d = d.replace("\',", "\",")
    d = d.replace("{\'", "{\"")
    d = d.replace("[\'", "[\"")
    d = d.replace("\']", "\"]")
    d = d.replace("\'}", "\"}")
    d = d.replace("\':", "\":")
    d = d.replace("None", '""')
    return d 