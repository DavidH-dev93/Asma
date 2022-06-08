from flask import Flask,  render_template , url_for , redirect , flash, request, abort , jsonify, send_file
from server import app
import elasticfuncs
import background as b
import search as s
import update as u
from wtforms import StringField
from forms import SearchForm
from werkzeug.utils import secure_filename
import os, time, json
import ast

UPLOAD_FOLDER = app.root_path + app.static_url_path + '/uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

data = []

@app.context_processor
def headers():
    return dict(headers = [f for f in elasticfuncs.getFieldsForView()])

@app.route('/')
def home():
    global data
    data = []
    try:
        data = elasticfuncs.getInitialData(15)  
    except:
        data = []    
    fmap = elasticfuncs.convertFields()
    fields = elasticfuncs.getFieldsForView()    
    return render_template('index.html',data=data,fields=fields,fmap=fmap,elements=[], current_page=1)

@app.route('/partial', methods=['POST','GET'])
def partialShow():
    global data
    #data = request.form.getlist('data')
    element = request.form.get('element')
    #data = [ast.literal_eval(d) for d in data]
    fmap = elasticfuncs.convertFields()
    """elems = []
    for k,v in fmap.items():
        if v==element:
            elems.append(k)"""
    data = u.updatePartial(data,element)
    total = len(data)        
    viewSample = data[:100]        
    return render_template('index.html',data=viewSample,fmap=fmap,fields=elasticfuncs.getFieldsForView(),total=total,result=True) 

@app.route('/upload', methods=['POST','GET'])
def upload():
    i=0
    start_time = time.time()
    indices = elasticfuncs.getIndices()
    if request.method == 'POST':
        index = ''
        if request.form.get('indexChoice') != 'other':
            index = request.form.get('indexChoice').lower()
            elasticfuncs.deleteIndex(index)
        else:
            index = request.form.get('index').lower()
        uploaded_files = request.files.getlist("file[]")
        for file in uploaded_files:
            i = i + 1 
            if file and elasticfuncs.allowed_file(file.filename):
                print('loading file----->', file.filename)                   
                index = index.replace(" ","")
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                elasticfuncs.upload(app.root_path,UPLOAD_FOLDER + '/' + filename,index, False, filename)
                print("--- %s seconds ---" % (time.time() - start_time))
            if i == len(uploaded_files):
                return redirect(url_for('home'))
            
    return render_template('upload.html',indices=indices)

@app.route('/export', methods=['GET','POST'])
def export():   
    global data
    data = request.form.getlist('data')
    data = [ast.literal_eval(d) for d in data]
    exported = elasticfuncs.export(data,app.root_path)
    if exported:
        result = send_file(app.root_path + '/exported.csv', as_attachment=True,mimetype='text/csv')
        return result
    return redirect(url_for('home'))

@app.route('/status')
def status():
    status = elasticfuncs.getStatus()
    return render_template('status.html',status=status)

@app.route('/settings' , methods=['GET','POST'])
def settings():
    allowedFields = elasticfuncs.getFieldsForView()
    allowedFields = [f['field'] for f in allowedFields if f != 'facebook_UID']
    deleteColumn = request.args.get('delete_column')
    delete = request.args.get('delete')
    modify = request.args.get('modify')
    if request.method == 'POST':
        if modify:
            old = request.form.get('field')
            new = request.form.get('new')
            if elasticfuncs.modifyField(old,new):
                return redirect(url_for('home'))
        elif delete:
            target = request.form.get('index')
            print(target)
            elasticfuncs.deleteIndex(target)
            return redirect(url_for('home'))
        elif deleteColumn:
            field = request.form.get('field_d')
            if elasticfuncs.deleteField(field):
                return redirect(url_for('settings'))
            else:
                return redirect(url_for('home'))            
        else:
            name = request.form.get('name')
            if elasticfuncs.addField(name):
                return redirect(url_for('home'))
    return render_template('add.html',allowedFields=allowedFields,indices=elasticfuncs.getIndices())

@app.route('/search')
def search():
    global data
    data = []
    fmap = elasticfuncs.convertFields()
    if 'phone_FB' in request.args.get('options'):
        key = 'phone_FB'
    else:
        key = request.args.get('options')
    search = request.args.get('search')
    page = int(request.args.get('page', '1'))
    body = {'term': {key:search.lower()}}
    total, data, total_result= s.generalSearch(body=body, index=None,page=page)
    viewSample = data[:10000]
    total_page = int(total_result/1000)+1
    query_data = '&options='+key+'&search=' + search
    return render_template('index.html',
                            fmap=fmap,
                            data=viewSample,
                            result=True,
                            total=total,
                            total_page=total_page,
                            total_amount=total_result,
                            key=key,
                            search=search,
                            query_data=query_data,
                            search_type='search',
                            current_page=page,
                            fields=elasticfuncs.getFieldsForView(),elements=[])

@app.route('/searchForm', methods=['POST','GET'])
def searchForm():
    global data
    data = []
    fmap = elasticfuncs.convertFields()
    body = []
    if request.method == 'GET':
        if len(request.args.getlist('options')) == 0 and len(request.args.get('index', '')) == 0:
            return render_template('searchForm.html',indices=elasticfuncs.getIndices())
        else:
            options = request.args.getlist('options')
            choices = request.args.getlist('choice')
            page = int(request.args.get('page', '1'))
            index = request.args.get('index')
            print(options)
            for o in options:
                body.append({ 'term' : { o : choices[options.index(o)].lower()}})
            total, data, total_result = s.generalSearch(body=body,index=index, page=page)
            viewSample = data[:1000]
            total_page = int(total_result/1000)+1
            query_data='&index='+index
            for i in range(len(options)):
                query_data=query_data+'&options='+options[i]+'&choice='+choices[i]
            return render_template('index.html',
                                    fmap=fmap,
                                    data=viewSample,
                                    result=True,
                                    total=total,
                                    total_page=total_page,
                                    total_amount=total_result,
                                    query_data=query_data,
                                    current_page=page,
                                    search_type='searchForm',
                                    fields=elasticfuncs.getFieldsForView(),elements=[])

@app.route('/manualUpdate', methods=['POST','GET'])
def manualUpdate():
    if request.method == 'POST':
        
        index = request.form.get('index').lower()
        elasticfuncs.checkIndex(index, None)
        # elasticfuncs.checkIndexTask.apply_async((index,None))
        print('submit manual update!!!!!!!!!!!!!!!!!!')
        return redirect(url_for('home'))
    indices = elasticfuncs.getIndices()
    return render_template('update.html',indices=indices)

@app.route('/stopUpdates', methods=['POST','GET'])
def stopUpdates():
    os.system('pkill celery')
    return redirect(url_for('manualUpdate'))

@app.route('/keys', methods=['POST','GET'])
def keys():
    fields = elasticfuncs.getFieldsForView()
    if request.method == 'POST':
        choices = request.form.getlist('chosen')
        elasticfuncs.setKeys(choices)
        redirect(url_for('settings'))
    return render_template('keys.html',fields=fields)

@app.route('/seckeys', methods=['POST','GET'])
def seckeys():
    checked = elasticfuncs.getSecKeys()
    fields = [c['field'] for c in elasticfuncs.getFieldsForView()]
    if request.method == 'POST':
        choices = request.form.getlist('chosen')
        elasticfuncs.setSecKeys(choices)
        return redirect(url_for('settings'))
    return render_template('keys.html',fields=fields,checked=checked)   

@app.route('/priority', methods=['POST','GET'])
def priority():
    if request.method == 'POST':
        fs = elasticfuncs.getFieldsForView()
        fmap = elasticfuncs.convertFields()
        priorityList = []
        for f in fs:
            p = request.form.get(f['old'])
            if p == '':
                p = 0
            else:
                p = int(p)  
            priorityList.append({f['old']:p})    
        elasticfuncs.setPriorities(priorityList)  
        return redirect(url_for('settings'))    
    return render_template('priority.html',fields=elasticfuncs.getFieldsForView())    