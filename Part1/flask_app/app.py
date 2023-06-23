from flask import Flask, request, jsonify

import requests
from celery import Celery

app = Flask(__name__)
simple_app = Celery('tasks', broker='redis://10.10.66.193:6379/0', backend='redis://10.10.66.193:6379/0')


@app.route('/analyze_file', methods=['POST'])
def analyze_file():
    if 'file' not in request.files:
        return 'No file uploaded', 400

    file = request.files['file']
    if file.filename == '':
        return 'No file selected', 400
    bulan = request.form.get('bulan')
    if bulan == '':
        return 'No bulan selected', 400
    tahun = request.form.get('tahun')
    if tahun == '':
        return 'No tahun selected', 400
    jenis = request.form.get('jenis')
    if jenis == '':
        return 'No jenis selected', 400
    typename = request.form.get('typename')
    if typename == '':
        return 'No typename selected', 400
    indexname = jenis+'-'+bulan+'-'+tahun+'-'+typename
    file_data = file.read()
    r = simple_app.send_task('tasks.process_task', kwargs={'file': file_data, 'indexname': indexname})
    app.logger.info(r.backend)
    return jsonify({'status':"SUCCES",'id':r.id})

@app.route('/task_status/<task_id>')
def get_status(task_id):
    status = simple_app.AsyncResult(task_id, app=simple_app)
    print("Invoking Method ")
    return jsonify({'status':str(status.state)})

@app.route('/cancel_upload/<task_id>')
def cancel_upload(task_id):
    result = simple_app.AsyncResult(task_id)
    
    if result.state == 'PENDING':
        # Hentikan tugas yang sedang berjalan
        result.revoke(terminate=True)
        return jsonify({'status':f"Upload task with ID {task_id} has been canceled."})
    
    return f"Cannot cancel task with ID {task_id}. Task is not in PENDING state."


@app.route('/task_result/<task_id>')
def task_result(task_id):
    result = simple_app.AsyncResult(task_id).result
    # return "Result of the Task " + str(result)
    return jsonify({'status':str(result)})

if __name__ == '__main__':
    app.run(debug=True)
