from flask import Flask, request, jsonify, render_template
import os

import access_control
import compliance
import auditing
import query
from sd import SDClient
import upload


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

sdclient = SDClient()
con = sdclient.con  # TODO: remove to avoid busting interface after refactoring

@app.route('/')
def hello_world():
   return render_template('welcome.html')  

@app.route('/upload')
def upload_path():
    table_metadata_df = con.execute('select * from internal.table_metadata').df()
    return render_template('upload_csv.html', df=table_metadata_df.to_html(classes='table'))

@app.route('/login')
def login():
   return render_template('login.html')   

@app.route('/query')
def query_page():
   return render_template('write_query.html')   

@app.route('/get_queries')
def get_queries():
   return render_template('get_queries.html')  

@app.route('/provenance')
def get_prov():
   return render_template('provenance.html')   

@app.route('/compliance')
def get_comp():
   return render_template('compliance.html')   

@app.route('/access_control')
def get_ac():
   return render_template('access_control.html')   

@app.route('/login-backend', methods=['POST'])
def login_backend():
    data = request.get_json() 
    username = data['username']
    password = data['password']
    
    user = con.execute('SELECT * FROM internal.user WHERE username = ?', None, (username,)).fetchone()

    if user and (password == user[3]):
        return jsonify({'success': True, 'message': 'Login successful'}), 200
    else:
        return jsonify({'success': False, 'message': 'Invalid username or password'}), 401

@app.route('/upload-csv/<analyst_name>', methods=['POST'])
def upload_csv(analyst_name):
    return upload.upload(sdclient, analyst_name, request, app.config['UPLOAD_FOLDER'])

@app.route('/query-df/', defaults={'analyst_id': -1}, methods=['POST'])
@app.route('/query-df/<analyst_id>', methods=['POST'])
def query_df(analyst_id):
    return query.query(sdclient, analyst_id, request)

@app.route('/user-data-by-name/<user_name>', methods=['GET'])
def get_user_data_by_name(user_name):
    return compliance.get_user_by_name(user_name, sdclient)

@app.route('/delete-user-by-name/<user_name>', methods=['DELETE'])
def delete_user_by_name(user_name):
    return compliance.delete_user_by_name(user_name, sdclient)

@app.route('/user-queries/<int:user_id>', methods=['GET'])
def get_user_queries(user_id):
    return auditing.get_user_queries(user_id, sdclient)

@app.route('/provenance/<int:log_id>', methods=['GET'])
def get_query_log(log_id):
    return auditing.get_query_log(log_id, sdclient)

@app.route('/user-access/<user_id>', methods=['GET'])
def check_user_access(user_id):
    return access_control.check_user_access(user_id, sdclient)

if __name__ == '__main__':
    app.run(debug=True, port=5005)