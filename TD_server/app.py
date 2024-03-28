from flask import Flask, request, jsonify, render_template, session
import pandas as pd
import smokedduck
import os
import json
from smokedduck import ProgrammingError
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Initialize a smokedduck connection
con = smokedduck.prov_connect(database=':memory:', read_only=False)
con.execute("""
CREATE TABLE IF NOT EXISTS user (
    user_id INTEGER PRIMARY KEY,
    username TEXT UNIQUE,
    admin BOOLEAN,
    password TEXT
);
""")
con.execute("""
CREATE TABLE IF NOT EXISTS query_log (
    log_id INTEGER PRIMARY KEY,
    query_id INTEGER NOT NULL,
    query_text TEXT NOT NULL,
    query_plan TEXT NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES user(user_id)
);
""")
con.execute("INSERT INTO user (user_id, username, admin, password) VALUES (?, ?, ?, ?)", None, (0, "William", False, "Password"))

uploaded = False

def analyst_exists(analyst_id):
    # Execute a query to find if the analyst_name exists
    result = con.execute("SELECT EXISTS(SELECT 1 FROM user WHERE user_id = ? LIMIT 1)", None, (analyst_id,)).fetchone()[0]
    return result != None

def generate_new_user_id():
    result = con.execute("SELECT MAX(user_id) FROM user", None).fetchone()[0]
    if(result != None):
        return result + 1
    else:
        return 0
    
def generate_new_log_id():
    print('bar')
    result = con.execute("SELECT MAX(log_id) FROM query_log", None).fetchone()[0]
    print('baz')
    if(result != None):
        return result + 1
    else:
        return 0
    
def log_query(query, user_id=None, query_id=None, query_plan=None):
    print(query, user_id, query_id, query_plan)
    log_id = generate_new_log_id()

    print('bin')
    #con.execute("SELECT 1")
    query = query.replace("'", "''")
    con.execute(f"INSERT INTO query_log (query_id, query_text, query_plan, user_id, log_id) VALUES ({query_id}, '{query}', '{query_plan}', '{user_id}', {log_id})")
    print('foo')
    print(con.execute("SELECT * FROM query_log"), None)

@app.route('/')
def hello_world():
   return render_template('welcome.html')  

@app.route('/upload')
def upload():
    try:
        # Attempt to select from the table.
        df = con.execute("SELECT * FROM df_global2").df()
        return render_template('display_df.html', df=df.to_html(classes='table'))
    except ProgrammingError:
        # If the table does not exist, catch the exception and return the upload template.
        return render_template('upload_csv.html')
   

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

@app.route('/login-backend', methods=['POST'])
def login_backend():
    data = request.get_json() 
    username = data['username']
    password = data['password']
    
    user = con.execute('SELECT * FROM user WHERE username = ?', None, (username,)).fetchone()

    if user and (password == user[3]):
        return jsonify({'success': True, 'message': 'Login successful'}), 200
    else:
        return jsonify({'success': False, 'message': 'Invalid username or password'}), 401

@app.route('/upload-csv/<analyst_name>', methods=['POST'])
def upload_csv(analyst_name):
    print("uploading")
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        filename = file.filename
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        # Load the CSV file into a pandas DataFrame
        df_global = pd.read_csv(file_path)

        # Register the DataFrame as a table in smokedduck
        con.register('df_global', df_global)
        con.execute('CREATE TABLE df_global2 AS (SELECT * FROM df_global)')
        uploaded = True
        return render_template('display_df.html', df=df_global.to_html(classes='table'))
        return jsonify({
            'message': 'File successfully uploaded and DataFrame registered in smokedduck',
            'shape': df_global.shape,
            'columns': df_global.columns.tolist()
        })

@app.route('/query-df/<analyst_id>', methods=['POST'])
def query_df(analyst_id):
    if not analyst_exists(analyst_id):
        return jsonify({'error': f'Analyst name {analyst_id} not found'}), 404

    data = request.json
    if not data or 'query' not in data:
        return jsonify({'error': 'No SQL query provided'}), 400

    query = data['query']
    #try:
        # Execute the query using smokedduck

    result_df = con.execute(query, capture_lineage='lineage').df()
    
    #selected_a = con.lineage()

    log_query(query, analyst_id, con.query_id, json.dumps(con.query_plan).replace("'", "''"))
    return jsonify({
        'message': 'Query executed successfully',
        'result': result_df.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
    })
    #except Exception as e:
     #   return jsonify({'error': str(e)}), 400

@app.route('/add-user', methods=['POST'])
def add_user():
    data = request.get_json()  # Get data from the request body

    if not data or 'username' not in data or 'admin' not in data:
        return jsonify({'error': 'Missing username or admin status'}), 400

    user_id = generate_new_user_id()
    username = data['username']
    admin = data['admin']

    try:
        con.execute("INSERT INTO user (user_id, username, admin) VALUES (?, ?, ?)", None, (user_id, username, admin))
        return jsonify({'message': f'User {username} successfully added'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/user-queries/<int:user_id>', methods=['GET'])
def get_user_queries(user_id):
    if not analyst_exists(user_id):
        return jsonify({'error': f'User ID {user_id} not found'}), 404

    try:
        query_logs = con.execute("SELECT * FROM query_log WHERE user_id = ?", None, (user_id,)).fetchall()
        return jsonify([{'log_id': log[0], 'query_text': log[1], 'executed_at': log[2], 'user_id': log[3]} for log in query_logs])
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/provenance/<int:log_id>', methods=['GET'])
def get_query_log(log_id):
    query_log = con.execute(f"SELECT * FROM query_log WHERE log_id = {log_id}").df()
    
    # Set parameters for lineage tracing based on the query log
    con.query_id = query_log.loc[0, 'query_id']
    con.query_plan = json.loads(query_log.loc[0, 'query_plan'])
    
    # Perform lineage tracing
    query_lineage = con.lineage().df()
    print(query_lineage)

    # Fetch user data
    df_users = con.execute("SELECT * FROM df_global2").fetchdf()
    df_users = df_users.reset_index().rename(columns={'index': 'df_global2'})
    print(df_users)
    
    # Join the lineage data with user data
    df_joined = pd.merge(query_lineage, df_users, on='df_global2', how='left')
    print(df_joined)
    
    # Select only the Name and UserID columns
    df_filtered = df_joined[['Name', 'User_ID']]
    print(df_filtered)
    
    # Return the filtered DataFrame as JSON
    return jsonify(df_filtered.to_json(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
