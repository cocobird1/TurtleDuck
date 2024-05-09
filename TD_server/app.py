from flask import Flask, request, jsonify, render_template
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
CREATE SCHEMA IF NOT EXISTS internal;
""")
con.execute("""
CREATE TABLE IF NOT EXISTS internal.user (
    user_id INTEGER PRIMARY KEY,
    username TEXT UNIQUE,
    admin BOOLEAN,
    password TEXT
);
""")
con.execute("""
CREATE TABLE IF NOT EXISTS internal.query_log (
    log_id INTEGER PRIMARY KEY,
    query_id INTEGER NOT NULL,
    query_text TEXT NOT NULL,
    query_plan TEXT NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES internal.user(user_id)
);
""")
con.execute("""
CREATE TABLE IF NOT EXISTS internal.table_metadata (
    table_name TEXT PRIMARY KEY,
    user_id_column TEXT,
    purpose_column TEXT,
    access_column TEXT
);
""")

con.execute("INSERT INTO internal.user (user_id, username, admin, password) VALUES (?, ?, ?, ?)", None, (0, "William", False, "Password"))
con.execute("INSERT INTO internal.user (user_id, username, admin, password) VALUES (?, ?, ?, ?)", None, (-1, "No Set Analyst", False, "SECRETPassword"))

uploaded = False

def analyst_exists(analyst_id):
    # Execute a query to find if the analyst_name exists
    result = con.execute("SELECT EXISTS(SELECT 1 FROM internal.user WHERE user_id = ? LIMIT 1)", None, (analyst_id,)).fetchone()[0]
    return result != None

def generate_new_user_id():
    result = con.execute("SELECT MAX(user_id) FROM internal.user", None).fetchone()[0]
    if(result != None):
        return result + 1
    else:
        return 0
    
def generate_new_log_id():
    result = con.execute("SELECT MAX(log_id) FROM internal.query_log", None).fetchone()[0]
    if(result != None):
        return result + 1
    else:
        return 0
    
def log_query(query, analyst_id, query_id, query_plan):
    log_id = generate_new_log_id()

    query = query.replace("'", "''")
    
    con.execute(f"INSERT INTO internal.query_log (query_id, query_text, query_plan, user_id, log_id) VALUES ({query_id}, '{query}', '{query_plan}', '{analyst_id}', {log_id})")
    print(con.execute("SELECT * FROM internal.query_log"), None)

@app.route('/')
def hello_world():
   return render_template('welcome.html')  

@app.route('/upload')
def upload():
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
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    table_name = request.form.get('table_name')
    if table_name == '':
        return jsonify({'error': 'No table name provided'}), 400
    user_id_column = request.form.get('userid_column')
    purpose_column = request.form.get('purposes_column')
    access_column = request.form.get('access_column')
    if(user_id_column == ''):
        user_id_column = "User_ID"
    if(purpose_column == ''):
        purpose_column = "Purposes_Column"
    if(access_column == ''):
        access_column = "Access_Column"
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        filename = file.filename
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        tmp = pd.read_csv(file_path)
        con.execute("""
        INSERT INTO internal.table_metadata (table_name, user_id_column, purpose_column, access_column) VALUES (?, ?, ?, ?)
        ON CONFLICT(table_name) DO UPDATE SET 
            user_id_column = excluded.user_id_column, 
            purpose_column = excluded.purpose_column,
            access_column = excluded.access_column
        """, None, (table_name, user_id_column, purpose_column, access_column))


        con.execute(f'CREATE TABLE {table_name} AS (SELECT * FROM tmp)')
        return render_template('display_df.html', df=tmp.to_html(classes='table'))

@app.route('/query-df/', defaults={'analyst_id': -1}, methods=['POST'])
@app.route('/query-df/<analyst_id>', methods=['POST'])
def query_df(analyst_id):
    if analyst_id != -1 and not analyst_exists(analyst_id):
        return jsonify({'error': f'Analyst name {analyst_id} not found'}), 404

    data = request.json
    if not data or 'query' not in data:
        return jsonify({'error': 'No SQL query provided'}), 400

    query = data['query']
    query_purpose = data['purpose']

    result_df = con.execute(query, capture_lineage='lineage').df()
    if 'User_ID' not in result_df.columns:
        return jsonify({'error': 'Query result must include a "User_ID" column'}), 400

    query_id = con.query_id
    query_plan = con.query_plan
    table_name = get_table_name_from_query_plan(query_plan)
    print(table_name)

    if query_purpose != '':
        for id in result_df.loc[:, "User_ID"]:
            # store purposes as json object
            if query_purpose not in get_purpose_array(table_name, id):
                result_df.drop(id, inplace=True)

    access_column = get_access_column_for_table(table_name)
    if access_column and access_column in result_df.columns:
        result_df = result_df[result_df[access_column]]
    else:
        return jsonify({'error': f'Access column "{access_column}" not found in the result'}), 400

    log_query(query, analyst_id, query_id, json.dumps(query_plan).replace("'", "''"))

    return jsonify({
        'message': 'Query executed successfully',
        'result': result_df.to_dict(orient='records')
    })

@app.route('/add-user', methods=['POST'])
def add_user():
    data = request.get_json()  # Get data from the request body

    if not data or 'username' not in data or 'admin' not in data:
        return jsonify({'error': 'Missing username or admin status'}), 400

    user_id = generate_new_user_id()
    username = data['username']
    admin = data['admin']

    try:
        con.execute("INSERT INTO internal.user (user_id, username, admin) VALUES (?, ?, ?)", None, (user_id, username, admin))
        return jsonify({'message': f'User {username} successfully added'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/user-data-by-name/<user_name>', methods=['GET'])
def get_user_data_by_name(user_name):
    table_metadata_df = con.execute('select * from internal.table_metadata').df()
    # TODO: fix simplifying assumption that the user is only in a single table and that user data is only in each table once
    for _, row in table_metadata_df.iterrows():
        table_name = row['table_name']
        user_column = get_user_column_for_table(table_name)

        query = f"SELECT * FROM {table_name} WHERE {user_column} = ?"
        user_data = con.execute(query, None, (user_name,)).fetchone()

        if user_data:
            # Convert user_data tuple to a dictionary for jsonify to work correctly.
            # This assumes you know the structure of your user_data, i.e., the columns returned by your query.
            # For example, if your columns are user_id, username, full_name, and admin, adjust accordingly.
            user_data_dict = {
                'user_id': user_data[0],
                'username': user_data[1],
                'full_name': user_data[2],
                # Add or adjust fields according to the actual structure of your user_data
            }
            return jsonify(user_data_dict), 200
    return jsonify({'error': 'User not found'}), 404

@app.route('/delete-user-by-name/<user_name>', methods=['DELETE'])
def delete_user_by_name(user_name):
    table_metadata_df = con.execute('select * from internal.table_metadata').df()
    for _, row in table_metadata_df.iterrows():
        table_name = row['table_name']
        user_column = get_user_column_for_table(table_name)

        query = f"DELETE FROM {table_name} WHERE {user_column} = ?"
        con.execute(query, None, (user_name,))
    return jsonify({'success': True, 'message': f'User "{user_name}" successfully deleted'}), 200

def get_table_name_from_query_plan(query_plan):
    table_names = []
    if query_plan['table'] != '':
        table_names.append(query_plan['table'])
    for child in query_plan['children']:
        child_table_names = get_table_name_from_query_plan(child)
        table_names.extend(child_table_names)
    assert len(table_names) < 2, 'Multiple tables found in query plan'
    return table_names[0] if table_names else None

def get_user_column_for_table(table_name):
    user_id_column = con.execute(f"SELECT user_id_column FROM internal.table_metadata WHERE table_name = '{table_name}'").fetchone()
    return user_id_column[0] if user_id_column else None

def get_purposes_column_for_table(table_name):
    purposes_column = con.execute(f"SELECT purpose_column FROM internal.table_metadata WHERE table_name = '{table_name}'").fetchone()
    return purposes_column[0] if purposes_column else None

def get_access_column_for_table(table_name):
    access_column = con.execute(f"SELECT access_column FROM internal.table_metadata WHERE table_name = '{table_name}'").fetchone()
    return access_column[0] if access_column else None

def get_purpose_array(table_name, user_id):
    purpose_column_name = get_purposes_column_for_table(table_name)
    user_column_name = get_user_column_for_table(table_name)
    
    if purpose_column_name and user_column_name:
        query = f"SELECT {purpose_column_name} FROM {table_name} WHERE {user_column_name} = ?"
        result = con.execute(query, None, (user_id,)).fetchall()
        
        purpose_set = set()
        
        for row in result:
            if row[0]:
                purposes = row[0].split(',')
                purpose_set.update(purpose.strip() for purpose in purposes)  # Remove any surrounding whitespace

        return list(purpose_set)
    else:
        return []
  


@app.route('/user-data/<int:user_id>', methods=['GET'])
def get_user_data(user_id):
    print("hi")
    user_data = con.execute("SELECT * FROM internal.user WHERE User_ID = ?", None, (user_id,)).fetchone()
    print(user_data)
    if user_data is None:
        return jsonify({'error': 'User not found'}), 404

    user_response = {
        'user_id': user_data[0],
        'username': user_data[1],
        'admin': user_data[2]
    }
    
    return jsonify(user_response), 200

@app.route('/user-queries/<int:user_id>', methods=['GET'])
def get_user_queries(user_id):
    try:
        # Query the database for all queries made by the given user ID
        query_logs = con.execute("SELECT * FROM internal.query_log WHERE user_id = ?", None, (user_id,)).fetchall()

        # If query_logs is not empty, format the result
        if query_logs:
            queries = [{'log_id': log[0], 'query_id': log[1], 'query_text': log[2], 'query_plan': log[3], 'executed_at': log[4], 'user_id': log[5]} for log in query_logs]
            return jsonify(queries), 200
        else:
            return jsonify({'message': 'No queries found for this user'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/provenance/<int:log_id>', methods=['GET'])
def get_query_log(log_id):
    query_log = con.execute(f"SELECT * FROM internal.query_log WHERE log_id = {log_id}").df()
    print(query_log)
    # Set parameters for lineage tracing based on the query log
    con.query_id = query_log.loc[0, 'query_id']
    con.query_plan = json.loads(query_log.loc[0, 'query_plan'])
    
    # Perform lineage tracing
    query_lineage = con.lineage().df()
    print(query_lineage)

    table_name = get_table_name_from_query_plan(con.query_plan)

    # Fetch user data
    df_users = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    df_users = df_users.reset_index().rename(columns={'index': table_name})
    print(df_users)
    
    # Join the lineage data with user data
    df_joined = pd.merge(query_lineage, df_users, on=table_name, how='left')
    df_joined['Analyst'] = query_log.loc[0, 'user_id']
    print("joined")
    print(df_joined)
    
    # Select only the Name and UserID columns
    df_filtered = df_joined[['Name', get_user_column_for_table(table_name), 'Analyst']]
    print(df_filtered)

    # Return the filtered DataFrame as JSON
    return jsonify(df_filtered.to_json(orient='records'))

@app.route('/user-access/<user_id>', methods=['GET'])
def check_user_access(user_id):
    table_metadata_df = con.execute('select * from internal.table_metadata').df()
    for _, row in table_metadata_df.iterrows():
        table_name = row['table_name']

        # Assuming each table has a column to identify users (like 'user_id') and a column for access status ('access_column').
        user_column = get_user_column_for_table(table_name)
        access_column = get_access_column_for_table(table_name)

        if not user_column or not access_column:
            return jsonify({'error': 'User or access column not configured'}), 500

        # Fetch the user's access status based on the user ID.
        query = f"SELECT {access_column} FROM {table_name} WHERE {user_column} = ?"
        result = con.execute(query, None, (user_id,)).fetchone()

        if result:
            access_status = result[0]
            return jsonify({'user_id': user_id, 'access': access_status}), 200
    return jsonify({'error': 'User not found'}), 404

if __name__ == '__main__':
    app.run(debug=True, port=5005)