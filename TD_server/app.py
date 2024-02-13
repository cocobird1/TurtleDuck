from flask import Flask, request, jsonify
import pandas as pd
import smokedduck
from werkzeug.utils import secure_filename
import os

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Initialize a smokedduck connection
con = smokedduck.prov_connect(database=':memory:', read_only=False)
con.execute("""
CREATE TABLE IF NOT EXISTS user (
    user_id INTEGER PRIMARY KEY,
    admin BOOLEAN
);
""")

def analyst_exists(analyst_name):
    # Query the user table to check if the analyst_name exists
    result = con.execute("SELECT COUNT(*) AS count FROM user WHERE username = ?", (analyst_name,)).fetchone()
    return result[0] > 0

@app.route('/upload-csv/<analyst_name>', methods=['POST'])
def upload_csv(analyst_name):
    if not analyst_exists(analyst_name):
        print("hi")
 #       return jsonify({'error': f'Analyst name {analyst_name} not found'}), 404

    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        # Load the CSV file into a pandas DataFrame
        df_global = pd.read_csv(file_path)

        # Register the DataFrame as a table in smokedduck
        con.register('df_global', df_global)

        return jsonify({
            'message': 'File successfully uploaded and DataFrame registered in smokedduck',
            'shape': df_global.shape,
            'columns': df_global.columns.tolist()
        })

@app.route('/query-df/<analyst_name>', methods=['POST'])
def query_df(analyst_name):
  #  if not analyst_exists(analyst_name):
    #    return jsonify({'error': f'Analyst name {analyst_name} not found'}), 404

    data = request.json
    if not data or 'query' not in data:
        return jsonify({'error': 'No SQL query provided'}), 400

    query = data['query']
    try:
        # Execute the query using smokedduck
        result_df = con.execute(query).df()

        return jsonify({
            'message': 'Query executed successfully',
            'result': result_df.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
