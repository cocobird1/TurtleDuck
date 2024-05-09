from flask import jsonify, render_template
import os
import pandas as pd

def upload(sdclient, analyst_name, request, upload_folder):
    con = sdclient.con
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
        file_path = os.path.join(upload_folder, filename)
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
