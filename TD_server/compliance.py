from flask import jsonify

def get_user_by_name(user_name, sdclient):
    con = sdclient.con
    table_metadata_df = con.execute('select * from internal.table_metadata').df()
    # TODO: fix simplifying assumption that the user is only in a single table and that user data is only in each table once
    for _, row in table_metadata_df.iterrows():
        table_name = row['table_name']
        user_column = sdclient.get_user_column_for_table(table_name)

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

def delete_user_by_name(user_name, sdclient):
    con = sdclient.con
    table_metadata_df = con.execute('select * from internal.table_metadata').df()
    for _, row in table_metadata_df.iterrows():
        table_name = row['table_name']
        user_column = sdclient.get_user_column_for_table(table_name)

        query = f"DELETE FROM {table_name} WHERE {user_column} = ?"
        con.execute(query, None, (user_name,))
    return jsonify({'success': True, 'message': f'User "{user_name}" successfully deleted'}), 200  

