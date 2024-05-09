from flask import jsonify

def check_user_access(user_id, sdclient):
    con = sdclient.con

    table_metadata_df = con.execute('select * from internal.table_metadata').df()
    for _, row in table_metadata_df.iterrows():
        table_name = row['table_name']

        # Assuming each table has a column to identify users (like 'user_id') and a column for access status ('access_column').
        user_column = sdclient.get_user_column_for_table(table_name)
        access_column = sdclient.get_access_column_for_table(table_name)

        if not user_column or not access_column:
            return jsonify({'error': 'User or access column not configured'}), 500

        # Fetch the user's access status based on the user ID.
        query = f"SELECT {access_column} FROM {table_name} WHERE {user_column} = ?"
        result = con.execute(query, None, (user_id,)).fetchone()

        if result:
            access_status = result[0]
            return jsonify({'user_id': user_id, 'access': access_status}), 200
    return jsonify({'error': 'User not found'}), 404
