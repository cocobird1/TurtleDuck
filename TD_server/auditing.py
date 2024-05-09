from flask import jsonify
import json
import pandas as pd

def get_user_queries(user_id, sdclient):
    con = sdclient.con

    # Query the database for all queries made by the given user ID
    query_logs = con.execute("SELECT * FROM internal.query_log WHERE user_id = ?", None, (user_id,)).fetchall()

    # If query_logs is not empty, format the result
    if query_logs:
        queries = [{'log_id': log[0], 'query_id': log[1], 'query_text': log[2], 'query_plan': log[3], 'executed_at': log[4], 'user_id': log[5]} for log in query_logs]
        return jsonify(queries), 200
    else:
        return jsonify({'message': 'No queries found for this user'}), 404

def get_query_log(log_id, sdclient):
    con = sdclient.con

    print(con.execute("SELECT * FROM internal.query_log").df())
    query_log = con.execute(f"SELECT * FROM internal.query_log WHERE log_id = {log_id}").df()
    print(query_log)
    # Set parameters for lineage tracing based on the query log
    con.query_id = query_log.loc[0, 'query_id']
    con.query_plan = json.loads(query_log.loc[0, 'query_plan'])
    
    # Perform lineage tracing
    query_lineage = con.lineage().df()
    print(query_lineage)

    table_name = sdclient.get_table_name_from_query_plan(con.query_plan)

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
    df_filtered = df_joined[['Name', sdclient.get_user_column_for_table(table_name), 'Analyst']]
    print(df_filtered)

    # Return the filtered DataFrame as JSON
    return jsonify(df_filtered.to_json(orient='records'))
