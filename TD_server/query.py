from flask import jsonify
import json

def query(sdclient, analyst_id, request):
    con = sdclient.con
    if analyst_id != -1 and not sdclient.analyst_exists(analyst_id):
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
    table_name = sdclient.get_table_name_from_query_plan(query_plan)
    print(table_name)

    if query_purpose != '':
        for id in result_df.loc[:, "User_ID"]:
            # store purposes as json object
            if query_purpose not in sdclient.get_purpose_array(table_name, id):
                result_df.drop(id, inplace=True)

    access_column = sdclient.get_access_column_for_table(table_name)
    if access_column and access_column in result_df.columns:
        result_df = result_df[result_df[access_column]]
    else:
        return jsonify({'error': f'Access column "{access_column}" not found in the result'}), 400

    sdclient.log_query(query, analyst_id, query_id, json.dumps(query_plan).replace("'", "''"))

    return jsonify({
        'message': 'Query executed successfully',
        'result': result_df.to_dict(orient='records')
    })

