# Does NOT implement the PEP 249 spec, but the return type is suggested by the .fetchall function as specified here: https://www.python.org/dev/peps/pep-0249/#fetchall

import time
import boto3

# query_string: a SQL-like query that Athena will execute
# client: an Athena client created with boto3
def fetch_user_events(client, query_string, database_name, output_bucket):

    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': 'database_name'
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + output_bucket
        }
    )['QueryExecutionId']
    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_string))
        time.sleep(10)
    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )
    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data_list.append(row['Data'])
    for datum in data_list[1:]:
        results.append([x['VarCharValue'] for x in datum])
    return [tuple(x) for x in results]