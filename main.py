import json
import boto3
import os
from athena import fetch_user_events

def handler(event, context):

    athenaDatabase = os.environ['athenaDatabase']
    table = os.environ['table']
    outputBucket = os.environ['outputBucket']
    deletedUser = json.loads(json.dumps(event))['detail']['requestParameters']['userName']


    client = boto3.client('athena')
    query_string = \
        """SELECT userIdentity.userName, eventName, eventSource, requestParameters
        FROM %s.%s 
        WHERE userIdentity.userName = %s and
        (eventName like 'Create%' or eventName like 'Run%');""" % (athenaDatabase, table, deletedUser)
    results = fetch_user_events(client, query_string, athenaDatabase ,outputBucket)
    print (results)