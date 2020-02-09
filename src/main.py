import json
import boto3
import os
from athena import fetch_user_events

def handler(event, context):
    
    client = boto3.client('athena')
    
    athenaDatabase = os.environ['athenaDatabase']
    athenaTable = os.environ['athenaTable']
    outputBucket = os.environ['outputBucket']
    deletedUser = json.loads(json.dumps(event))['detail']['requestParameters']['userName']

    query_string = "SELECT userIdentity.userName, eventName, eventSource, requestParameters FROM \"{}\".\"{}\" WHERE userIdentity.userName = \'{}\' and (eventName like 'Create%' or eventName like 'Run%');".format(athenaDatabase, athenaTable, deletedUser)
    results = fetch_user_events(client, query_string, athenaDatabase ,outputBucket)
    
    print (results)