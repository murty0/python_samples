from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import base64
import logging
from boto3 import client

def suspend_admin(event, context):
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger()
    logger.info('## EVENT')
    logger.info(event)

    service = get_service()
    logger.info(vars(service))

    service_account = 'xxxx'

    request_body = {'suspended': True}

    try:
        update_user = service.users().update(userKey=service_account, body=request_body).execute()
        json_response = json.loads(json.dumps(update_user))
        logger.info(json_response)
        return json_response
    except Exception as error:
        print(error)
        return error

def get_service():

    admin_directory_user_scope = 'https://www.googleapis.com/auth/admin.directory.user'
    delegated_admin = 'xxxx'

    auth_token = client('ssm').get_parameter(Name='g_suite_api_cert', WithDecryption=True)

    auth_token_value = auth_token['Parameter']['Value']

    decoded_cert = json.loads(base64.b64decode(auth_token_value.encode('utf-8')))

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            decoded_cert,
            scopes=[admin_directory_user_scope])
    creds = credentials.create_delegated(delegated_admin)

    service = build('admin', 'directory_v1', credentials=creds, cache_discovery=False)
    
    return service
