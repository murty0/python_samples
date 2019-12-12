import json
import base64
import logging
import requests
from boto3 import client

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()

okta_admin_user_id = '00u57xng5yarEeUiq2p7'
super_admin_group_id = '00gsldic3ljlHuHD82p6'

api_token_secret = client('ssm').get_parameter(Name='okta_api_token', WithDecryption=True)['Parameter']['Value']
api_token = base64.b64decode(api_token_secret).decode('utf-8')

def suspend_okta_admin(event, context):
    logger.info('## EVENT')
    logger.info(event)

    remove_service_account_from_super_admins_group(
        okta_admin_user_id=okta_admin_user_id,
        super_admin_group_id=super_admin_group_id)
    
    suspend_service_acounnt(
        okta_admin_user_id=okta_admin_user_id)

def remove_service_account_from_super_admins_group(
    okta_admin_user_id=okta_admin_user_id,
    super_admin_group_id=super_admin_group_id):

    remove_from_group_url = 'https://xxxx.okta.com/api/v1/groups/{super_admin_group_id}/users/okta_admin_user_id}'.format(
        super_admin_group_id = super_admin_group_id,
        okta_admin_user_id = okta_admin_user_id
    )

    remove_from_group_headers = {	
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'SSWS {api_token}'.format(api_token = api_token)
    }

    try:
        remove_from_group_request = requests.delete(
            remove_from_group_url, 
            headers = remove_from_group_headers
        )

        logger.info(remove_from_group_request.text)
        remove_from_group_request.raise_for_status()
        
        logger.info('Service account has been removed from Super Admins group successfully!')

    except Exception as err:
        logger.info(f'Error occurred: {err}')
        logger.info(json.loads(remove_from_group_request.text)['errorCauses'])

def suspend_service_acounnt(
    okta_admin_user_id=okta_admin_user_id):

    suspend_account_url = 'https://xxxx.okta.com/api/v1/users/{okta_admin_user_id}/lifecycle/suspend'.format(
        okta_admin_user_id = okta_admin_user_id
    )

    suspend_account_headers = {	
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'SSWS {api_token}'.format(api_token = api_token)
    }

    try:
        suspend_account_request = requests.post(
            suspend_account_url, 
            headers = suspend_account_headers
        )

        logger.info(suspend_account_request.text)
        suspend_account_request.raise_for_status()
        
        logger.info('Service account has been suspended successfully!')

    except Exception as err:
        logger.info(f'Error occurred: {err}')
        logger.info(json.loads(suspend_account_request.text)['errorCauses'])
