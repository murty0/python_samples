import json
import math
import base64
import logging
import requests
from boto3 import client

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()

#api_token_secret = client('ssm').get_parameter(Name='okta_api_token', WithDecryption=True)['Parameter']['Value']
#api_token = base64.b64decode(api_token_secret).decode('utf-8')

def namely_scraper():

    number_of_profiles_pages = get_number_of_pages()
    
    all_profiles = get_all_namely_profiles(number_of_profiles_pages)

    flattened_list_of_dicts = convert_and_flatten_list_of_lists_to_list_of_dicts(all_profiles)

    all_required_profile_fields = extract_all_required_profile_fields_and_convert_to_json(flattened_list_of_dicts)   

def get_number_of_pages():
    get_number_of_pages_url = 'https://xxxx.namely.com/api/v1/profiles.json'

    get_number_of_pages_headers = { 
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer XXXX'
    }

    try:
        get_number_of_pages_request = requests.get(
            get_number_of_pages_url, 
            headers = get_number_of_pages_headers
        )

        total_number_of_profiles = int(json.loads(get_number_of_pages_request.text)['meta']['total_count'])
        max_results_per_page = 50 # as per Namely API docs
        total_number_of_pages = math.ceil(total_number_of_profiles/50)
        get_number_of_pages_request.raise_for_status()
        print('Number of pages retrieved from Namely successfully!')
        return total_number_of_pages

    except Exception as err:
        print(f'Error occurred: {err}')
        print(json.loads(get_number_of_pages_request.text)['errorCauses'])

def get_all_namely_profiles(number_of_profiles_pages):
    all_profiles_list = []
    for page in range(1, number_of_profiles_pages + 1):
        get_all_namely_profiles_url = 'https://xxxx.namely.com/api/v1/profiles.json?page={page}&per_page=50'.format(
            page = page)

        get_all_namely_profiles_headers = {	
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer XXXX'
        }

        try:
            get_all_namely_profiles_request = requests.get(
                get_all_namely_profiles_url, 
                headers = get_all_namely_profiles_headers
            )

            profiles_blob = json.loads(get_all_namely_profiles_request.text)['profiles']
            all_profiles_list.append(profiles_blob)
            print('Profiles retrieved from page {page}'.format(page = page))
            get_all_namely_profiles_request.raise_for_status()
            print('All profiles retrieved from Namely successfully!')

        except Exception as err:
            print(f'Error occurred: {err}')
            print(json.loads(get_all_namely_profiles_request.text)['errorCauses'])

    return all_profiles_list

def convert_and_flatten_list_of_lists_to_list_of_dicts(all_profiles):
    profiles_list_of_dicts = []
    for profiles in all_profiles:
        for profile in profiles:
            profiles_list_of_dicts.append(profile)

    return profiles_list_of_dicts
              
def extract_all_required_profile_fields_and_convert_to_json(list_of_dicts):
    required_fields_list = ['id', 'email', 'user_status', 'reports_to']
    profiles_list_with_only_needed_keys_values = []
    for profile_blob in list_of_dicts:
        profiles_list_with_only_needed_keys_values.append((dict((k, profile_blob[k]) for k in required_fields_list)))

    json_with_all_fields = json.loads(json.dumps(profiles_list_with_only_needed_keys_values))
    for profile in json_with_all_fields:
        print(profile)
    
    return json_with_all_fields

namely_scraper()
