#!/usr/bin/env python3

from botocore.exceptions import ClientError
from boto3 import client
import logging
import os
import datetime

def save_file(s3_client, s3_bucket, s3_key, file_body=None, upload_file_path=None):
	if (upload_file_path is not None):
		try:
			response = s3_client.upload_file(
				Bucket=s3_bucket,
				Key=s3_key,
				Filename=upload_file_path
				)
			return True

		except ClientError as e:
			logging.error(e)
			return False

	elif (upload_file_path is None) and (file_body is not None):
		try:
			response = s3_client.put_object(
				Bucket=s3_bucket,
				Key=s3_key,
				Body=file_body
				)
			return True

		except ClientError as e:
			logging.error(e)
			return False

	else:
		logging.info("No file_body or upload_file_path provided!")
		

def load_file(s3_client, s3_bucket, s3_key, save_to_path=None):
	if (save_to_path is not None):
		try:
			response = s3_client.download_file(
				Bucket=s3_bucket,
				Key=s3_key,
				Filename=save_to_path
				)
			return save_to_path

		except ClientError as e:
			logging.error(e)
			return None

	elif (save_to_path is None):
		try:
			response = s3_client.get_object(
				Bucket=s3_bucket,
				Key=s3_key
				)

			return response['Body']

		except ClientError as e:
			logging.error(e)
			return None

	else:
		return None
	
def file_exists(s3_client, s3_bucket, s3_key):
	try:
		response = s3_client.head_object(
			Bucket=s3_bucket,
			Key=s3_key
			)
		return True

	except ClientError as e:
		logging.error(e)
		return False

def main():

	s3_client = client(
		's3',
		aws_access_key_id='ACCESS_KEY_ID', # replace with ACCESS_KEY_ID
		aws_secret_access_key='SECRET_ACCESS_KEY' # replace with SECRET_ACCESS_KEY
		)

	logging.basicConfig(
		level=logging.INFO,
		filename='/var/log/cron.log',
		format='%(levelname)s: %(asctime)s: %(message)s'
		)

	s3_bucket = 'xxxx'
	s3_key_save_file = 'assignments/infra/timestamp_usman'
	s3_key_load_file = 'assignments/infra/timestamp_usman'
	s3_key_file_exists = 'assignments/infra/timestamp_usman'
	#file_body = 'Foo!'

	#Create 'timestamp_usman' file with current epoch time on the fly so it can be used for save_file
	timestamp_now_epoch = int(datetime.datetime.now().timestamp())
	timestamp_usman_filename = "timestamp_usman"
	os.system(f'touch {timestamp_usman_filename}')
	os.system(f'echo {timestamp_now_epoch} > {timestamp_usman_filename}')
	current_working_directory = str(os.getcwd())
	upload_file_path = (current_working_directory + '/' + timestamp_usman_filename)
	logging.info(f'\'{timestamp_usman_filename}\' file created in local directory \'{current_working_directory}\'!')
	save_to_path = './new_timestamp_usman'

	response_save_file = save_file(
		s3_client=s3_client,
		s3_bucket=s3_bucket,
		s3_key=s3_key_save_file,
		upload_file_path=upload_file_path # can also be None and instead can pass in file_body, or both
		)

	logging.info(f'Added {s3_key_save_file} to {s3_bucket}!')
	logging.info(f'save_file: {response_save_file}')

	response_load_file = load_file(
		s3_client=s3_client,
		s3_bucket=s3_bucket,
		s3_key=s3_key_load_file,
		save_to_path=save_to_path
		)
	
	logging.info(f'Loaded {s3_key_load_file} from {s3_bucket}!')
	logging.info(f'load_file: {response_load_file}')
	
	response_file_exists = file_exists(
		s3_client=s3_client,
		s3_bucket=s3_bucket,
		s3_key=s3_key_file_exists
		)

	logging.info(f'Checked if {s3_key_file_exists} exists in {s3_bucket}!')
	logging.info(f'file_exists: {response_file_exists}')

if __name__ == '__main__':
	main()
