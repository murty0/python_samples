#!/usr/bin/env python3

from datetime import datetime
import dateutil.parser
import logging
import os
from s3_wrapper import *

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
s3_key_timestamp = 'assignments/infra/timestamp'
s3_key_timestamp_usman = 'assignments/infra/timestamp_usman'

timestamp_load_file = load_file(
		s3_client=s3_client,
		s3_bucket=s3_bucket,
		s3_key=s3_key_timestamp,
	)

if timestamp_load_file is not None:
	timestamp_stream = timestamp_load_file.read()

	try:
		timestamp_epoch_datetime = int(dateutil.parser.parse(timestamp_stream).timestamp())
		timestamp_usman_load_file = load_file(
			s3_client=s3_client,
			s3_bucket=s3_bucket,
			s3_key=s3_key_timestamp_usman,
			)

		if (timestamp_usman_load_file is not None):
			timestamp_usman_stream = timestamp_usman_load_file.read()
			try:
				timestamp_usman_epoch_datetime = int(timestamp_usman_stream)
				if (timestamp_epoch_datetime == timestamp_usman_epoch_datetime):
					logging.info(f'\'{s3_key_timestamp}\' file has not been updated since previous check 1 minute ago, no update to \'{s3_key_timestamp_usman}\' file required!')
				else:
					logging.info(f'\'{s3_key_timestamp}\' has been updated since previous check 1 minute ago, update to \'{s3_key_timestamp_usman}\' file required!')
					logging.info(f'Updating \'{s3_key_timestamp_usman}\' file with new converted epoch time from \'{s3_key_timestamp}\' file...')

					timestamp_usman_update_file = save_file(
						s3_client=s3_client,
						s3_bucket=s3_bucket,
						s3_key=s3_key_timestamp_usman,
						file_body=str(timestamp_epoch_datetime)
						)

					if timestamp_usman_update_file:
						logging.info(f'\'{s3_key_timestamp_usman}\' file in S3 \'{s3_bucket}\' bucket has been updated successfully with value \'{timestamp_epoch_datetime}\'!')
					else:
						logging.info(timestamp_usman_update_file)

			except Exception as err:
				logging.info(f'Error occurred: {err}')
				logging.info(f'\'{s3_key_timestamp_usman}\' file does not contain a valid epoch timestamp, or the file is empty!')

		else:
			logging.info(timestamp_usman_load_file)
			logging.info(f'No \'{s3_key_timestamp_usman}\' file exists!')

			timestamp_usman_filename = "timestamp_usman"
			os.system(f'touch {timestamp_usman_filename}')
			os.system(f'echo {timestamp_epoch_datetime} > {timestamp_usman_filename}')
			current_working_directory = str(os.getcwd())
			
			upload_file_path = (current_working_directory + '/' + timestamp_usman_filename)
			logging.info(f'\'{timestamp_usman_filename}\' file created in local directory \'{current_working_directory}\'!')
			logging.info(f'Uploading \'{timestamp_usman_filename}\' file to S3 \'{s3_bucket}\' bucket...')

			timestamp_usman_save_file = save_file(
				s3_client=s3_client,
				s3_bucket=s3_bucket,
				s3_key=s3_key_timestamp_usman,
				upload_file_path=upload_file_path
				)

			if timestamp_usman_save_file:
				logging.info(f'\'{s3_key_timestamp_usman}\' file uploaded to S3 \'{s3_bucket}\' bucket successfully!')
			else:
				logging.info(timestamp_usman_save_file)

	except Exception as err:
		logging.info(f'Error occurred: {err}')
		logging.info(f'\'{s3_key_timestamp}\' file does not contain a valid timestamp string, or the file is empty!')

else:
	logging.info(timestamp_load_file)
	logging.info(f'No \'{s3_key_timestamp}\' file exists, or invalid AWS credentials!')
