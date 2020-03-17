#!/usr/bin/env python3

import boto3
import psycopg2 # pip install psycopg2-binary
import sys
import os
import logging
 
def extract_redshift_schema(
    region,
    db_name,
    db_user,
    cluster_identifier,
    endpoint,
    scripts_redshift_copy_schema_dir,
    v_generate_tbl_ddl_sql_file,
    get_schema_sql,
    schema_file):
 
    try:
        # Create redshift connection
        client = boto3.client('redshift', region_name=region)
     
        # Get temporary username and password
        cluster_creds = client.get_cluster_credentials(
            DbUser=db_user, DbName=db_name, ClusterIdentifier=cluster_identifier, AutoCreate=False)

        temp_user = cluster_creds['DbUser']
        temp_password = cluster_creds['DbPassword']
     
        # Create connection string to database
        conn = psycopg2.connect(f"host='{endpoint}' port='5439' user={temp_user} password={temp_password} dbname='{db_name}'")
        cursor = conn.cursor()

        # Execute SQL file to create view, which we can then query to get the schema
        cursor.execute(v_generate_tbl_ddl_sql_file.read())

        # Query view to get the schema of all tables in the schema
        cursor.execute(get_schema_sql)
        for line in cursor:
            schema_file.write(f"{line[0]}\n")
        conn.commit()
        print("Schema has been exported successfully!")
     
        #report any errors
    except Exception as e:
        logging.error("Failed!")
        logging.error("Exception name : " + e.__class__.__name__)
        logging.error(str(e))
        sys.exit(1)
     
    #close all connections
    finally:
        cursor.close()
        conn.close()

def main():

    # Sources:
    # - https://github.com/awslabs/amazon-redshift-utils/blob/4646dacf0d25494d2b2225c66c1b50305564e8c3/src/AdminViews/v_generate_tbl_ddl.sql

    # VARS:
    region = 'eu-west-2' # change
    db_name = 'dev' # change
    db_user = 'admin' # change
    cluster_identifier = 'murty-cluster-1' # change
    endpoint = 'murty-cluster-1.xxxxxxxx.eu-west-2.redshift.amazonaws.com' # change
    scripts_redshift_copy_schema_dir = f"/scripts/redshift/extract_schema"
    v_generate_tbl_ddl_sql_file = open(f"{scripts_redshift_copy_schema_dir}/v_generate_tbl_ddl.sql", 'r')
    get_schema_sql = "select ddl from ( " + \
        "(select * from public.v_generate_tbl_ddl " + \
            "where ddl not like 'ALTER TABLE %' " + \
            "order by tablename)" + \
        "UNION ALL " + \
        "(select * from public.v_generate_tbl_ddl " + \
            "where ddl like 'ALTER TABLE %' " + \
            "order by tablename) " + \
        ") where schemaname = 'public' and tablename !~ '_';" # the last filter returns only tables with no underscores. This can be chnaged later if needed.

    schema_file = open(f"{scripts_redshift_copy_schema_dir}/schema_file.sql", 'w')

    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(asctime)s: %(message)s'
        )

    extract_redshift_schema(
        region=region,
        db_name=db_name,
        db_user=db_user,
        cluster_identifier=cluster_identifier,
        endpoint=endpoint,
        scripts_redshift_copy_schema_dir=scripts_redshift_copy_schema_dir,
        v_generate_tbl_ddl_sql_file=v_generate_tbl_ddl_sql_file,
        get_schema_sql=get_schema_sql,
        schema_file=schema_file)

if __name__ == '__main__':
    main()
    
