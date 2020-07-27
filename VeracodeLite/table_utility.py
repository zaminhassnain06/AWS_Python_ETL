''' json parser '''
import json
import boto3
from VeracodeLite import ssm_utility
from VeracodeLite import table_Item
from VeracodeLite import audit

LOCAL_CONFIG_FILE = 'driver_config.json'
BUCKET_NAME = ssm_utility.get_ssm_value('/teamb/s3/bucketname')
INGESTION_CONFIG_NAME = ssm_utility.get_ssm_value('/teamb/config/ingestion-driver')


def download_object(bucket_name, source_filename, local_filename):
    ''' download object '''
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, source_filename, local_filename)

def get_table_list():
    ''' parsing json to fetch data '''
    if audit.is_table_empty('table_marker'):
        try:
            download_object(BUCKET_NAME, INGESTION_CONFIG_NAME, LOCAL_CONFIG_FILE)
            data = open(LOCAL_CONFIG_FILE, )
            info = json.loads(data.read())
            DATABASE_NAME = info['databases'][0]
            tables_list = []
            for item in info[DATABASE_NAME]:
                table_entry = table_Item.TableItem(item, None)
                tables_list.append(table_entry)
            return DATABASE_NAME, tables_list
        except:
            print('unable to download config file from s3')
            return []
    else:
        db_name = ''
        tables_list = []
        for row in audit.fetch_table_marker():
            db_name = row['db_name']
            table_entry = table_Item.TableItem(None, row)
            tables_list.append(table_entry)
        return db_name, tables_list

        