'''Parquet Worker - Convert CSV to Parquet and make entries in DB'''
import json
import sys
from datetime import datetime
from VeracodeLite import audit
from VeracodeLite import ssm_utility
from VeracodeLite import common_tuples
from VeracodeLite import common
from VeracodeLite import crawler_worker
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

ARGS = getResolvedOptions(sys.argv, ['JOB_NAME', 'list'])
JSON_OF_ROWS = ARGS['list']
JOB_ID = ARGS['JOB_RUN_ID']
BUCKET_NAME = ssm_utility.get_ssm_value('/teamb/dev/s3/bucketname')
DB_WIKI = ssm_utility.get_ssm_value('/teamb/dev/db/name')

SPARK_SESSION = SparkSession \
    .builder \
    .getOrCreate()


def initiate_worker():
    '''Fetch All rows and create dataframe'''
    final_dataframe = ''
    list_of_rows = json.loads(JSON_OF_ROWS)
    process_id = ''
    source_type = ''
    data_source = ''
    source_table = ''
    batch_id = ''
    stage = common_tuples.stages_tuple().parquet_worker
    for row in list_of_rows:
        print(object)
        process_id = row['process_id']
        source_type = row['source_type']
        data_source = row['data_source']
        source_table = row['source_table']
        batch_id = row['batch_id']
        destination_path = create_file_path(source_type=row['source_type'],
                                            table_name=row['source_table'])
        if final_dataframe == '':
            final_dataframe = SPARK_SESSION.read.csv(row['file_path'] + '*')
        else:
            dataframe = SPARK_SESSION.read.csv(row['file_path'] + '*')
            final_dataframe = final_dataframe.union(dataframe)

        perform_db_operations(row)


    write_df_to_parquet(final_dataframe, destination_path)
    audit.insert_initial_batches_entry(process_id=process_id,
                                       source_type=source_type,
                                       data_source=data_source,
                                       source_table=source_table,
                                       target_table=source_table,
                                       stage=stage,
                                       status=common_tuples.status_tuple().pending,
                                       column_start='',
                                       column_end='',
                                       batch_limit='',
                                       batch_offset='',
                                       batch_id=batch_id,
                                       column_name='',
                                       created_by=stage,
                                       updated_by=stage,
                                       starting_value='',
                                       job_records_count=str(final_dataframe.count()),
                                       rerun_params=JSON_OF_ROWS)
    crawler_worker.execute_crawler_worker(batch_id)


def write_df_to_parquet(dataframe, destination_path):
    '''Write df to parquet'''
    dataframe.write.parquet(destination_path, mode="append")

def perform_db_operations(row):
    '''
    Update file_status status
    Make new entry in file_status
    '''
    file_path = create_file_path(source_type=row['source_type'], table_name=row['source_table'])
    stage = common_tuples.stages_tuple().parquet_worker
    audit.update_file_status_entry(file_status_id=row['id'],
                                   status=common_tuples.status_tuple().completed,
                                   stage=stage)
    audit.audit_insert_file_status_ingest_workers(source_type=row['source_type'],
                                                  data_source=row['data_source'],
                                                  process_id=row['process_id'],
                                                  file_path=file_path,
                                                  source_table=row['source_table'],
                                                  stage=stage,
                                                  batch_id=row['batch_id'],
                                                  job_id=JOB_ID,
                                                  status=common_tuples.status_tuple().pending,
                                                  start_time=common.current_date_time(),
                                                  end_time=common.current_date_time(),
                                                  created_by=stage,
                                                  updated_by=stage)

def create_file_path(source_type: str, table_name: str):
    return 's3://' + BUCKET_NAME + '/' + source_type + '/' + DB_WIKI\
           + '/' + table_name + '/' + table_name


initiate_worker()
