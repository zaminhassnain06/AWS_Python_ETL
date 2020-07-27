"""ingestion worker job """
import re
import boto3
from datetime import datetime
from VeracodeLite import ssm_utility
from VeracodeLite import db_utility
from VeracodeLite import audit
from VeracodeLite import common_tuples
from VeracodeLite import common
import sys
from awsglue.utils import getResolvedOptions

ARGS = getResolvedOptions(sys.argv,
                          ['starting_value', 'table_name', 'batch_offset', 'batch_limit',
                           'bucket_name', 'column_name', 'id', 'source_type', 'data_source', 'process_id',
                           'batch_id'])


BUCKET_NAME = ssm_utility.get_ssm_value('/teamb/dev/s3/bucketname')
BUCKET_FOLDER = "out_files"
DB_WIKI = ssm_utility.get_ssm_value('/teamb/dev/db/name')
S3_STRUCTURE_PATTERN = re.compile(r'(^\d{4})-(\d{2})-(\d{2})\s*(\S+)', re.IGNORECASE)
STAGE = common_tuples.stages_tuple().ingest_worker
JOB_TYPE = 'Worker'
UPDATED_BY = common_tuples.stages_tuple().ingest_worker
BATCH_ID = ARGS['batch_id']
PROCESS_STATISTICS_ID = ARGS['id']
SOURCE_TYPE = ARGS['source_type']
DATA_SOURCE = ARGS['data_source']
PROCESS_ID = ARGS['process_id']
TABLE_NAME = ARGS['table_name']


def parse_s3_folder_structure(column_value, job_run_id):
    """Returns S3 folder hierarchy yyyy-mm-dd hh:mm:ss"""
    print('Parsing S3 Folder Structure...')
    match = re.search(S3_STRUCTURE_PATTERN, column_value)
    if match is not None:
        return dict(path= 's3://' +str(BUCKET_NAME)+ '/' + str(SOURCE_TYPE) + '/' + str(DB_WIKI) + '/' + str(TABLE_NAME) +
                    '/' + str(match.group(1)) + '/' + str(match.group(2)) + '/' + str(match.group(3)) +
                    '/' + str(BATCH_ID) + '/' + str(job_run_id) + '/' + str(TABLE_NAME),
                    fileName=str(match.group(4))
                    )
    return None

def get_glue_job_run_id():
    """Returns job id"""
    print('Get Glue Job Run Id...')
    glue = boto3.client('glue')
    job_run_status = glue.get_job_runs(JobName='team-b-ingestion-worker-job', MaxResults=10)
    i = 0
    while i < 10:
        if(
                job_run_status['JobRuns'][i]['Arguments']['--batch_id'] == BATCH_ID and
                job_run_status['JobRuns'][i]['Arguments']['--id'] == PROCESS_STATISTICS_ID and
                job_run_status['JobRuns'][i]['Arguments']['--process_id'] == PROCESS_ID
        ):
            if job_run_status['JobRuns'][i]['Id'] is not None:
                print('Glue Job Run Id found...')
            return job_run_status['JobRuns'][i]['Id']
            break
        i = i+1
    if job_run_status['JobRuns'][i]['Id'] is None:
        print('Glue Job Run Id  not found...')
    return job_run_status['JobRuns'][0]['Id']


def execute_outfile():
    """Executes outfile query"""
    try:
        print('Ingestion Worker Started...')
        db_connection_wiki = db_utility.get_db_connection(DB_WIKI)
        cur_wiki = db_connection_wiki.cursor()

        start_time = common.current_date_time()
        column_name = ARGS['column_name']
        starting_value = ARGS['starting_value']
        offset = ARGS['batch_offset']
        limit = ARGS['batch_limit']
        job_run_id = get_glue_job_run_id()
        re_run_params = {"BatchID": BATCH_ID, "TableName": TABLE_NAME,
                         "ColumnName": column_name, "ColumnStart": starting_value,
                         "OverrideInfo": "overwrite"}

        audit.audit_update_by_ingest_worker(STAGE, common_tuples.status_tuple().pending,
                                            JOB_TYPE, UPDATED_BY, PROCESS_STATISTICS_ID, re_run_params, '')
        print('Ingestion-Worker Status is Pending...')

        bucket_path = parse_s3_folder_structure(starting_value, job_run_id)

        outfile_sql = 'select *  from ' + str(TABLE_NAME) \
                      + ' where ' + str(column_name) + ' > \'' + str(starting_value) \
                      + '\' order by ' + str(column_name) \
                      + ' LIMIT ' + str(offset) + ',' + str(limit) + \
                      " INTO OUTFILE S3 " + \
                      "'" + str(bucket_path['path']) + "'" + \
                      " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' " \
                      " LINES TERMINATED BY '\\n' "

        cur_wiki.execute(outfile_sql)
        print('Ingestion-Worker OUTFILE query is executed successfully...')
        print('File created at', str(BUCKET_NAME)+"/" + str(bucket_path['path']) + "/out_files_"
              + str(bucket_path['fileName']))

        audit.audit_update_by_ingest_worker(STAGE, common_tuples.status_tuple().completed, JOB_TYPE, UPDATED_BY,
                                            PROCESS_STATISTICS_ID, re_run_params, '')

        end_time = common.current_date_time()
        print('start time is', start_time)
        print('end time is', end_time)
        file_path = bucket_path['path']
        audit.audit_insert_file_status_ingest_workers(SOURCE_TYPE, DATA_SOURCE, PROCESS_ID, file_path, TABLE_NAME, STAGE
                                                      , BATCH_ID, job_run_id, common_tuples.status_tuple().pending,
                                                      start_time, end_time, JOB_TYPE,
                                                      JOB_TYPE)

        print('Ingestion-Worker Status is Completed...')
    except Exception as error:
        print('Ingestion-Worker Status Failed...')
        print(error)
        re_run_params = {"BatchID": BATCH_ID, "TableName": TABLE_NAME,
                         "ColumnName": column_name, "ColumnStart": starting_value,
                         "OverrideInfo": "overwrite"}
        audit.audit_update_by_ingest_worker(STAGE, common_tuples.status_tuple().failed, JOB_TYPE, UPDATED_BY,
                                            PROCESS_STATISTICS_ID, re_run_params, error)

        raise
    finally:
        db_connection_wiki.close()


execute_outfile()
