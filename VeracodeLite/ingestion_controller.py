''' ingestion controller '''
import time
import sys
from VeracodeLite import db_utility
import pandas
import boto3
from awsglue.utils import getResolvedOptions

ARGS = getResolvedOptions(sys.argv,
                          ['process_id'])
PROCESS_ID = ARGS['process_id']
glue = boto3.client('glue')
flag = True
buffer_size = 5
running_processes = []
batches = []


def get_job_status(job_name, job_run_id):
    """returns jobs status"""
    job_status = glue.get_job_run(JobName=job_name, RunId=job_run_id)
    return job_status['JobRun']['JobRunState']


def check_jobs_status():
    ''' checking statuses of jobs that they are done or not  '''
    print('checking job statuses')
    remove = 0
    for job in running_processes:
        status_value = get_job_status('team-b-ingestion-worker-job', job['JobRunId'])
        if status_value == 'SUCCEEDED':
            print('Job SUCCEEDED ')
            running_processes.remove(job)
            remove = remove + 1
        else:
            print('still running')
    return remove


def consumer():
    ''' wait and then check status '''
    while len(batches) > 0 or len(running_processes) > 0:
        print('buffer start ', running_processes, ' buffer end')
        time.sleep(10)
        count = check_jobs_status()
        if count > 0:
            add_new_jobs(count)


def start_glue_job(row):
    ''' starts glue job '''
    job_run = glue.start_job_run(JobName='team-b-ingestion-worker-job',
                                 Arguments={
                                     '--bucket_name': 'veracode-lite-team-b',
                                     '--id': str(row[1]['id']),
                                     '--table_name': str(row[1]['source_table']),
                                     '--column_name': str(row[1]['column_name']),
                                     '--starting_value': str(row[1]['starting_value']),
                                     '--batch_offset': str(row[1]['batch_offset']),
                                     '--batch_id': str(row[1]['batch_id']),
                                     '--batch_limit': str(row[1]['batch_limit']),
                                     '--source_type': str(row[1]['source_type']),
                                     '--data_source': str(row[1]['data_source']),
                                     '--process_id': str(row[1]['process_id'])
                                 }
                                 )
    return job_run


def add_new_jobs(counter):
    ''' added counter new jobs in running_processes '''
    count = 0
    while count < counter and len(batches) > 0:
        flag = False
        while flag == False:
            try:
                glue_job_id = start_glue_job(batches[0])
                flag = True
            except:
                print('waiting for removal of succeeded job')
        running_processes.append(glue_job_id)
        batches.pop(0)
        count = count + 1


def scatter_and_gather_batches(process_id):
    ''' initiating jobs. '''
    db_connection = db_utility.get_db_connection(db_utility.AUDIT_DB_NAME)
    sql = 'select id, source_table, batch_limit, batch_id, source_type, data_source, ' \
          'process_id,batch_offset,starting_value,column_name from process_statistics ' \
          'where process_id =' + process_id
    list = pandas.read_sql(sql, db_connection)

    for row in list.iterrows():
        batches.append(row)

    cur_audit = db_connection.cursor()
    update_process_status = " UPDATE process_status " \
                            " SET status = 'Pending'" \
                            "WHERE id = " + str(process_id)

    cur_audit.execute(update_process_status)
    db_connection.commit()

    add_new_jobs(buffer_size)
    consumer()

    update_process_status = " UPDATE process_status " \
                            " SET status = 'Completed'" \
                            "WHERE id = " + str(process_id)

    cur_audit.execute(update_process_status)
    db_connection.commit()


scatter_and_gather_batches(PROCESS_ID)
