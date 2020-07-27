from VeracodeLite import audit
import json
import boto3
import time

glue = boto3.client('glue')
stage = 'Ingestion-Worker'
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
        status_value = get_job_status('team-b-parquet-worker', job['JobRunId'])
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

def start_csv_parquet_job(batch_id):
    rows = audit.fetch_file_status_entries(batch_id, 'pending', stage)
    data = json.dumps(rows)
    job_run = glue.start_job_run(JobName='team-b-parquet-worker',
                                 Arguments={
                                     '--list': data
                                  }
                                 )
    print(data)
    return job_run

def add_new_jobs(counter):
    ''' added counter new jobs in running_processes '''
    count = 0
    while count < counter and len(batches) > 0:
        flag = False
        while flag == False:
            try:
                glue_job_id = start_csv_parquet_job(batches[0]['batch_id'])
                flag = True
            except:
                print('waiting for removal of succeeded job')
        running_processes.append(glue_job_id)
        batches.pop(0)
        count = count + 1

batches = audit.fetch_batch_ids()
add_new_jobs(buffer_size)
consumer()
