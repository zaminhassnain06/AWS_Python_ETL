"""Ingestion Driver Module"""
import math
import datetime
import uuid
import pandas
from VeracodeLite import db_utility
from VeracodeLite import table_utility
from VeracodeLite import audit
from VeracodeLite import common_tuples
import boto3

def make_db_connection(db_name):
    """Making Connection to the DB"""
    db_connection = db_utility.get_db_connection(db_name)
    return db_connection

def make_batches(process_id, table_entry, db_connection, db_name):
    """Making Batches according to the provided info"""
    table_name = table_entry.table_name
    batch_size = table_entry.batch_size
    column_name = table_entry.column_name
    starting_value = table_entry.starting_value
    batch_id = table_entry.batch_id
    if batch_id == '':
        batch_id = str(uuid.uuid4())

    sql = 'select count(*) from ' + table_name + \
          ' where ' + column_name + ' > \'' + starting_value + '\''
    data_frame = pandas.read_sql(sql, db_connection)
    count = data_frame.values[0][0]
    batches_count = math.ceil(count / int(batch_size))

    base_sql = 'select ' + column_name + ' from ' + table_name \
               + ' where ' + column_name + ' > \'' + starting_value + '\' order by ' + column_name

    print('Making batches...')
    stage = common_tuples.stages_tuple().ingest_driver
    for i in range(batches_count):
        offset = i * int(batch_size)
        job_records_count = batch_size
        # batch_limit = str(int(batch_size) - 1)
        if i == batches_count - 1:
            batch_limit = (count - 1) - offset
            job_records_count = batch_limit
            ending_sql = base_sql + ' LIMIT ' + str(offset + int(batch_limit)) + ',1'
            end_column = pandas.read_sql(ending_sql, db_connection).values[0][0]

        if i == 0:
            starting_sql = base_sql + ' LIMIT ' + str(offset) + ',1'
            start_column = pandas.read_sql(starting_sql, db_connection).values[0][0]
            table_marker_column_start = str(start_column)

        audit.insert_initial_batches_entry(process_id=process_id,
                                           source_type='Structured',
                                           data_source='Aurora',
                                           source_table=table_name,
                                           target_table=table_name,
                                           stage=stage,
                                           status=common_tuples.status_tuple().pending,
                                           column_start='',
                                           column_end='',
                                           batch_limit=str(batch_size),
                                           batch_offset=str(offset),
                                           batch_id=batch_id,
                                           column_name=column_name,
                                           created_by=stage, updated_by=stage,
                                           starting_value=starting_value,
                                           job_records_count=str(job_records_count),
                                           rerun_params='')

    print('batches created')
    if batches_count > 0:
        end_date_time = pandas.to_datetime(str(end_column))
        end_column = end_date_time.strftime('%Y-%m-%d %H:%M:%S')
        start_date_time = pandas.to_datetime(str(table_marker_column_start))
        table_marker_column_start = start_date_time.strftime('%Y-%m-%d %H:%M:%S')
        audit.make_table_marker(process_id=process_id, source_type='Structured',
                                data_source='Aurora', source_table=table_name,
                                source_column=column_name, status='pending',
                                column_start=table_marker_column_start, column_end=str(end_column),
                                jobs_count=batches_count, data_size=batch_size,
                                db_name=db_name, batch_id=batch_id,
                                created_by='Ingest-Driver', updated_by='Ingest-Driver')
        initiate_controller(str(process_id))
    print('Finished')

def initiate_controller(process_id: str):
    """Initiate controller"""
    print('process_id: ' + process_id)
    glue_client = boto3.client('glue')
    glue_client.start_job_run(
        JobName='team-b-ingestion-controller',
        Arguments={
            '--process_id': process_id
        }
    )
    print('Controller Inititated')

def initialize_driver():
    """Fetch Configurations"""
    process_id = audit.make_process(source_type='Structured', data_source='Aurora',
                                    status=common_tuples.status_tuple().pending,
                                    start_time=datetime.datetime.now(),
                                    created_by=common_tuples.stages_tuple().ingest_driver,
                                    updated_by=common_tuples.stages_tuple().ingest_driver)

    db_name, table_list = table_utility.get_table_list()
    db_connection = make_db_connection(db_name)
    for table_entry in table_list:
        make_batches(process_id, table_entry, db_connection, db_name)


initialize_driver()
