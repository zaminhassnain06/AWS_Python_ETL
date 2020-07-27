"""Audit Module"""
import json
import pymysql
from VeracodeLite import db_utility


CONNECTION = db_utility.get_db_connection(db_utility.AUDIT_DB_NAME)


def fetch_batch_ids():
    """Fetch Batch Ids """
    sql = 'SELECT batch_id from table_marker'
    cursor = CONNECTION.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql)
    CONNECTION.commit()
    return cursor.fetchall()


def make_process(source_type, data_source, status, start_time, created_by, updated_by):
    """Initialize New Process"""
    sql = '''
    INSERT INTO process_status 
    (source_type, data_source, status, start_time, created_by, updated_by) 
    VALUES (%s, %s, %s, %s, %s, %s)
    '''
    cursor = CONNECTION.cursor()
    cursor.execute(sql, (source_type, data_source, status, start_time, created_by, updated_by))
    CONNECTION.commit()
    return cursor.lastrowid


def make_table_marker(process_id, source_type, data_source, source_table,
                      source_column, status, column_start, column_end, jobs_count,
                      data_size, db_name, batch_id,
                      created_by, updated_by):
    """Make Table Marker of table"""
    sql = '''
    INSERT INTO table_marker
    (process_id, source_type, data_source, source_table, source_column, status,
    column_start, column_end, jobs_count, data_size, created_by, updated_by, 
    db_name, batch_id) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
    column_start=%s, column_end=%s, data_size=%s
    '''

    cursor = CONNECTION.cursor()
    cursor.execute(sql, (process_id, source_type, data_source, source_table, source_column,
                         status, column_start, column_end, jobs_count, data_size,
                         created_by, updated_by, db_name, batch_id,
                         column_start, column_end, data_size))
    CONNECTION.commit()


def insert_initial_batches_entry(process_id, source_type, data_source, source_table,
                                 target_table, stage, status, column_start, column_end,
                                 batch_limit, batch_offset, batch_id, column_name,
                                 created_by, updated_by,
                                 starting_value, job_records_count, rerun_params):
    """Inserting Batches In DB"""
    sql = '''
    INSERT INTO process_statistics 
    (process_id, source_type, data_source, source_table, target_table, stage, status,
    column_start, column_end, batch_limit, batch_offset, batch_id, column_name, 
    created_by, updated_by, starting_value, job_records_count, rerun_params) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    cursor = CONNECTION.cursor()
    cursor.execute(sql, (process_id, source_type, data_source, source_table, target_table, stage,
                         status, column_start, column_end, batch_limit, batch_offset, batch_id,
                         column_name, created_by, updated_by, starting_value, job_records_count,
                         rerun_params))
    CONNECTION.commit()


def audit_update_by_ingest_worker(stage, status, job_type,
                                  updated_by, process_stat_id, rerun_params,
                                  error):
    """To update status of process_statistics by ingestion worker"""
    update_process_stat_pending = " UPDATE process_statistics " \
                                  + " SET stage = '" + str(stage) + "'" \
                                  + " , status = '" + str(status) + "'" \
                                  + " , job_type = '" + str(job_type) + "'" \
                                  + " , updated_by = '" + str(updated_by) + "'" \
                                  + " , rerun_params = \'" + json.dumps(rerun_params) + "\' " \
                                  + " , error= ' " + str(error) + "'" \
                                  + "  WHERE id = " + str(process_stat_id)
    cursor = CONNECTION.cursor()
    cursor.execute(update_process_stat_pending)
    CONNECTION.commit()


def fetch_table_marker():
    """Fetch All Entries From Table Marker"""
    sql = "SELECT * FROM table_marker"
    cursor = CONNECTION.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql)
    CONNECTION.commit()
    return cursor.fetchall()


def is_table_empty(table_name):
    """Return true or false"""
    try:
        sql = "SELECT EXISTS(SELECT * FROM " + table_name + ")"
        cursor = CONNECTION.cursor()
        cursor.execute(sql)
        if cursor.fetchone()[0] == 0:
            return True
        else:
            return False
    except:
        return False


def audit_insert_file_status_ingest_workers(source_type, data_source, process_id, file_path,
                                            source_table, stage, batch_id, job_id, status,
                                            start_time, end_time,
                                            created_by, updated_by):
    """To insert file status  table by ingestion worker"""
    sql = '''
    INSERT INTO file_status 
    ( source_type, data_source, process_id, file_path, source_table, stage, batch_id, job_id, status, start_time,
    end_time, created_by, updated_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s)
    '''

    cursor = CONNECTION.cursor()
    cursor.execute(sql, (source_type, data_source, process_id, file_path,
                         source_table, stage, batch_id, job_id, status,
                         start_time, end_time, created_by, updated_by))
    CONNECTION.commit()


def fetch_file_status_entries(batch_id, status, stage):
    """Get pending entries from file status"""
    sql = 'SELECT id, source_type, data_source, process_id, file_path, source_table, batch_id,' \
          'job_id, status FROM file_status where batch_id = \'' + batch_id + '\''\
          + " AND status = \'" + status + '\''\
          + " AND stage = \'" + stage + '\''
    print(sql)
    cursor = CONNECTION.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql)
    CONNECTION.commit()
    return cursor.fetchall()

def fetch_table_marker_batches():
    """Fetch batch_id Entries From Table Marker"""
    sql = "SELECT batch_id FROM table_marker"
    cursor = CONNECTION.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql)
    CONNECTION.commit()
    return cursor.fetchall()


def update_file_status(stage, batch_id, status, updated_by):
    """To update file status  table by ingestion worker"""
    sql = '''
          UPDATE  file_status 
          SET 
          stage = %s,
          status = %s,
          updated_by = %s
          where batch_id = %s '''

    cursor = CONNECTION.cursor()
    cursor.execute(sql, (stage, status, updated_by, batch_id))
    CONNECTION.commit()


def update_file_status_entry(file_status_id, status, stage):
    """To update file status table by parquet worker"""
    sql = '''
          UPDATE  file_status 
          SET 
          status = %s,
          updated_by = %s
          where id = %s'''

    cursor = CONNECTION.cursor()
    cursor.execute(sql, (status, stage, file_status_id))
    CONNECTION.commit()


def update_file_status_entry_by_batch_id(batch_id, status, stage, stage_condition):
    """To update file status table by parquet worker"""
    sql = '''
          UPDATE  file_status 
          SET 
          status = %s,
          stage = %s,
          updated_by = %s
          where batch_id = %s AND stage = %s '''

    cursor = CONNECTION.cursor()
    cursor.execute(sql, (status, stage, stage, batch_id, stage_condition))
    CONNECTION.commit()
