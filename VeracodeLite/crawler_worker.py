"""Crawler Worker"""
import time
from VeracodeLite import crawler_utility as cu
from VeracodeLite import audit
from VeracodeLite import ssm_utility
from VeracodeLite import common_tuples


DB_WIKI = ssm_utility.get_ssm_value('/teamb/dev/db/name')
STAGE = common_tuples.stages_tuple().parquet_crawler


def execute_crawler_worker(batch_id):
    """this method will get batch id from table marker"""
    try:
        print('Crawler Worker Started')
        s3_path = cu.create_s3_target(batch_id, common_tuples.status_tuple().pending,
                                      common_tuples.stages_tuple().parquet_worker)
        crawler_name = s3_path[0]['Source_Table']
        print(s3_path)
        execute_crawler(s3_path[0]['Path'], DB_WIKI, crawler_name)
        audit.update_file_status_entry_by_batch_id(batch_id,
                                                   common_tuples.status_tuple().completed, STAGE,
                                                   common_tuples.stages_tuple().parquet_worker)
        check_crawlers_status(crawler_name)
    except Exception as error:
        print('Crawler-Worker Status Failed...')
        print(error)
        audit.update_file_status_entry_by_batch_id(batch_id,
                                                   common_tuples.status_tuple().failed, STAGE,
                                                   common_tuples.stages_tuple().parquet_worker)


def execute_crawler(s3_path, db_name, crawler_name):
    """this will execute crawler"""
    print('executing crawler')

    cu.create_crawler(crawler_name, db_name, s3_path)
    cu.start_crawler(crawler_name)


def check_crawlers_status(crawler):
    """Checks crawlers status and deletes crawled crawler"""
    crawling = True
    while crawling:
        state = cu.get_crawler_state(crawler)

        print('waiting crawler execution for 20 seconds')
        time.sleep(20)
        if state == 'READY':
            cu.delete_crawler(crawler)
            print(crawler, 'crawler deleted.')
            break
execute_crawler_worker('4971d831-07a6-4dd4-8472-18f36ac34e54')