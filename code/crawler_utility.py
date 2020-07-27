"""crawler utility contains all crawler helper methods"""
import boto3
from VeracodeLite import constants
from VeracodeLite import audit
from VeracodeLite import ssm_utility

GLUE_CLIENT = boto3.client('glue', region_name='us-east-1')


def create_s3_target(batch_id, status, stage):
    """returns s3 path and source table to crawl"""
    tables_list = []
    for row in audit.fetch_file_status_entries(batch_id, status, stage):
        path = {'Path': row['file_path'], 'Source_Table': row['source_table']}
        print(path)
        tables_list.append(path)
    return tables_list


def create_crawler(crawler_name, db_name, s3_paths):
    """Create Crawler:
    s3_path: It can be fetched from create_s3_target function
    """
    print('creating crawler', crawler_name)
    response = GLUE_CLIENT.create_crawler(
        Name=crawler_name,
        Role=ssm_utility.get_ssm_value(constants.ROLE_SSM),
        DatabaseName=db_name,
        Description='Crawler for generated' + db_name + 'schema',
        Targets={
            'S3Targets': [{
                'Path': s3_paths}]},
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        }
    )
    print(crawler_name, ' crawler created successfully')

    return response


def start_crawler(crawler_name):
    """Start crawler of table"""
    print('starting crawler', crawler_name)
    response = GLUE_CLIENT.start_crawler(
        Name=crawler_name
    )
    print(crawler_name, ' crawler started successfully')
    return response


def delete_crawler(crawler_name):
    """Delete crawler"""
    print('Deleting Crawler:', crawler_name)
    response = GLUE_CLIENT.delete_crawler(
        Name=crawler_name
    )
    print(crawler_name, ' crawler deleted successfully')
    return response


def get_crawler_state(crawler_name):
    """Get crawler state"""
    response = GLUE_CLIENT.get_crawler(
        Name=crawler_name
    )
    state = response['Crawler']['State']
    print(crawler_name, 'crawler state is', state)
    return state
