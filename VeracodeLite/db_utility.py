"""Database Utilities"""
import pymysql
from VeracodeLite import ssm_utility
from VeracodeLite import constants

DB_HOST = ssm_utility.get_ssm_value(constants.BASE_SSM_DB_PATH + '/host')
DB_PORT = int(ssm_utility.get_ssm_value(constants.BASE_SSM_DB_PATH + '/port'))
DB_USER = ssm_utility.get_ssm_value(constants.BASE_SSM_DB_PATH + '/user')
DB_PASS = ssm_utility.get_ssm_value(constants.BASE_SSM_DB_PATH + '/pass')
AUDIT_DB_NAME = ssm_utility.get_ssm_value(constants.BASE_SSM_DB_PATH + '/audit')


def get_db_connection(db_name):
    """DB Connection With Provided Name"""
    return pymysql.connect(DB_HOST, user=DB_USER, port=DB_PORT,
                           passwd=DB_PASS, db=db_name)
