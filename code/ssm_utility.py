"""SSM Utility"""
import boto3

SSM_CLIENT = boto3.client('ssm', region_name='us-east-1')


def get_ssm_value(key: str):
    """Fetch SSM parameter"""
    return SSM_CLIENT.get_parameter(Name=key, WithDecryption=True)['Parameter']['Value']
