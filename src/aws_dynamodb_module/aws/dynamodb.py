import boto3

from aws_dynamodb_module.core.config import config as _config


_dynamodb_client = boto3.client(
    'dynamodb',
    config=_config.boto3_dynamodb_config,
)

_dynamodb_resource = boto3.resource(
    'dynamodb',
    config=_config.boto3_dynamodb_config,

).Table(_config.dynamodb_tbl)
