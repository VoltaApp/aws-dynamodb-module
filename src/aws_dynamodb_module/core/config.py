import os

from botocore.config import Config


class Settings(object):
    debug: bool = True
    max_pool_connections = 15
    retries = {
        "max_attempts": 10,
        "mode": "standard"
    }
    region = "ap-southeast-2"
    boto3_dynamodb_config = Config(
        region_name=region,
        max_pool_connections=max_pool_connections,
        retries=retries,
    )

class DevSettings(Settings):
    env = "dev"
    dynamodb_tbl = "dev.volta"


class StagingSettings(Settings):
    env = "staging"
    dynamodb_tbl = "staging.volta"


class ProdSettings(Settings):
    env = "prod"
    debug: bool = False
    dynamodb_tbl = "prod.volta"


def get_settings():
    env = os.getenv("env", None)
    if env == "staging":
        return StagingSettings()
    elif env == "prod":
        return ProdSettings()
    else:
        return DevSettings()


config = get_settings()
