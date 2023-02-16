from botocore.config import Config


DEFAULT_DYNAMODB_CONFIG = {
    "service_name": "dynamodb",
    "config": Config(
        region_name="ap-southeast-2",
        max_pool_connections=15,
        retries={
            "max_attempts": 10,
            "mode": "standard"
        },
    )
}
