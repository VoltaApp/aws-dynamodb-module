import json
from typing import Union


class DictUtils():

    @staticmethod
    def transform_last_evaluated_key(
        last_evaluated_key: str
    ):
        last_key = DictUtils.load_metadata(
            metadata=last_evaluated_key.replace("\'", "\"")
        )
        return last_key

    @staticmethod
    def load_metadata(
        metadata: Union[str, bytes]
    ) -> dict:
        metadata = json.loads(metadata)
        return metadata
