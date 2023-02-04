import functools
from typing import List


class FunctionBuilder:

    def __init__(self, func):
        self.func = func

    def having(self, *args, **kwargs):
        self.func = functools.partial(self.func, *args, **kwargs)
        return self

    def get(self):
        return self.func


# ./dynamo_iterator.py
class DynamoIterator:
    def __init__(self, func):
        """Given dynamodb function that returns items and a LastEvaluatedKey
        iterate
        Keyword arguments:
        func -- function that takes LastEvaluatedKey and returns { items:  [] }
        """
        self.func = func
        self.last_evaluated_key = {}
        self.first_fetch = True

    def get_first_item(self) -> dict:
        items = self.get_items_as_list()
        if items:
            return items[0]
        return {}

    def get_items_as_list(self) -> List[dict]:
        items = list(self)
        return items

    def _get_items(self, db_result) -> List[dict]:
        return db_result.get('Items', [])

    def _has_more_results(self) -> bool:
        return self.last_evaluated_key and len(self.data) == 0

    def __iter__(self):
        """Required to implement the Iterator interface in python"""
        return self

    def __next__(self):
        """Required to implement the Iterator interface in python"""
        if self.first_fetch or self._has_more_results():
            self.first_fetch = False
            if self.last_evaluated_key:
                result = self.func(ExclusiveStartKey=self.last_evaluated_key)
            else:
                result = self.func()
            self.last_evaluated_key = result.get('LastEvaluatedKey', None)
            self.data = self._get_items(result)

        if len(self.data) <= 0:
            raise StopIteration

        return self.data.pop()

    """Python 2.7 requireds next vs 3.7 that requires __next__
    hence we alias the function"""
    next = __next__
