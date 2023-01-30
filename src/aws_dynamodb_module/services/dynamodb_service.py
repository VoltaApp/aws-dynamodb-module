from functools import reduce
from typing import List, Union

from boto3.dynamodb.conditions import And, Equals, Or
from pydantic import BaseModel

from aws_dynamodb_module.aws.dynamodb import (_dynamodb_client,
                                              _dynamodb_resource)
from aws_dynamodb_module.utils.dynamodb_iterator import (DynamoIterator,
                                                         FunctionBuilder)


class DynamodbService():

    def __init__(self) -> None:
        self._dynamodb_resource = _dynamodb_resource
        self._dynamodb_client = _dynamodb_client

    def db_iterator(
        self,
        query_in: dict,
    ) -> DynamoIterator:
        """Use this function to query data from AWS DynamoDB

        :param query_in: the DynamoDB query statement

        Example::
            from boto3.dynamodb.conditions import Key, Attr

            gsi1_pk = "example_pk"
            gsi1_sk_prefix = "example_sk_prefix"
            krawgs = {
                'IndexName': 'GSI1',
                'KeyConditionExpression': Key('GSI1PK').eq(gsi1_pk)
                & Key('GSI1SK').begins_with(gsi1_sk_prefix)
            }
            iterator = db_iterator(**krawgs)
        """
        partial_scan_all = FunctionBuilder(
            self._dynamodb_resource.query
        ).having(**query_in).get()
        return DynamoIterator(partial_scan_all)

    def add_batch_items(
        self,
        items: List[Union[dict, BaseModel]],
    ) -> None:
        with self._dynamodb_resource.batch_writer(
            overwrite_by_pkeys=['PK', 'SK']
        ) as batch:
            for item in items:
                if isinstance(item, dict):
                    batch.put_item(
                        Item=item
                    )
                elif isinstance(item, BaseModel):
                    exclude = item.get_exclude()
                    batch.put_item(
                        Item=item.dict(by_alias=True, exclude=exclude)
                    )

    def delete_batch_items(
        self,
        items: List[dict]
    ) -> None:
        with self._dynamodb_resource.batch_writer() as batch:
            for delete_item in items:
                batch.delete_item(
                    Key={
                        'PK': delete_item['PK'],
                        'SK': delete_item['SK']
                    }
                )

    @staticmethod
    def get_combine_filter_express(
        list_filter: list
    ):
        filter_express = reduce(
            function=DynamodbService.chain_with_and_operator,
            sequence=list_filter,
        )
        return filter_express

    @staticmethod
    def chain_with_or_operator(
        attrs_1: Equals,
        attrs_2: Equals,
    ):
        return Or(attrs_1, attrs_2)

    @staticmethod
    def chain_with_and_operator(
        attrs_1: Equals,
        attrs_2: Equals,
    ):
        return And(attrs_1, attrs_2)


    @staticmethod
    def remove_pks(
        db_model: dict,
    ) -> None:
        keys = db_model.keys()
        for key in list(keys):
            if key in ['PK', 'SK'] or 'GSI' in key:
                db_model.pop(key)

    def update_item(
        self,
        pk: str,
        sk: str,
        update_expression: str,
        expression_attribute_values: dict = {},
        return_values: str = 'ALL_NEW',
    ) -> None:
        kwargs = {
            "Key": {
                'PK': pk,
                'SK': sk
            },
            "UpdateExpression": update_expression,
            "ConditionExpression": "attribute_exists(PK) AND "
            "attribute_exists(SK)",
            "ReturnValues": return_values
        }
        if expression_attribute_values:
            kwargs.update(
                {
                    "ExpressionAttributeValues": expression_attribute_values,
                }
            )
        response = self._dynamodb_resource.update_item(
            **kwargs
        )
        return response
