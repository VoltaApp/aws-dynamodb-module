from functools import reduce
from typing import List, Union

import boto3
from boto3.dynamodb.conditions import And, Equals, Or
from pydantic import BaseModel

from aws_dynamodb_module.config.aws_dynamodb import DEFAULT_DYNAMODB_CONFIG
from aws_dynamodb_module.utils.dynamodb_iterator import (DynamoIterator,
                                                         FunctionBuilder)


class DynamodbService():
    ''' Query and manipulate the AWS DynamoDB

        Example::

            # Example generated from non-compiling source. May contain errors.
            from aws_dynamodb_module.services.dynamodb_service import DynamodbService
            from boto3.dynamodb.conditions import Key, Attr
            from botocore.config import Config

            dynamodb_table: str = "your_dynamodb_table"
            dynamodb_config: dict = {
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
            dynamodb_service = DynamodbService(
                dynamodb_table=dynamodb_table,
                dynamodb_config=dynamodb_config
            )
            # Add an item
            dynamodb_service.add_item(
                item={
                    "PK": "PK",
                    "SK": "SK"
                }
            )
            # Add list of items
            dynamodb_service.add_batch_items(
                items=[
                    {
                        "PK": "PK",
                        "SK": "SK"
                    }
                ]
            )
            # Get items as iterator
            iter_items = dynamodb_service.db_iterator(
                query_in={
                    "IndexName": "GSI1",
                    "KeyConditionExpression": Key("GSI1PK").eq("PK")
                    & Key("SK").begins_with("GSI1SK")
                }
            )
            # Get items as list
            iter_items.get_items_as_list()
            # Get first item in list
            iter_items.get_first_item()
            # Delete list of items
            dynamodb_service.delete_batch_items(
                items=[
                    {
                        "PK": "PK",
                        "SK": "SK"
                    }
                ]
            )
    '''

    def __init__(
        self,
        dynamodb_table: str = "dev.volta",
        dynamodb_config: dict = DEFAULT_DYNAMODB_CONFIG,
    ) -> None:
        self._dynamodb_tbl = dynamodb_table
        self._dynamodb_client = boto3.client(
            **dynamodb_config,
        )
        self._dynamodb_resource = boto3.resource(
            **dynamodb_config,
        ).Table(dynamodb_table)

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
                "IndexName": "GSI1",
                "KeyConditionExpression": Key("GSI1PK").eq(gsi1_pk)
                & Key("GSI1SK").begins_with(gsi1_sk_prefix)
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
            overwrite_by_pkeys=["PK", "SK"]
        ) as batch:
            for item in items:
                item = self._standalize_dynamodb_item(item)
                batch.put_item(Item=item)

    def add_item(
        self,
        item: Union[dict, BaseModel],
        condition_expression: str = "",
    ) -> None:
        item = self._standalize_dynamodb_item(item)
        put_item_params = {
            "Item": item,
        }
        if condition_expression:
            put_item_params.update({
                "ConditionExpression": condition_expression,
            })
        self._dynamodb_resource.put_item(**put_item_params)

    def delete_batch_items(
        self,
        items: List[dict]
    ) -> None:
        with self._dynamodb_resource.batch_writer() as batch:
            for delete_item in items:
                batch.delete_item(
                    Key={
                        "PK": delete_item["PK"],
                        "SK": delete_item["SK"]
                    }
                )

    def delete_item(
        self,
        item: dict,
        condition_expression: str = "attribute_exists(PK) AND attribute_exists(SK)",
    ) -> None:
        self._dynamodb_resource.delete_item(
            Key={
                "PK": item.get("PK"),
                "SK": item.get("SK")
            },
            ConditionExpression=condition_expression,
        )

    @staticmethod
    def get_combine_filter_express(
        filter_list: list
    ):
        filter_express = reduce(
            function=DynamodbService.chain_with_and_operator,
            sequence=filter_list,
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
        attr_1: Equals,
        attr_2: Equals,
    ):
        return And(attr_1, attr_2)

    @staticmethod
    def remove_pks(
        db_model: dict,
    ) -> None:
        keys = db_model.keys()
        for key in list(keys):
            if key in ["PK", "SK"] or "GSI" in key:
                db_model.pop(key)

    def update_item(
        self,
        pk: str,
        sk: str,
        update_expression: str,
        expression_attribute_values: dict = {},
        condition_expression: str = "attribute_exists(PK) AND attribute_exists(SK)",
        return_values: str = "ALL_NEW",
    ) -> None:
        kwargs = {
            "Key": {
                "PK": pk,
                "SK": sk
            },
            "UpdateExpression": update_expression,
            "ConditionExpression": condition_expression,
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

    def _standalize_dynamodb_item(
        self,
        item: Union[dict, BaseModel]
    ) -> dict:
        if isinstance(item, BaseModel):
            exclude = item.get_exclude()
            item = item.dict(by_alias=True, exclude=exclude)
        return item
