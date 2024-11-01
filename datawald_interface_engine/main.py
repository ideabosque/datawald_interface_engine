#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from typing import List

from graphene import Schema

from silvaengine_dynamodb_base import SilvaEngineDynamoDBBase

from .handlers import handlers_init
from .schema import Mutations, Query, type_class


# Hook function applied to deployment
def deploy() -> List:
    return [
        {
            "service": "DataWald Integration",
            "class": "DataWaldInterfaceEngine",
            "functions": {
                "datawald_interface_graphql": {
                    "is_static": False,
                    "label": "DataWald Interface GraphQL",
                    "query": [
                        {
                            "action": "txStaging",
                            "label": "View TX Staging",
                        },
                        {
                            "action": "txStagingList",
                            "label": "View TX Staging List",
                        },
                        {
                            "action": "cutDate",
                            "label": "View Cut Date",
                        },
                        {
                            "action": "syncTask",
                            "label": "View Sync Task",
                        },
                        {
                            "action": "syncTaskList",
                            "label": "View Sync Task List",
                        },
                        {
                            "action": "productMetadata",
                            "label": "View Product Metadata",
                        },
                        {
                            "action": "dataFeedCount",
                            "label": "View Data Feed Count",
                        },
                        {
                            "action": "dataFeedEntities",
                            "label": "View Data Feed Entities",
                        },
                    ],
                    "mutation": [
                        {
                            "action": "insertTxStaging",
                            "label": "Create Tx Staging",
                        },
                        {
                            "action": "upateTxStaging",
                            "label": "Update Tx Staging",
                        },
                        {
                            "action": "deleteTxStaging",
                            "label": "Delete Tx Staging",
                        },
                        {
                            "action": "InsertUpdateSyncTask",
                            "label": "Create Update Sync Task",
                        },
                        {
                            "action": "DeleteSyncTask",
                            "label": "Delete Sync Task",
                        },
                        {
                            "action": "insertUpdateProductMetadata",
                            "label": "Insert Update Product Metadata",
                        },
                        {
                            "action": "deleteProductMetadata",
                            "label": "Delete Product Metadata",
                        },
                        {
                            "action": "putMessages",
                            "label": "Put Messages",
                        },
                        {
                            "action": "retrySyncTask",
                            "label": "Retry Sync Task",
                        },
                    ],
                    "type": "RequestResponse",
                    "support_methods": ["POST"],
                    "is_auth_required": False,
                    "is_graphql": True,
                    "settings": "datawald_interface_engine",
                    "disabled_in_resources": True,  # Ignore adding to resource list.
                },
            },
        }
    ]


class DataWaldInterfaceEngine(SilvaEngineDynamoDBBase):
    def __init__(self, logger, **setting):
        handlers_init(logger, **setting)

        self.logger = logger
        self.setting = setting

        SilvaEngineDynamoDBBase.__init__(self, logger, **setting)

    def datawald_interface_graphql(self, **params):
        schema = Schema(
            query=Query,
            mutation=Mutations,
            types=type_class(),
        )
        return self.graphql_execute(schema, **params)
