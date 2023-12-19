#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import time
from graphene import ObjectType, String, List, Field, Int, DateTime
from .types import (
    CutDateType,
    SyncTaskType,
    SyncTaskListType,
    TxStagingType,
    TxStagingListType,
    ProductMetadataType,
    ProductMetadataListType,
    DataFeedEntityType,
)
from .mutations import (
    InsertTxStaging,
    UpdateTxStaging,
    DeleteTxStaging,
    InsertUpdateSyncTask,
    DeleteSyncTask,
    InsertUpdateProductMetadata,
    DeleteProductMetadata,
    PutMessages,
    RetrySyncTask,
)
from .queries import (
    resolve_tx_staging,
    resolve_tx_staging_list,
    resolve_cut_date,
    resolve_sync_task,
    resolve_sync_task_list,
    resolve_product_metadata,
    resolve_product_metadata_list,
    resolve_data_feed_count,
    resolve_data_feed_entities,
)


def type_class():
    return [
        CutDateType,
        SyncTaskType,
        SyncTaskListType,
        TxStagingType,
        TxStagingListType,
        ProductMetadataType,
        ProductMetadataListType,
        DataFeedEntityType,
    ]


class Query(ObjectType):
    ping = String()
    tx_staging = Field(
        TxStagingType,
        required=True,
        source=String(required=True),
        tx_type_src_id=String(required=True),
        target=String(required=True),
    )

    tx_staging_list = Field(
        TxStagingListType,
        page_number=Int(),
        limit=Int(),
        source=String(required=True),
        target=String(required=True),
        tx_type=String(),
    )

    cut_date = Field(
        CutDateType,
        tx_type=String(required=True),
        source=String(required=True),
        target=String(required=True),
    )

    sync_task = Field(
        SyncTaskType,
        tx_type=String(required=True),
        id=String(required=True),
    )

    sync_task_list = Field(
        SyncTaskListType,
        page_number=Int(),
        limit=Int(),
        tx_type=String(required=True),
        source=String(),
        end_date_from=DateTime(),
        end_date_to=DateTime(),
        sync_statuses=List(String),
        id=String(),
    )

    product_metadata = Field(
        ProductMetadataType,
        target_source=String(required=True),
        column=String(required=True),
    )

    product_metadata_list = Field(
        ProductMetadataListType,
        target_source=String(required=True),
    )

    data_feed_count = Field(
        Int,
        source=String(required=True),
        updated_at_from=DateTime(required=True),
        updated_at_to=DateTime(required=True),
        table_name=String(required=True),
    )

    data_feed_entities = List(
        DataFeedEntityType,
        table_name=String(required=True),
        source=String(required=True),
        updated_at_from=DateTime(),
        updated_at_to=DateTime(),
        limit=Int(),
        offset=Int(),
        value=String(),
        key=String(required=True),
    )

    def resolve_ping(self, info):
        return f"Hello at {time.strftime('%X')}!!"

    def resolve_tx_staging(self, info, **kwargs):
        return resolve_tx_staging(info, **kwargs)

    def resolve_tx_staging_list(self, info, **kwargs):
        return resolve_tx_staging_list(info, **kwargs)

    def resolve_cut_date(self, info, **kwargs):
        return resolve_cut_date(info, **kwargs)

    def resolve_sync_task(self, info, **kwargs):
        return resolve_sync_task(info, **kwargs)

    def resolve_sync_task_list(self, info, **kwargs):
        return resolve_sync_task_list(info, **kwargs)

    def resolve_product_metadata(self, info, **kwargs):
        return resolve_product_metadata(info, **kwargs)

    def resolve_product_metadata_list(self, info, **kwargs):
        return resolve_product_metadata_list(info, **kwargs)

    def resolve_data_feed_count(self, info, **kwargs):
        return resolve_data_feed_count(info, **kwargs)

    def resolve_data_feed_entities(self, info, **kwargs):
        return resolve_data_feed_entities(info, **kwargs)


class Mutations(ObjectType):
    insert_tx_staging = InsertTxStaging.Field()
    update_tx_staging = UpdateTxStaging.Field()
    delete_tx_staging = DeleteTxStaging.Field()
    insert_update_sync_task = InsertUpdateSyncTask.Field()
    delete_sync_task = DeleteSyncTask.Field()
    insert_update_product_metadata = InsertUpdateProductMetadata.Field()
    delete_product_metadata = DeleteProductMetadata.Field()
    put_messages = PutMessages.Field()
    retry_sync_task = RetrySyncTask.Field()
