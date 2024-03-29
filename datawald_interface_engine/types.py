#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from graphene import (
    ObjectType,
    Field,
    List,
    String,
    Int,
    Decimal,
    DateTime,
    Boolean,
)
from silvaengine_utility import JSON
from silvaengine_dynamodb_base import ListObjectType


class CutDateType(ObjectType):
    cut_date = DateTime()
    offset = Int()


class SyncTaskType(ObjectType):
    task = String()
    id = String()
    source = String()
    target = String()
    tx_type = String()
    cut_date = DateTime()
    start_date = DateTime()
    end_date = DateTime()
    offset = Int()
    sync_note = String()
    sync_status = String()
    entities = List(JSON)


class TxStagingType(ObjectType):
    source = String()
    tx_type_src_id = String()
    target = String()
    tgt_id = String()
    data = JSON()
    old_data = JSON()
    created_at = DateTime()
    updated_at = DateTime()
    tx_note = String()
    tx_status = String()


class ProductMetadataType(ObjectType):
    target_source = String()
    column = String()
    metadata = JSON()
    created_at = DateTime()
    updated_at = DateTime()


class DataFeedEntityType(ObjectType):
    source = String()
    id = String()
    key = String()
    value = String()
    data = JSON()
    created_at = DateTime()
    updated_at = DateTime()


class TxStagingListType(ListObjectType):
    tx_staging_list = List(TxStagingType)


class SyncTaskListType(ListObjectType):
    sync_task_list = List(SyncTaskType)


class ProductMetadataListType(ListObjectType):
    product_metadata_list = List(ProductMetadataType)
