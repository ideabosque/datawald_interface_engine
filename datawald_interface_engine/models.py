#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import os
from pynamodb.models import Model
from pynamodb.attributes import (
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UTCDateTimeAttribute,
    BooleanAttribute,
)
from pynamodb.indexes import GlobalSecondaryIndex, LocalSecondaryIndex, AllProjection


class BaseModel(Model):
    class Meta:
        region = os.getenv("REGIONNAME")
        billing_mode = "PAY_PER_REQUEST"


class TxTypeSourceIndex(LocalSecondaryIndex):
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = "tx_type-source-index"
        billing_mode = "PAY_PER_REQUEST"
        projection = AllProjection()

    # This attribute is the hash key for the index
    # Note that this attribute must also exist
    # in the model
    tx_type = UnicodeAttribute(hash_key=True)
    source = UnicodeAttribute(range_key=True)


class SyncTaskModel(BaseModel):
    class Meta(BaseModel.Meta):
        table_name = "dw-sync_task"

    tx_type = UnicodeAttribute(hash_key=True)
    id = UnicodeAttribute(range_key=True)
    source = UnicodeAttribute()
    target = UnicodeAttribute()
    cut_date = UTCDateTimeAttribute()
    start_date = UTCDateTimeAttribute()
    end_date = UTCDateTimeAttribute()
    offset = NumberAttribute()
    sync_note = UnicodeAttribute()
    sync_status = UnicodeAttribute()
    entities = ListAttribute(of=MapAttribute)
    tx_type_source_index = TxTypeSourceIndex()


class TxStagingModel(BaseModel):
    class Meta(BaseModel.Meta):
        table_name = "dw-tx_staging"

    source = UnicodeAttribute(hash_key=True)
    tx_type_src_id = UnicodeAttribute(range_key=True)
    target = UnicodeAttribute()
    tgt_id = UnicodeAttribute(null=True)
    data = MapAttribute()
    old_data = MapAttribute()
    created_at = UTCDateTimeAttribute()
    updated_at = UTCDateTimeAttribute()
    tx_note = UnicodeAttribute()
    tx_status = UnicodeAttribute()


class ProductMetadataModel(BaseModel):
    class Meta(BaseModel.Meta):
        table_name = "dw-product_metadata"

    target = UnicodeAttribute(hash_key=True)
    column = UnicodeAttribute(range_key=True)
    metadata = MapAttribute()
    created_at = UTCDateTimeAttribute()
    updated_at = UTCDateTimeAttribute()
