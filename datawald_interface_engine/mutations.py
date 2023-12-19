#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback
from graphene import String, Field, Mutation, Boolean, String, List, DateTime, Int
from silvaengine_utility import JSON
from .types import SyncTaskType, TxStagingType, ProductMetadataType
from .handlers import (
    insert_tx_staging_handler,
    update_tx_staging_status_handler,
    delete_tx_staging_handler,
    insert_update_sync_task_handler,
    delete_sync_task_handler,
    insert_update_product_metadata_handler,
    delete_product_metadata_handler,
    put_messages_handler,
    retry_sync_task_handler,
)


class InsertTxStaging(Mutation):
    tx_staging = Field(TxStagingType)

    class Arguments:
        source = String(required=True)
        tx_type_src_id = String(required=True)
        target = String(required=True)
        data = JSON(required=True)
        tx_status = String(required=True)
        tx_note = String(required=True)
        created_at = DateTime(required=True)
        updated_at = DateTime(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            tx_staging = insert_tx_staging_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return InsertTxStaging(tx_staging=tx_staging)


class UpdateTxStaging(Mutation):
    status = Boolean()

    class Arguments:
        source = String(required=True)
        tx_type_src_id = String(required=True)
        target = String(required=True)
        tgt_id = String(required=True)
        tx_note = String(required=True)
        tx_status = String(required=True)
        updated_at = DateTime(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            status = update_tx_staging_status_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return UpdateTxStaging(status=status)


class DeleteTxStaging(Mutation):
    status = Boolean()

    class Arguments:
        source = String(required=True)
        tx_type_src_id = String(required=True)
        target = String(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            status = delete_tx_staging_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return DeleteSyncTask(status=status)


class InsertUpdateSyncTask(Mutation):
    sync_task = Field(SyncTaskType)

    class Arguments:
        id = String()
        tx_type = String(required=True)
        source = String()
        target = String()
        cut_date = DateTime()
        offset = Int()
        entities = List(JSON, required=True)
        funct = String()

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            sync_task = insert_update_sync_task_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return InsertUpdateSyncTask(sync_task=sync_task)


class DeleteSyncTask(Mutation):
    status = Boolean()

    class Arguments:
        tx_type = String(required=True)
        id = String(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            status = delete_sync_task_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return DeleteSyncTask(status=status)


class InsertUpdateProductMetadata(Mutation):
    product_metadata = Field(ProductMetadataType)

    class Arguments:
        target_source = String(required=True)
        column = String(required=True)
        metadata = JSON(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            product_metadata = insert_update_product_metadata_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return InsertUpdateProductMetadata(product_metadata=product_metadata)


class DeleteProductMetadata(Mutation):
    status = Boolean()

    class Arguments:
        target_source = String(required=True)
        column = String(required=True)

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            status = delete_product_metadata_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return DeleteProductMetadata(status=status)


class PutMessages(Mutation):
    message_group_id = String()

    class Arguments:
        tx_type = String(required=True)
        source = String(required=True)
        target = String(required=True)
        funct = String()
        messages = List(JSON, required=True)
        message_group_id = String()

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            message_group_id = put_messages_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return PutMessages(message_group_id=message_group_id)


class RetrySyncTask(Mutation):
    message_group_id = String()

    class Arguments:
        tx_type = String(required=True)
        source = String(required=True)
        id = String(required=True)
        funct = String()

    @staticmethod
    def mutate(root, info, **kwargs):
        try:
            message_group_id = retry_sync_task_handler(info, **kwargs)
        except Exception:
            log = traceback.format_exc()
            info.context.get("logger").exception(log)
            raise

        return PutMessages(message_group_id=message_group_id)
