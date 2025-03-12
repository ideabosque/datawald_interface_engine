#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import copy
import math
import time
import traceback
import uuid
from datetime import datetime, timedelta

import boto3
from deepdiff import DeepDiff
from pytz import timezone
from tenacity import retry, stop_after_attempt, wait_exponential

from dynamodb_connector import DynamoDBConnector
from silvaengine_dynamodb_base import (
    delete_decorator,
    insert_update_decorator,
    monitor_decorator,
    resolve_list_decorator,
)
from silvaengine_utility import Utility

from .models import ProductMetadataModel, SyncTaskModel, TxStagingModel
from .types import (
    CutDateType,
    DataFeedEntityType,
    ProductMetadataListType,
    ProductMetadataType,
    SyncTaskListType,
    SyncTaskType,
    TxStagingListType,
    TxStagingType,
)

data_attributes_except_for_data_diff = ["created_at", "updated_at"]
sqs = None
aws_lambda = None
dynamodbconnector = None
default_cut_date = None
task_queue = None
input_queue = None
max_entities_in_message_body = None
allow_total_messages = None
deadline_hours = None
default_timezone = None
sync_task_notification = None
sync_statuses = None


def handlers_init(logger, **setting):
    global sqs, aws_lambda, dynamodbconnector, default_cut_date, task_queue, input_queue, max_entities_in_message_body, allow_total_messages, deadline_hours, default_timezone, sync_task_notification, sync_statuses
    if (
        setting.get("region_name")
        and setting.get("aws_access_key_id")
        and setting.get("aws_secret_access_key")
    ):
        sqs = boto3.resource(
            "sqs",
            region_name=setting.get("region_name"),
            aws_access_key_id=setting.get("aws_access_key_id"),
            aws_secret_access_key=setting.get("aws_secret_access_key"),
        )
        aws_lambda = boto3.client(
            "lambda",
            region_name=setting.get("region_name"),
            aws_access_key_id=setting.get("aws_access_key_id"),
            aws_secret_access_key=setting.get("aws_secret_access_key"),
        )
    else:
        sqs = boto3.resource("sqs")
        aws_lambda = boto3.client("lambda")

    dynamodbconnector = DynamoDBConnector(logger, **setting)
    default_cut_date = setting.get("default_cut_date", None)
    task_queue = sqs.get_queue_by_name(QueueName=setting.get("task_queue_name"))
    input_queue = sqs.get_queue_by_name(QueueName=setting.get("input_queue_name"))
    max_entities_in_message_body = int(setting.get("max_entities_in_message_body", 10))
    allow_total_messages = int(setting.get("allow_total_messages", 10))
    deadline_hours = int(setting.get("deadline_hours", 336))
    default_timezone = setting.get("default_timezone", "UTC")
    sync_task_notification = setting.get("sync_task_notification", {})
    sync_statuses = setting.get(
        # "sync_statuses", ["Completed", "Fail", "Incompleted", "Processing"]
        "sync_statuses",
        ["Completed"],
    )


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(5),
)
def get_tx_staging(source_target, tx_type_src_id):
    return TxStagingModel.get(source_target, tx_type_src_id)


def _get_tx_staging(source_target, tx_type_src_id):
    tx_staging = get_tx_staging(source_target, tx_type_src_id).__dict__[
        "attribute_values"
    ]
    source_target = tx_staging.pop("source_target")
    return dict(
        {
            "source": source_target.split("_")[0],
            "target": source_target.split("_")[1],
        },
        **tx_staging,
    )


def get_tx_staging_count(source_target, tx_type_src_id):
    return TxStagingModel.count(
        source_target, TxStagingModel.tx_type_src_id == tx_type_src_id
    )


def get_tx_staging_type(info, tx_staging):
    tx_staging = tx_staging.__dict__["attribute_values"]
    source_target = tx_staging.pop("source_target")
    tx_staging = dict(
        {
            "source": source_target.split("_")[0],
            "target": source_target.split("_")[1],
        },
        **tx_staging,
    )
    return TxStagingType(**Utility.json_loads(Utility.json_dumps(tx_staging)))


def resolve_tx_staging_handler(info, **kwargs):
    return get_tx_staging_type(
        info,
        get_tx_staging(
            f"{kwargs.get('source')}_{kwargs.get('target')}",
            kwargs.get("tx_type_src_id"),
        ),
    )


@monitor_decorator
@resolve_list_decorator(
    attributes_to_get=["source_target", "tx_type_src_id"],
    list_type_class=TxStagingListType,
    type_funct=get_tx_staging_type,
)
def resolve_tx_staging_list_handler(info, **kwargs):
    source_target = f"{kwargs.get('source')}_{kwargs.get('target')}"  ## PK

    ## Set up the query arguments with key condition expression.
    args = []
    inquiry_funct = TxStagingModel.scan
    count_funct = TxStagingModel.count
    if source_target:
        args = [source_target, None]
        inquiry_funct = TxStagingModel.query

    ## Set up the query arguments with filter expression.
    the_filters = None  # We can add filters for the query.
    if the_filters is not None:
        args.append(the_filters)

    return inquiry_funct, count_funct, args


def insert_tx_staging_handler(info, **kwargs):
    info.context.get("logger").info(kwargs)

    old_data = {}
    tx_status = kwargs.get("tx_status")
    created_at = kwargs.get("created_at")
    count = TxStagingModel.count(
        f"{kwargs.get('source')}_{kwargs.get('target')}",
        TxStagingModel.tx_type_src_id == kwargs.get("tx_type_src_id"),
    )

    if tx_status == "N" and count >= 1:
        tx_staging_model = TxStagingModel.get(
            f"{kwargs.get('source')}_{kwargs.get('target')}",
            kwargs.get("tx_type_src_id"),
        )
        created_at = tx_staging_model.created_at
        data_diff = DeepDiff(
            Utility.json_loads(
                Utility.json_dumps(tx_staging_model.data.__dict__["attribute_values"])
            ),
            Utility.json_loads(Utility.json_dumps(kwargs.get("data"))),
        )
        if data_diff == {}:
            if tx_staging_model.tx_status in ("S", "I"):
                tx_status = "I"
                tx_staging_model.update(
                    actions=[
                        TxStagingModel.updated_at.set(kwargs.get("updated_at")),
                        TxStagingModel.tx_note.set(
                            f"No update {kwargs.get('source')}_{kwargs.get('target')}/{kwargs.get('tx_type_src_id')}"
                        ),
                        TxStagingModel.tx_status.set(tx_status),
                    ]
                )
        else:
            old_data = tx_staging_model.data.__dict__["attribute_values"]

    if tx_status != "I":
        TxStagingModel(
            f"{kwargs.get('source')}_{kwargs.get('target')}",
            kwargs.get("tx_type_src_id"),
            **Utility.json_loads(
                Utility.json_dumps(
                    {
                        "data": kwargs.get("data"),
                        "old_data": old_data,
                        "created_at": created_at,
                        "updated_at": kwargs.get("updated_at"),
                        "tx_note": kwargs.get("tx_note"),
                        "tx_status": tx_status,
                    }
                ),
                parser_number=False,
            ),
        ).save()

    return get_tx_staging_type(
        info,
        get_tx_staging(
            f"{kwargs.get('source')}_{kwargs.get('target')}",
            kwargs.get("tx_type_src_id"),
        ),
    )


def update_tx_staging_status_handler(info, **kwargs):
    TxStagingModel.get(
        f"{kwargs.get('source')}_{kwargs.get('target')}", kwargs.get("tx_type_src_id")
    ).update(
        actions=[
            TxStagingModel.tgt_id.set(kwargs.get("tgt_id")),
            TxStagingModel.updated_at.set(datetime.now(tz=timezone("UTC"))),
            TxStagingModel.tx_note.set(kwargs.get("tx_note")),
            TxStagingModel.tx_status.set(kwargs.get("tx_status")),
        ]
    )
    return True


def delete_tx_staging_handler(info, **kwargs):
    TxStagingModel.get(
        f"{kwargs.get('source')}_{kwargs.get('target')}", kwargs.get("tx_type_src_id")
    ).delete()
    return True


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(5),
)
def get_sync_task(tx_type, id):
    return SyncTaskModel.get(tx_type, id)


def _get_sync_task(tx_type, id):
    sync_task = get_sync_task(tx_type, id)
    return sync_task.__dict__["attribute_values"]


def get_sync_task_count(tx_type, id):
    return SyncTaskModel.count(tx_type, SyncTaskModel.id == id)


def get_sync_task_type(info, sync_task):
    return SyncTaskType(
        **Utility.json_loads(Utility.json_dumps(sync_task.__dict__["attribute_values"]))
    )


def resolve_sync_task_handler(info, **kwargs):
    return get_sync_task_type(
        info, get_sync_task(kwargs.get("tx_type"), kwargs.get("id"))
    )


def resolve_cut_date_handler(info, **kwargs):
    cut_date = datetime.strptime(default_cut_date, "%Y-%m-%dT%H:%M:%S%z")
    offset = 0
    sync_tasks = [
        sync_task
        for sync_task in SyncTaskModel.tx_type_source_index.query(
            kwargs.get("tx_type"),
            SyncTaskModel.source == kwargs.get("source"),
            (SyncTaskModel.target == kwargs.get("target"))
            & (SyncTaskModel.sync_status.is_in(*sync_statuses)),
        )
    ]

    if len(sync_tasks) > 0:
        last_sync_task = max(
            sync_tasks,
            key=lambda sync_task: (sync_task.cut_date, int(sync_task.offset)),
        )
        id = last_sync_task.id
        cut_date = last_sync_task.cut_date
        offset = int(last_sync_task.offset)

        # Flsuh Sync Task Table by souce and tx_type.
        flush_sync_task(
            kwargs.get("tx_type"), kwargs.get("source"), kwargs.get("target"), id
        )
    return CutDateType(cut_date=cut_date, offset=offset)


def flush_sync_task(tx_type, source, target, id):
    for sync_task in SyncTaskModel.tx_type_source_index.query(
        tx_type,
        SyncTaskModel.source == source,
        (SyncTaskModel.target == target) & (SyncTaskModel.id != id),
    ):
        if sync_task.sync_status in sync_statuses:
            sync_task.delete(SyncTaskModel.id != id)


@monitor_decorator
@resolve_list_decorator(
    attributes_to_get=["tx_type", "id"],
    list_type_class=SyncTaskListType,
    type_funct=get_sync_task_type,
)
def resolve_sync_task_list_handler(info, **kwargs):
    tx_type = kwargs.get("tx_type")  ## PK
    source = kwargs.get("source")  ## LSK
    end_date_from = kwargs.get("end_date_from")
    sync_statuses = kwargs.get("sync_statuses")
    id = kwargs.get("id")

    ## Set up the query arguments with key condition expression.
    args = []
    inquiry_funct = SyncTaskModel.scan
    count_funct = SyncTaskModel.count
    if tx_type:
        args = [tx_type, None]
        inquiry_funct = SyncTaskModel.query
        if source:
            args[1] = SyncTaskModel.source == source
            inquiry_funct = SyncTaskModel.tx_type_source_index.query
            count_funct = SyncTaskModel.tx_type_source_index.count

    ## Set up the query arguments with filter expression.
    the_filters = None  # We can add filters for the query.
    if end_date_from:
        current = datetime.now(tz=timezone(default_timezone))
        deadline = end_date_from + timedelta(hours=deadline_hours)
        end_date_to = min(current, deadline)
        if kwargs.get("end_date_to"):
            end_date_to = min(current, deadline, kwargs.get("end_date_to"))
        the_filters &= SyncTaskModel.end_date.between(end_date_from, end_date_to)
    if sync_statuses:
        the_filters &= SyncTaskModel.sync_status.is_in(*sync_statuses)
    if id:
        the_filters &= SyncTaskModel.id.startswith(id)
    if the_filters is not None:
        args.append(the_filters)

    return inquiry_funct, count_funct, args


@insert_update_decorator(
    keys={
        "hash_key": "tx_type",
        "range_key": "id",
    },
    range_key_required=True,  ## Due S3 sync which will use the file name as the id.
    model_funct=get_sync_task,
    count_funct=get_sync_task_count,
    type_funct=get_sync_task_type,
    data_attributes_except_for_data_diff=data_attributes_except_for_data_diff,
    activity_history_funct=None,
)
def insert_update_sync_task_handler(info, **kwargs):
    tx_type = kwargs.get("tx_type")
    id = kwargs.get("id")
    if kwargs.get("entity") is None:
        SyncTaskModel(
            tx_type,
            id,
            **{
                "source": kwargs.get("source"),
                "target": kwargs.get("target"),
                "cut_date": kwargs.get("cut_date"),
                "start_date": datetime.now(tz=timezone("UTC")),
                "end_date": datetime.now(tz=timezone("UTC")),
                "offset": kwargs.get("offset", 0),
                "sync_note": f"Process {kwargs.get('tx_type')} data for source ({kwargs.get('source')}).",
                "sync_status": "Processing",
                "entities": kwargs.get("entities"),
            },
        ).save()

        sync_task = get_sync_task(tx_type, id)
        dispatch_sync_task(
            info.context.get("logger"),
            tx_type,
            id,
            kwargs.get("target"),
            kwargs.get("funct"),
            copy.deepcopy(sync_task.entities),
        )
        return

    sync_task = kwargs.get("entity")
    entities = kwargs.get("entities")
    sync_status = "Completed"
    if len(list(filter(lambda x: x["tx_status"] == "F", entities))) > 0:
        sync_status = "Fail"
    if len(list(filter(lambda x: x["tx_status"] == "?", entities))) > 0:
        sync_status = "Incompleted"

    sync_task.update(
        actions=[
            SyncTaskModel.sync_status.set(sync_status),
            SyncTaskModel.end_date.set(datetime.now(tz=timezone("UTC"))),
            SyncTaskModel.entities.set(entities),
        ]
    )

    # Send out the notification if the sync_task is completed.
    if (
        sync_task_notification.get(sync_task.target)
        and sync_task_notification[sync_task.target].get(tx_type)
        and sync_status != "Incompleted"
    ):
        endpoint_id = sync_task.target
        funct = sync_task_notification[sync_task.target][tx_type]

        Utility._invoke_funct_on_aws_sqs(
            info.context.get("logger"),
            task_queue,
            f"{tx_type}-{id}",
            **{
                "endpoint_id": endpoint_id,
                "funct": funct,
                "params": sync_task.__dict__["attribute_values"],
            },
        )

    return


@delete_decorator(
    keys={
        "hash_key": "tx_type",
        "range_key": "id",
    },
    model_funct=get_sync_task,
)
def delete_sync_task_handler(info, **kwargs):
    kwargs.get("entity").delete()
    return True


def dispatch_sync_task(logger, tx_type, id, target, funct, entities):
    try:
        message_body = {
            "params": {"entities": []},
        }

        while True:
            entity = entities.pop()
            if entity.get("tx_status", "N") != ["S", "I"]:
                message_body["params"]["entities"].append(
                    {
                        "source": entity["source"],
                        "tx_type_src_id": entity["tx_type_src_id"],
                        "target": entity["target"],
                        "created_at": entity["created_at"],
                        "updated_at": entity["updated_at"],
                    }
                )

            if (
                len(message_body["params"]["entities"]) >= max_entities_in_message_body
                or len(entities) == 0
            ):
                task_queue.send_message(
                    MessageAttributes={
                        "endpoint_id": {"StringValue": target, "DataType": "String"},
                        "funct": {"StringValue": funct, "DataType": "String"},
                    },
                    MessageBody=Utility.json_dumps(message_body),
                    MessageGroupId=f"{tx_type}-{id}",
                )
                message_body["params"]["entities"] = []

            if len(entities) == 0:
                message_body.update(
                    {
                        "params": {"tx_type": tx_type, "id": id},
                    }
                )
                task_queue.send_message(
                    MessageAttributes={
                        "endpoint_id": {"StringValue": target, "DataType": "String"},
                        "funct": {
                            "StringValue": "update_sync_task",
                            "DataType": "String",
                        },
                    },
                    MessageBody=Utility.json_dumps(message_body),
                    MessageGroupId=f"{tx_type}-{id}",
                )
                break

    except Exception:
        log = traceback.format_exc()
        logger.exception(log)
        raise


def retry_sync_task_handler(info, **kwargs):
    source = kwargs.get("source")
    tx_type = kwargs.get("tx_type")
    id = kwargs.get("id")
    message_group_id = f"resync_{tx_type}_{source}_{id}"
    params = {
        "tx_type": tx_type,
        "id": id,
    }
    if kwargs.get("funct"):
        params.update({"funct": kwargs.get("funct")})

    input_queue.send_message(
        MessageAttributes={
            "endpoint_id": {"StringValue": source, "DataType": "String"},
            "funct": {"StringValue": "retry_sync_task", "DataType": "String"},
        },
        MessageBody=Utility.json_dumps({"params": params}),
        MessageGroupId=message_group_id,
    )
    return message_group_id


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(5),
)
def get_product_metadata(target_source, column):
    return ProductMetadataModel.get(target_source, column)


def _get_product_metadata(target_source, column):
    product_metadata = get_product_metadata(target_source, column)
    return product_metadata.__dict__["attribute_values"]


def get_product_metadata_count(target_source, column):
    return ProductMetadataModel.count(
        target_source, ProductMetadataModel.column == column
    )


def get_product_metadata_type(info, product_metadata):
    return ProductMetadataType(
        **Utility.json_loads(
            Utility.json_dumps(product_metadata.__dict__["attribute_values"])
        )
    )


def resolve_product_metadata_handler(info, **kwargs):
    return get_product_metadata_type(
        info, get_product_metadata(kwargs.get("target_source"), kwargs.get("column"))
    )


@monitor_decorator
@resolve_list_decorator(
    attributes_to_get=["target_source", "column"],
    list_type_class=ProductMetadataListType,
    type_funct=get_product_metadata_type,
)
def resolve_product_metadata_list_handler(info, **kwargs):
    target_source = kwargs.get("target_source")  ## PK
    column = kwargs.get("column")  ## SK
    columns = kwargs.get("columns")  ## SK

    ## Set up the query arguments with key condition expression.
    args = []
    inquiry_funct = ProductMetadataModel.scan
    count_funct = ProductMetadataModel.count
    if target_source:
        args = [target_source, None]
        inquiry_funct = ProductMetadataModel.query

    ## Set up the query arguments with filter expression.
    the_filters = None  # We can add filters for the query.
    if column:
        the_filters &= ProductMetadataModel.column.contains(column)
    if columns:
        the_filters &= ProductMetadataModel.column.is_in(*columns)
    if the_filters is not None:
        args.append(the_filters)

    return inquiry_funct, count_funct, args


@insert_update_decorator(
    keys={
        "hash_key": "target_source",
        "range_key": "column",
    },
    range_key_required=True,
    model_funct=get_product_metadata,
    count_funct=get_product_metadata_count,
    type_funct=get_product_metadata_type,
    data_attributes_except_for_data_diff=data_attributes_except_for_data_diff,
    activity_history_funct=None,
)
def insert_update_product_metadata_handler(info, **kwargs):
    target_source = kwargs.get("target_source")
    column = kwargs.get("column")
    if kwargs.get("entity") is None:
        ProductMetadataModel(
            target_source,
            column,
            **{
                "metadata": Utility.json_loads(
                    Utility.json_dumps(kwargs.get("metadata")), parser_number=False
                ),
                "created_at": datetime.now(tz=timezone("UTC")),
                "updated_at": datetime.now(tz=timezone("UTC")),
            },
        ).save()
        return

    product_metadata = kwargs.get("entity")
    product_metadata.update(
        actions=[
            ProductMetadataModel.metadata.set(
                Utility.json_loads(
                    Utility.json_dumps(kwargs.get("metadata")), parser_number=False
                )
            ),
            ProductMetadataModel.updated_at.set(datetime.now(tz=timezone("UTC"))),
        ]
    )
    return


@delete_decorator(
    keys={
        "hash_key": "target_source",
        "range_key": "column",
    },
    model_funct=get_product_metadata,
)
def delete_product_metadata_handler(info, **kwargs):
    kwargs.get("entity").delete()
    return True


def put_messages_handler(info, **kwargs):
    tx_type = kwargs.get("tx_type")
    target = kwargs.get("target")
    source = kwargs.get("source")
    messages = kwargs.get("messages")
    funct = kwargs.get("funct", "retrieve_entities_from_source")
    message_group_id = kwargs.get("message_group_id")
    if message_group_id is None:
        id = uuid.uuid1().int >> 64
        message_group_id = f"{tx_type}_{source}_{target}_{id}"
    input_queue.send_message(
        MessageAttributes={
            "endpoint_id": {"StringValue": source, "DataType": "String"},
            "funct": {"StringValue": funct, "DataType": "String"},
        },
        MessageBody=Utility.json_dumps(
            {
                "params": {
                    "tx_type": tx_type,
                    "target": target,
                    "source": source,
                    "messages": messages,
                }
            }
        ),
        MessageGroupId=message_group_id,
    )
    return message_group_id


def resolve_data_feed_count_handler(info, **kwargs):
    source = kwargs.get("source")
    updated_at_from = kwargs.get("updated_at_from")
    updated_at_to = kwargs.get("updated_at_to")
    table_name = kwargs.get("table_name")
    count = dynamodbconnector.get_count(
        source, updated_at_from, updated_at_to, table_name=table_name
    )
    return count


def resolve_data_feed_entities_handler(info, **kwargs):
    table_name = kwargs.get("table_name")
    source = kwargs.get("source")
    updated_at_from = kwargs.get("updated_at_from")
    updated_at_to = kwargs.get("updated_at_to")
    key = kwargs.get("key")
    value = kwargs.get("value")

    _kwargs = {"table_name": table_name}
    if updated_at_from:
        current = datetime.now(tz=timezone(default_timezone))
        deadline = updated_at_from + timedelta(hours=deadline_hours)
        if updated_at_to:
            updated_at_to = min(current, deadline, updated_at_to)
        else:
            updated_at_to = min(current, deadline)
        _args = [source, updated_at_from, updated_at_to]
        if kwargs.get("limit"):
            _kwargs.update({"limit": kwargs.get("limit")})
        if kwargs.get("offset"):
            _kwargs.update({"offset": kwargs.get("offset")})
        data_feed_entities = dynamodbconnector.get_items(*_args, **_kwargs)
    elif key and value:
        _args = [source, value]
        _kwargs.update({"key": key})
        data_feed_entities = dynamodbconnector.get_items_by_key(*_args, **_kwargs)
    else:
        raise Exception("Miss variables!!!")

    for data_feed_entity in data_feed_entities:
        data_feed_entity.update({"key": key, "value": data_feed_entity.pop(key)})

    return [
        DataFeedEntityType(**Utility.json_loads(Utility.json_dumps(data_feed_entity)))
        for data_feed_entity in data_feed_entities
    ]
