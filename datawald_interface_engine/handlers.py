#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
from email import message
from html import entities

__author__ = "bibow"

import uuid, boto3, traceback, copy
from datetime import datetime, timedelta
from deepdiff import DeepDiff
from silvaengine_utility import Utility
from .models import TxStagingModel, SyncTaskModel, ProductMetadataModel
from .types import (
    CutDateType,
    SyncTaskType,
    TxStagingType,
    ProductMetadataType,
    DataFeedEntityType,
)
from tenacity import retry, wait_exponential, stop_after_attempt
from pytz import timezone
from dynamodb_connector import DynamoDBConnector

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


def handlers_init(logger, **setting):
    global sqs, aws_lambda, dynamodbconnector, default_cut_date, task_queue, input_queue, max_entities_in_message_body, allow_total_messages, deadline_hours, default_timezone
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
    deadline_hours = int(setting.get("deadline_hours", 96))
    default_timezone = setting.get("default_timezone", "UTC")


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(5),
)
def get_tx_staging(source, tx_type_src_id):
    return TxStagingModel.get(source, tx_type_src_id)


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(5),
)
def get_sync_task(tx_type, id):
    return SyncTaskModel.get(tx_type, id)


def resolve_tx_staging_handler(info, **kwargs):
    return TxStagingType(
        **Utility.json_loads(
            Utility.json_dumps(
                get_tx_staging(
                    kwargs.get("source"), kwargs.get("tx_type_src_id")
                ).__dict__["attribute_values"]
            )
        )
    )


def resolve_cut_date_handler(info, **kwargs):
    cut_date = datetime.strptime(default_cut_date, "%Y-%m-%dT%H:%M:%S%z")
    offset = 0
    sync_statuses = ["Completed", "Fail", "Incompleted", "Processing"]
    sync_tasks = [
        sync_task
        for sync_task in SyncTaskModel.tx_type_source_index.query(
            kwargs.get("tx_type"),
            SyncTaskModel.source == kwargs.get("source"),
            SyncTaskModel.sync_status.is_in(*sync_statuses),
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
        flush_sync_task(kwargs.get("tx_type"), kwargs.get("source"), id)
    return CutDateType(cut_date=cut_date, offset=offset)


def flush_sync_task(tx_type, source, id):
    for sync_task in SyncTaskModel.tx_type_source_index.query(
        tx_type, SyncTaskModel.source == source, SyncTaskModel.id != id
    ):
        sync_task.delete(SyncTaskModel.id != id)


def resolve_sync_task_handler(info, **kwargs):
    return SyncTaskType(
        **Utility.json_loads(
            Utility.json_dumps(
                get_sync_task(kwargs.get("tx_type"), kwargs.get("id")).__dict__[
                    "attribute_values"
                ]
            )
        )
    )


def resolve_sync_tasks_handler(info, **kwargs):
    tx_type = kwargs.get("tx_type")
    source = kwargs.get("source")
    end_date_from = kwargs.get("end_date_from")
    end_date_to = kwargs.get("end_date_to")
    sync_statuses = kwargs.get("sync_statuses")

    current = datetime.now(tz=timezone(default_timezone))
    deadline = end_date_from + timedelta(hours=deadline_hours)
    if end_date_to:
        end_date_to = min(current, deadline, end_date_to)
    else:
        end_date_to = min(current, deadline)

    args = [
        tx_type,
        SyncTaskModel.source == source,
    ]

    filters = ["SyncTaskModel.end_date.between(end_date_from, end_date_to)"]
    if sync_statuses:
        filters.append("SyncTaskModel.sync_status.is_in(*sync_statuses)")

    args = args + [eval(" & ".join(filters))]
    results = SyncTaskModel.tx_type_source_index.query(*args)
    entities = [entity for entity in results]
    return [
        SyncTaskType(
            **Utility.json_loads(
                Utility.json_dumps(entity.__dict__["attribute_values"])
            )
        )
        for entity in entities
    ]


def insert_tx_staging_handler(info, **kwargs):
    info.context.get("logger").info(kwargs)

    old_data = {}
    tx_status = kwargs.get("tx_status")
    created_at = kwargs.get("created_at")
    count = TxStagingModel.count(
        kwargs.get("source"),
        TxStagingModel.tx_type_src_id == kwargs.get("tx_type_src_id"),
    )

    if tx_status == "N" and count >= 1:
        tx_staging_model = TxStagingModel.get(
            kwargs.get("source"), kwargs.get("tx_type_src_id")
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
                            f"No update {kwargs.get('source')}/{kwargs.get('tx_type_src_id')}"
                        ),
                        TxStagingModel.tx_status.set(tx_status),
                    ]
                )
        else:
            old_data = tx_staging_model.data.__dict__["attribute_values"]

    if tx_status != "I":
        TxStagingModel(
            kwargs.get("source"),
            kwargs.get("tx_type_src_id"),
            **Utility.json_loads(
                Utility.json_dumps(
                    {
                        "target": kwargs.get("target"),
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

    tx_staging_model = get_tx_staging(
        kwargs.get("source"), kwargs.get("tx_type_src_id")
    )
    return TxStagingType(
        **Utility.json_loads(
            Utility.json_dumps(tx_staging_model.__dict__["attribute_values"])
        )
    )


def update_tx_staging_status_handler(info, **kwargs):
    TxStagingModel.get(kwargs.get("source"), kwargs.get("tx_type_src_id")).update(
        actions=[
            TxStagingModel.tgt_id.set(kwargs.get("tgt_id")),
            TxStagingModel.updated_at.set(datetime.utcnow()),
            TxStagingModel.tx_note.set(kwargs.get("tx_note")),
            TxStagingModel.tx_status.set(kwargs.get("tx_status")),
        ]
    )
    return True


def delete_tx_staging_handler(info, **kwargs):
    TxStagingModel.get(kwargs.get("source"), kwargs.get("tx_type_src_id")).delete()
    return True


def insert_sync_task_handler(info, **kwargs):
    id = str(uuid.uuid1().int >> 64)
    SyncTaskModel(
        kwargs.get("tx_type"),
        id,
        **{
            "source": kwargs.get("source"),
            "target": kwargs.get("target"),
            "cut_date": kwargs.get("cut_date"),
            "start_date": datetime.utcnow(),
            "end_date": datetime.utcnow(),
            "offset": kwargs.get("offset", 0),
            "sync_note": f"Process {kwargs.get('tx_type')} data for source ({kwargs.get('source')}).",
            "sync_status": "Processing",
            "entities": kwargs.get("entities"),
        },
    ).save()

    sync_task_model = get_sync_task(kwargs.get("tx_type"), id)
    dispatch_sync_task(
        info.context.get("logger"),
        kwargs.get("tx_type"),
        id,
        kwargs.get("target"),
        kwargs.get("funct"),
        copy.deepcopy(sync_task_model.entities),
    )
    return SyncTaskType(
        **Utility.json_loads(
            Utility.json_dumps(sync_task_model.__dict__["attribute_values"])
        )
    )


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


def update_sync_task_handler(info, **kwargs):
    entities = kwargs.get("entities")

    sync_status = "Completed"
    if len(list(filter(lambda x: x["tx_status"] == "F", entities))) > 0:
        sync_status = "Fail"
    if len(list(filter(lambda x: x["tx_status"] == "?", entities))) > 0:
        sync_status = "Incompleted"

    sync_task_model = SyncTaskModel.get(kwargs.get("tx_type"), kwargs.get("id"))
    sync_task_model.update(
        actions=[
            SyncTaskModel.sync_status.set(sync_status),
            SyncTaskModel.end_date.set(datetime.utcnow()),
            SyncTaskModel.entities.set(entities),
        ]
    )

    return SyncTaskType(
        **Utility.json_loads(
            Utility.json_dumps(sync_task_model.__dict__["attribute_values"])
        )
    )


def delete_sync_task_handler(info, **kwargs):
    SyncTaskModel.get(kwargs.get("tx_type"), kwargs.get("id")).delete()
    return True


def resolve_product_metadatas_handle(info, **kwargs):
    results = ProductMetadataModel.query(kwargs.get("target_source"))
    return [
        ProductMetadataType(
            **Utility.json_loads(
                Utility.json_dumps(entity.__dict__["attribute_values"])
            )
        )
        for entity in results
    ]


def insert_product_metadata_handler(info, **kwargs):
    ProductMetadataModel(
        kwargs.get("target_source"),
        kwargs.get("column"),
        **Utility.json_loads(
            Utility.json_dumps(
                {
                    "metadata": kwargs.get("metadata"),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            ),
            parser_number=False,
        ),
    ).save()

    product_metadata_model = ProductMetadataModel.get(
        kwargs.get("target_source"), kwargs.get("column")
    )
    return ProductMetadataType(
        **Utility.json_loads(
            Utility.json_dumps(product_metadata_model.__dict__["attribute_values"])
        )
    )


def update_product_metadata_handler(info, **kwargs):
    product_metadata_model = ProductMetadataModel.get(
        kwargs.get("target_source"), kwargs.get("column")
    )
    product_metadata_model.update(
        actions=[
            ProductMetadataModel.metadata.set(
                Utility.json_loads(
                    Utility.json_dumps(kwargs.get("metadata")), parser_number=False
                )
            ),
            ProductMetadataModel.updated_at.set(datetime.utcnow()),
        ]
    )

    return ProductMetadataType(
        **Utility.json_loads(
            Utility.json_dumps(product_metadata_model.__dict__["attribute_values"])
        )
    )


def delete_product_metadata_handler(info, **kwargs):
    ProductMetadataModel.get(kwargs.get("target_source"), kwargs.get("column")).delete()
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
