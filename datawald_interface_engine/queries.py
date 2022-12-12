#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from .handlers import (
    resolve_tx_staging_handler,
    resolve_tx_stagings_handler,
    resolve_cut_date_handler,
    resolve_sync_task_handler,
    resolve_sync_tasks_handler,
    resolve_sync_task_list_handler,
    resolve_product_metadatas_handle,
    resolve_data_feed_count_handler,
    resolve_data_feed_entities_handler,
)


def resolve_tx_staging(info, **kwargs):
    return resolve_tx_staging_handler(info, **kwargs)


def resolve_tx_stagings(info, **kwargs):
    return resolve_tx_stagings_handler(info, **kwargs)


def resolve_cut_date(info, **kwargs):
    return resolve_cut_date_handler(info, **kwargs)


def resolve_sync_task(info, **kwargs):
    return resolve_sync_task_handler(info, **kwargs)


def resolve_sync_tasks(info, **kwargs):
    return resolve_sync_tasks_handler(info, **kwargs)


def resolve_sync_task_list(info, **kwargs):
    return resolve_sync_task_list_handler(info, **kwargs)


def resolve_product_metadatas(info, **kwargs):
    return resolve_product_metadatas_handle(info, **kwargs)


def resolve_data_feed_count(info, **kwargs):
    return resolve_data_feed_count_handler(info, **kwargs)


def resolve_data_feed_entities(info, **kwargs):
    return resolve_data_feed_entities_handler(info, **kwargs)
