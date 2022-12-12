#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import logging, sys, unittest, os
from dotenv import load_dotenv

load_dotenv()
setting = {
    "region_name": os.getenv("region_name"),
    "aws_access_key_id": os.getenv("aws_access_key_id"),
    "aws_secret_access_key": os.getenv("aws_secret_access_key"),
    "default_cut_date": os.getenv("default_cut_date"),
    "task_queue_name": os.getenv("task_queue_name"),
    "input_queue_name": os.getenv("input_queue_name"),
    "max_entities_in_message_body": os.getenv("max_entities_in_message_body"),
    "sync_task_notification": {"ss3": {"inventory": "import_inventory_email"}},
}

sys.path.insert(0, "/var/www/projects/datawald_interface_engine")
sys.path.insert(1, "/var/www/projects/dynamodb_connector")

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

from datawald_interface_engine import DataWaldInterfaceEngine


class DataWaldInterfaceEngineTest(unittest.TestCase):
    def setUp(self):
        self.datawald_interface_engine = DataWaldInterfaceEngine(logger, **setting)
        logger.info("Initiate DataWaldInterfaceEngineTest ...")

    def tearDown(self):
        logger.info("Destory DataWaldInterfaceEngineTest ...")

    @unittest.skip("demonstrating skipping")
    def test_graphql_ping(self):
        query = """
            query {
                ping
            }
        """
        variables = {}

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_insert_tx_staging(self):
        query = """
            mutation insertTxStaging(
                    $source: String!,
                    $txTypeSrcId: String!,
                    $target: String!,
                    $data: JSON!,
                    $txStatus: String!,
                    $txNote: String!,
                    $createdAt: DateTime!,
                    $updatedAt: DateTime!

                ) {
                insertTxStaging(
                    source: $source,
                    txTypeSrcId: $txTypeSrcId,
                    target: $target,
                    data: $data,
                    txStatus: $txStatus,
                    txNote: $txNote,
                    createdAt: $createdAt,
                    updatedAt: $updatedAt
                ) {
                    txStaging{
                        source
                        txTypeSrcId
                        target
                        tgtId
                        data
                        oldData
                        createdAt
                        updatedAt
                        txNote
                        txStatus
                    }
                }
            }
        """
        variables = {
            "source": "MAGE2SQS-SANDBOX",
            "txTypeSrcId": "order-2000051509",
            "target": "NS-MAGE2-SANDBOX",
            "data": {
                "additional_tc_timestamp": "2021-05-16 13:42:12",
                "additional_tc_version": "####",
                "addresses": {
                    "billto": {
                        "address": "47 Discovery",
                        "city": "Irvine",
                        "company": "SilkSoftware",
                        "country": "US",
                        "email": "daniel.qin@silksoftware.com",
                        "firstname": "D",
                        "lastname": "Q",
                        "postcode": "92618",
                        "region": "California",
                    },
                    "shipto": {
                        "address": "3369 Ashley Phosphate Rd",
                        "city": "North Charleston",
                        "company": "All Phase Nutra",
                        "country": "US",
                        "email": "daniel.qin@silksoftware.com",
                        "firstname": "D",
                        "lastname": "Q",
                        "postcode": "29418",
                        "region": "South Carolina",
                    },
                },
                "bo_customer_id": "TBD",
                "class": "Sales-Online",
                "create_dt": "2021-05-16 13:45:06",
                "customer_po_no": "202105161642",
                "delivery_type": "Regular",
                "fe_customer_id": "13378",
                "fe_oppo_id": "2000051509",
                "fe_order_date": "2021-05-16 13:42:12",
                "fe_order_id": "2000051509",
                "fe_order_status": "new",
                "fe_order_update_date": "2021-05-16 13:42:12",
                "firstname": "D",
                "fob_remarks": "Origin",
                "freight_terms": "Collect",
                "frontend": "MAGE2SQS-SANDBOX",
                "hold_reason": "Pending Online Order Approval",
                "items": [
                    {
                        "price": "9.5000",
                        "qty": "50.0000",
                        "sku": "84900-146-10310-11648",
                    }
                ],
                "lastname": "Q",
                "order_type": "Online",
                "payment_method": "banktransfer",
                "ship_method_internal_id": "15",
                "shipping_carrier_other": "To be determined by shipper",
                "shipping_instructions": None,
                "shipping_total": "133.7900",
                "subsidiary": "GWI",
                "warehouse": "GWI-Chino",
            },
            "txStatus": "N",
            "txNote": "MAGE2SQS-SANDBOX -> DataWald",
            "createdAt": "2022-03-08T13:00:00",
            "updatedAt": "2022-03-08T13:00:00",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_update_tx_staging(self):
        query = """
            mutation updateTxStaging(
                    $source: String!,
                    $txTypeSrcId: String!,
                    $target: String!,
                    $tgtId: String!,
                    $txStatus: String!,
                    $txNote: String!,
                    $updatedAt: DateTime!

                ) {
                updateTxStaging(
                    source: $source,
                    txTypeSrcId: $txTypeSrcId,
                    target: $target,
                    tgtId: $tgtId,
                    txStatus: $txStatus,
                    txNote: $txNote,
                    updatedAt: $updatedAt
                ) {
                    status
                }
            }
        """
        variables = {
            "source": "MAGE2SQS-SANDBOX",
            "txTypeSrcId": "order-2000051509",
            "target": "NS-MAGE2-SANDBOX",
            "tgtId": "xxxx1",
            "txStatus": "S",
            "txNote": "DataWald -> NS-MAGE2",
            "updatedAt": "2022-03-08T13:00:00",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_delete_tx_staging(self):
        query = """
            mutation deleteTxStaging(
                    $source: String!,
                    $txTypeSrcId: String!,
                    $target: String!
                ) {
                deleteTxStaging(
                    source: $source,
                    txTypeSrcId: $txTypeSrcId,
                    target: $target
                ) {
                    status
                }
            }
        """
        variables = {
            "source": "MAGE2SQS-SANDBOX",
            "txTypeSrcId": "order-2000051509",
            "target": "NS-MAGE2-SANDBOX",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_tx_staging(self):
        query = """
            query($source: String!, $txTypeSrcId: String!, $target: String!) {
                txStaging(source: $source, txTypeSrcId: $txTypeSrcId, target: $target) {
                    source
                    txTypeSrcId
                    target
                    tgtId
                    data
                    oldData
                    createdAt
                    updatedAt
                    txNote
                    txStatus
                }
            }
        """
        variables = {
            "source": "sqs",
            "txTypeSrcId": "order-201",
            "target": "ns",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_tx_stagings(self):
        query = """
            query($pageNumber: Int, $limit: Int, $source: String!, $target: String!, $txType: String) {
                txStagings(pageNumber: $pageNumber, limit: $limit, source: $source, target: $target, txType: $txType) {
                    txStagings{
                        source
                        txTypeSrcId
                        target
                        tgtId
                        data
                        oldData
                        createdAt
                        updatedAt
                        txNote
                        txStatus
                    }
                    pageSize
                    pageNumber
                    total
                }
            }
        """
        variables = {
            "source": "sqs",
            "target": "ns",
            "txType": "order",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_insert_sync_task(self):
        query = """
            mutation insertSyncTask(
                    $id: String,
                    $txType: String!,
                    $source: String!,
                    $target: String!,
                    $cutDate: DateTime!,
                    $offset: Int,
                    $entities: [JSON]!,
                    $funct: String!
                ) {
                insertSyncTask(
                    id: $id,
                    txType: $txType,
                    source: $source,
                    target: $target,
                    cutDate: $cutDate,
                    offset: $offset,
                    entities: $entities,
                    funct: $funct
                ) {
                    syncTask{
                        txType
                        id
                        source
                        target
                        cutDate
                        startDate
                        endDate
                        offset
                        syncNote
                        syncStatus
                        entities
                    }
                }
            }
        """
        variables = {
            "txType": "order",
            "source": "MAGE2SQS-SANDBOX",
            "target": "NS-MAGE2-SANDBOX",
            "cutDate": "2022-03-08T13:00:00",
            "offset": 0,
            "entities": [
                {
                    "source": "MAGE2SQS-SANDBOX",
                    "tx_type_src_id": "order-2000051509",
                    "target": "NS-MAGE2-SANDBOX",
                    "created_at": "2022-03-08T13:00:00",
                    "updated_at": "2022-03-08T13:00:00",
                }
            ],
            "funct": "insert_update_entities_to_target",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_update_sync_task(self):
        query = """
            mutation updateSyncTask(
                    $txType: String!,
                    $id: String!,
                    $entities: [JSON]!
                ) {
                updateSyncTask(
                    txType: $txType,
                    id: $id,
                    entities: $entities
                ) {
                    syncTask{
                        txType
                        id
                        source
                        target
                        cutDate
                        startDate
                        endDate
                        offset
                        syncNote
                        syncStatus
                        entities
                    }
                }
            }
        """
        variables = {
            "txType": "inventory",
            "id": "1653017238675",
            "entities": [
                {
                    "created_at": "2022-05-20T03:27:24+0000",
                    "source": "s3",
                    "tx_note": "datawald -> ss3",
                    "tx_status": "S",
                    "tx_type_src_id": "inventory-ASHWAGANDHARTPDRBOX55",
                    "updated_at": "2022-05-20T03:27:42+0000",
                }
            ],
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_delete_sync_task(self):
        query = """
            mutation deleteSyncTask(
                    $txType: String!,
                    $id: String!
                ) {
                deleteSyncTask(
                    txType: $txType,
                    id: $id
                ) {
                    status
                }
            }
        """
        variables = {"txType": "order", "id": "13229596337889808876"}

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_sync_task(self):
        query = """
            query($txType: String!, $id: String!) {
                syncTask(txType: $txType, id: $id) {
                    txType
                    id
                    source
                    target
                    cutDate
                    startDate
                    endDate
                    offset
                    syncNote
                    syncStatus
                    entities
                }
            }
        """
        variables = {
            "txType": "order",
            "id": "4175549117482144236",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_sync_tasks(self):
        query = """
            query(
                $txType: String!, 
                $source: String!, 
                $endDateFrom: DateTime!, 
                $endDateTo: DateTime, 
                $syncStatuses: [String],
                $id: String,
                ) {
                syncTasks(
                    txType: $txType, 
                    source: $source, 
                    endDateFrom: $endDateFrom, 
                    endDateTo: $endDateTo, 
                    syncStatuses: $syncStatuses
                    id: $id) {
                    txType
                    id
                    source
                    target
                    cutDate
                    startDate
                    endDate
                    offset
                    syncNote
                    syncStatus
                    entities
                }
            }
        """
        variables = {
            "txType": "order",
            "source": "ns",
            "endDateFrom": "2022-03-21T16:06:40+0000",
            # "endDateTo": "2022-03-24T16:06:40+0000",
            "syncStatuses": ["Completed", "Fail", "Processing"],
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)


    # @unittest.skip("demonstrating skipping")
    def test_graphql_sync_task_list(self):
        query = """
            query(
                $txType: String!, 
                $source: String!, 
                $pageNumber: Int, 
                $limit: Int
                ) {
                syncTaskList(
                    txType: $txType, 
                    source: $source,
                    pageNumber: $pageNumber, 
                    limit: $limit
                   ) {
                    syncTaskList{
                        txType
                        id
                        source
                        target
                        cutDate
                        startDate
                        endDate
                        offset
                        syncNote
                        syncStatus
                        entities
                    }
                    pageSize
                    pageNumber
                    total
                }
            }
        """
        variables = {
            "txType": "order",
            "source": "ns",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)


    @unittest.skip("demonstrating skipping")
    def test_graphql_cut_date(self):
        query = """
            query($txType: String!, $source: String!, $target: String!) {
                cutDate(txType: $txType, source: $source, target: $target) {
                    cutDate
                    offset
                }
            }
        """
        variables = {
            "txType": "opportunity",
            "source": "ns",
            "target": "hubspot",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_insert_product_metadata(self):
        query = """
            mutation insertProductMetadata(
                    $targetSource: String!,
                    $column: String!,
                    $metadata: JSON!
                ) {
                insertProductMetadata(
                    targetSource: $targetSource,
                    column: $column,
                    metadata: $metadata
                ) {
                    productMetadata{
                        targetSource
                        column
                        metadata
                        createdAt
                        updatedAt
                    }
                }
            }
        """
        variables = {
            "targetSource": "NS-MAGE2-SANDBOX",
            "column": "applications",
            "metadata": {
                "dest": "applications",
                "funct": "[] if src.get('applications') is None or src.get('applications') == '' else [src['application_list'][i] for i in src.get('applications').strip('[').strip(']').split(',') if i in src['application_list'].keys()] + ['Applications']",
                "schema": {"applications": {"required": False, "type": "list"}},
                "src": [
                    {"key": "applications", "label": "applications"},
                    {
                        "default": {
                            "137": "Animal Nutrition",
                            "744": "Sports Nutrition",
                            "745": "Weight Management",
                            "746": "Immune Support",
                            "747": "Joint Support",
                            "748": "Digestive Support",
                            "749": "Hair, Skin, & Nails",
                            "756": "Cognitive Support",
                            "766": "Food and Beverage",
                            "903": "Cosmetics",
                        },
                        "label": "application_list",
                    },
                ],
            },
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_update_product_metadata(self):
        query = """
            mutation updateProductMetadata(
                    $targetSource: String!,
                    $column: String!,
                    $metadata: JSON!
                ) {
                updateProductMetadata(
                    targetSource: $targetSource,
                    column: $column,
                    metadata: $metadata
                ) {
                    productMetadata{
                        targetSource
                        column
                        metadata
                        createdAt
                        updatedAt
                    }
                }
            }
        """
        variables = {
            "targetSource": "NS-MAGE2-SANDBOX",
            "column": "applications",
            "metadata": {
                "dest": "applications",
                "funct": "[] if src.get('applications') is None or src.get('applications') == '' else [src['application_list'][i] for i in src.get('applications').strip('[').strip(']').split(',') if i in src['application_list'].keys()] + ['Applications']",
                "schema": {"applications": {"required": False, "type": "list"}},
                "src": [
                    {"key": "applications", "label": "applications"},
                    {
                        "default": {
                            "137": "Animal Nutrition",
                            "744": "Sports Nutrition",
                            "745": "Weight Management",
                            "746": "Immune Support",
                            "747": "Joint Support",
                            "748": "Digestive Support",
                            "749": "Hair, Skin, & Nails",
                            "756": "Cognitive Support",
                            "766": "Food and Beverage",
                            "903": "Cosmetics",
                        },
                        "label": "application_list",
                    },
                ],
            },
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_delete_product_metadata(self):
        query = """
            mutation deleteProductMetadata(
                    $targetSource: String!,
                    $column: String!
                ) {
                deleteProductMetadata(
                    targetSource: $targetSource,
                    column: $column
                ) {
                    status
                }
            }
        """
        variables = {"targetSource": "NS-MAGE2-SANDBOX", "column": "applications"}

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_product_metadatas(self):
        query = """
            query($targetSource: String!) {
                productMetadatas(targetSource: $targetSource) {
                    targetSource
                    column
                    metadata
                    createdAt
                    updatedAt
                }
            }
        """
        variables = {
            "targetSource": "NS-MAGE2-SANDBOX",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_put_messages(self):
        query = """
            mutation putMessages(
                $txType: String!,
                $source: String!,
                $target: String!,
                $messages: [JSON]!
                ) {
                putMessages(
                    txType: $txType,
                    source: $source,
                    target: $target,
                    messages: $messages
                ) {
                    messageGroupId
                }
            }
        """
        variables = {
            "txType": "order",
            "source": "MAGE2SQS-SANDBOX",
            "target": "NS-MAGE2-SANDBOX",
            "messages": [
                {
                    "additional_tc_timestamp": "2022-03-14 17:57:37",
                    "additional_tc_version": "####",
                    "addresses": {
                        "billto": {
                            "firstname": "Bibo",
                            "lastname": "W.",
                            "email": "bibo72@outlook.com",
                            "address": "101 Main St",
                            "city": "Tustin",
                            "region": "CA",
                            "postcode": "92705",
                            "country": "US",
                        },
                        "shipto": {
                            "firstname": "Bibo",
                            "lastname": "W.",
                            "address": "101 Main St",
                            "city": "Tustin",
                            "region": "CA",
                            "postcode": "92705",
                            "country": "US",
                        },
                    },
                    "ns_customer_id": "14142",
                    "class": "Sales-Online",
                    "order_create_date": "2022-03-14 18:00:06",
                    "credit_card_type": "VI",
                    "customer_po_no": "202203141057",
                    "delivery_type": "Regular",
                    "mage2_customer_id": "3142",
                    "order_date": "2022-03-14 17:57:37",
                    "order_id": "2000069787",
                    "order_status": "processing",
                    "order_update_date": "2022-03-14 17:57:37",
                    "firstname": "Bibo",
                    "fob_remarks": "Origin",
                    "freight_terms": "Collect",
                    "hold_reason": "Pending Online Order Approval",
                    "id": "94c24750-a3c0-11ec-a9c4-964b390a1137",
                    "items": [
                        {"price": 9.5, "qty": "25.0000", "sku": "93801-100-10245-11084"}
                    ],
                    "lastname": "W.",
                    "order_type": "Online",
                    "payment_method": "authnetcim",
                    "ship_method": "will_call",
                    "ship_method_internal_id": "15",
                    "shipping_carrier_other": "To be determined by shipper",
                    "shipping_instructions": "Please place at the hall.",
                    "shipping_total": "0.0000",
                    "subsidiary": "GWI",
                    "warehouse": "GWI-Chino",
                }
            ],
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_resolve_data_feed_count(self):
        query = """
            query(
                $source: String!, 
                $updatedAtFrom: DateTime!, 
                $updatedAtTo: DateTime!, 
                $tableName: String!) {
                dataFeedCount(
                    source: $source, 
                    updatedAtFrom: $updatedAtFrom, 
                    updatedAtTo: $updatedAtTo, 
                    tableName: $tableName
                )
            }
        """
        variables = {
            "source": "ns",
            "updatedAtFrom": "2022-03-21T16:06:40+0000",
            "updatedAtTo": "2022-03-23T16:06:40+0000",
            "tableName": "datamart_salesorders",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_resolve_data_feed_entities(self):
        query = """
            query(
                $source: String!, 
                $updatedAtFrom: DateTime, 
                $updatedAtTo: DateTime, 
                $tableName: String!,
                $limit: Int,
                $offset: Int,
                $value: String,
                $key: String!) {
                dataFeedEntities(
                    source: $source, 
                    updatedAtFrom: $updatedAtFrom, 
                    updatedAtTo: $updatedAtTo, 
                    tableName: $tableName,
                    limit: $limit,
                    offset: $offset,
                    value: $value,
                    key: $key) {
                        source
                        id
                        key
                        value
                        data
                        createdAt
                        updatedAt

                }
            }
        """
        variables = {
            "source": "ns",
            "updatedAtFrom": "2022-03-21T16:06:40+0000",
            # "updatedAtTo": "2022-03-23T16:06:40+0000",
            "tableName": "datamart_salesorders",
            # "value": "2000054555",
            "key": "ecom_so",
        }

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_retry_sync_task(self):
        query = """
            mutation retrySyncTask(
                $txType: String!,
                $source: String!,
                $id: String!,
                $funct: String
                ) {
                retrySyncTask(
                    txType: $txType,
                    source: $source,
                    id: $id,
                    funct: $funct
                ) {
                    messageGroupId
                }
            }
        """
        variables = {"txType": "order", "source": "sqs", "id": "256736530604495340"}

        payload = {"query": query, "variables": variables}
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)


if __name__ == "__main__":
    unittest.main()
