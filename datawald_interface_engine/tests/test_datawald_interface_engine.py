#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import logging, sys, unittest, os
from dotenv import load_dotenv
from pathlib import Path

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

document = Path(
    os.path.join(os.path.dirname(__file__), "datawald_interface_engine.graphql")
).read_text()
sys.path.insert(0, "/var/www/projects/datawald_interface_engine")
sys.path.insert(1, "/var/www/projects/dynamodb_connector")
sys.path.insert(2, "/var/www/projects/silvaengine_dynamodb_base")

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
        payload = {
            "query": document,
            "variables": {},
            "operation_name": "ping",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    # @unittest.skip("demonstrating skipping")
    def test_graphql_insert_tx_staging(self):
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
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "insertTxStaging",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_update_tx_staging(self):
        variables = {
            "source": "MAGE2SQS-SANDBOX",
            "txTypeSrcId": "order-2000051509",
            "target": "NS-MAGE2-SANDBOX",
            "tgtId": "xxxx1",
            "txStatus": "S",
            "txNote": "DataWald -> NS-MAGE2",
            "updatedAt": "2022-03-08T13:00:00",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "updateTxStaging",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_delete_tx_staging(self):
        variables = {
            "source": "MAGE2SQS-SANDBOX",
            "txTypeSrcId": "order-2000051509",
            "target": "NS-MAGE2-SANDBOX",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "deleteTxStaging",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_tx_staging(self):
        variables = {
            "source": "MAGE2SQS-SANDBOX",
            "txTypeSrcId": "order-2000051509",
            "target": "NS-MAGE2-SANDBOX",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getTxStaging",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_tx_staging_list(self):
        variables = {
            "source": "MAGE2SQS-SANDBOX",
            "target": "NS-MAGE2-SANDBOX",
            "txType": "order",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getTxStagingList",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_insert_sync_task(self):
        # variables = {
        #     "txType": "order",
        #     "source": "MAGE2SQS-SANDBOX",
        #     "target": "NS-MAGE2-SANDBOX",
        #     "cutDate": "2022-03-08T13:00:00",
        #     "offset": 0,
        #     "entities": [
        #         {
        #             "source": "MAGE2SQS-SANDBOX",
        #             "tx_type_src_id": "order-2000051509",
        #             "target": "NS-MAGE2-SANDBOX",
        #             "created_at": "2022-03-08T13:00:00",
        #             "updated_at": "2022-03-08T13:00:00",
        #         }
        #     ],
        #     "funct": "insert_update_entities_to_target",
        # }
        variables = {
            "txType": "order",
            "id": "14872411042064699886",
            "entities": [
                {
                    "created_at": "2022-05-20T03:27:24+0000",
                    "source": "MAGE2SQS-SANDBOX",
                    "target": "NS-MAGE2-SANDBOX",
                    "tx_note": "datawald -> NS-MAGE2-SANDBOX",
                    "tx_status": "S",
                    "tx_type_src_id": "order-2000051509",
                    "updated_at": "2022-05-20T03:27:42+0000",
                }
            ],
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "insertUpdateSyncTask",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_delete_sync_task(self):
        variables = {"txType": "order", "id": "17414272959944528366"}
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "deleteSyncTask",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_sync_task(self):
        variables = {
            "txType": "order",
            "id": "14872411042064699886",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getSyncTask",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_sync_task_list(self):
        variables = {
            "txType": "order",
            "source": "ns",
            "endDateFrom": "2023-05-21T16:06:40+0000",
            # "endDateTo": "2022-03-24T16:06:40+0000",
            "syncStatuses": ["Completed", "Fail", "Processing"],
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getSyncTaskList",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_cut_date(self):
        variables = {
            "txType": "product",
            "source": "ns",
            "target": "mage2",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getCutDate",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_insert_update_product_metadata(self):
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
        # variables = {
        #     "targetSource": "NS-MAGE2-SANDBOX",
        #     "column": "applications",
        #     "metadata": {
        #         "dest": "applications",
        #         "funct": "[] if src.get('applications') is None or src.get('applications') == '' else [src['application_list'][i] for i in src.get('applications').strip('[').strip(']').split(',') if i in src['application_list'].keys()] + ['Applications']",
        #         "schema": {"applications": {"required": False, "type": "list"}},
        #         "src": [
        #             {"key": "applications", "label": "applications"},
        #             {
        #                 "default": {
        #                     "137": "Animal Nutrition",
        #                     "744": "Sports Nutrition",
        #                     "745": "Weight Management",
        #                     "746": "Immune Support",
        #                     "747": "Joint Support",
        #                     "748": "Digestive Support",
        #                     "749": "Hair, Skin, & Nails",
        #                     "756": "Cognitive Support",
        #                     "766": "Food and Beverage",
        #                     "903": "Cosmetics",
        #                 },
        #                 "label": "application_list",
        #             },
        #         ],
        #     },
        # }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "insertUpdateProductMetadata",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_product_metadata(self):
        variables = {"targetSource": "NS-MAGE2-SANDBOX", "column": "applications"}
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getProductMetadata",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_delete_product_metadata(self):
        variables = {"targetSource": "NS-MAGE2-SANDBOX", "column": "applications"}
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "deleteProductMetadata",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_product_metadata_list(self):
        variables = {
            "targetSource": "mage2-ss3",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getProductMetadataList",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_put_messages(self):
        variables = {
            "target": "ns",
            "source": "sqs",
            "txType": "order",
            "messages": [
                {
                    "order_date": "2022-11-15 01:15:12",
                    "order_create_date": "2022-11-15 01:15:12",
                    "created_at": "2022-11-15T01:15:12+00:00",
                    "updated_at": "2022-11-15T01:15:24+00:00",
                    "order_id": "2000054714Y",
                    "order_status": "new",
                    "ship_method": "ups_ground",
                    "shipping_total": "31.8700",
                    "firstname": "Bibo",
                    "lastname": "W.",
                    "payment_method": "checkmo",
                    "ns_customer_id": "14142",
                    "order_type": "Non-Inventory Sample",
                    "shipping_carrier_other": "To be determined by shipper",
                    "freight_terms": "Collect",
                    "fob_remarks": "Origin",
                    "delivery_type": "Regular",
                    "hold_reason": "Pending Outside US Shipping Approval",
                    "ship_method_internal_id": "15",
                    "class": "Sales-Online",
                    "subsidiary": "GWI",
                    "shipping_instructions": "test po",
                    "warehouse": "GWI-Chino",
                    "customer_po_no": "canada po",
                    "items": [
                        {
                            "sku": "10002-101-10485-10763",
                            "qty": "20.0000",
                            "lot_no_locs": [{"lot_no": "12344", "deduct_qty": "20"}],
                        }
                    ],
                    "addresses": {
                        "billto": {
                            "firstname": "Bibo",
                            "lastname": "W.",
                            "company": "Believe Supplements",
                            "email": "bibo72@outlook.com",
                            "address": "719 boulevard industriel suite 104",
                            "city": "Blainville",
                            "region": "Quebec",
                            "postcode": "J7C3V3",
                            "country": "CA",
                        },
                        "shipto": {
                            "firstname": "Bibo",
                            "lastname": "W.",
                            "company": "Believe Supplements",
                            "email": "bibo72@outlook.com",
                            "address": "719 boulevard industriel suite 104",
                            "city": "Blainville",
                            "region": "Quebec",
                            "postcode": "J7C3V3",
                            "country": "CA",
                        },
                    },
                }
            ],
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "putMessages",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_resolve_data_feed_count(self):
        variables = {
            "source": "ns",
            "updatedAtFrom": "2022-03-21T16:06:40+0000",
            "updatedAtTo": "2023-03-23T16:06:40+0000",
            "tableName": "datamart_salesorders_v2",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getDataFeedCount",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_resolve_data_feed_entities(self):
        variables = {
            "source": "ns",
            "updatedAtFrom": "2022-03-21T16:06:40+0000",
            # "updatedAtTo": "2022-03-23T16:06:40+0000",
            "tableName": "datamart_salesorders_v2",
            # "value": "2000054555",
            "key": "ecom_so",
        }
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "getDataFeedEntities",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)

    @unittest.skip("demonstrating skipping")
    def test_graphql_retry_sync_task(self):
        variables = {"txType": "order", "source": "sqs", "id": "15340979593176158701"}
        payload = {
            "query": document,
            "variables": variables,
            "operation_name": "retrySyncTask",
        }
        response = self.datawald_interface_engine.datawald_interface_graphql(**payload)
        logger.info(response)


if __name__ == "__main__":
    unittest.main()
