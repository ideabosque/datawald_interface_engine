# DataWald Interface Engine
Serves as the central engine that orchestrates the entire data management framework.

**Configuration Setup**

Insert the following records into the `se-configdata` DynamoDB table:

```bash
{
    "setting_id": "datawald_interface_engine",
    "variable": "default_cut_date",
    "value": "2024-05-24T02:21:00+00:00"
},
{
    "setting_id": "datawald_interface_engine",
    "variable": "input_queue_name",
    "value": "datawald_input_queue.fifo"
},
{
    "setting_id": "datawald_interface_engine",
    "variable": "max_entities_in_message_body",
    "value": "200"
},
{
    "setting_id": "datawald_interface_engine",
    "variable": "sync_task_notification",
    "value": {
    "<endpoint_id>": {
        "<data_type>": "<async_function>"
    }
  }
},
{
    "setting_id": "datawald_interface_engine",
    "variable": "task_queue_name",
    "value": "silvaengine_task_queue.fifo"
}
```

**Configuration Details:**

- **default_cut_date**: The default cut-off date for data synchronization.
- **input_queue_name**: Specifies the SQS queue designated for `SQSAgency` and `SQSConnector` to receive incoming messages.
- **max_entities_in_message_body**: Defines the maximum number of entities allowed in the message body when sending data via `task_queue`.
- **sync_task_notification**: Configures notifications to trigger an asynchronous function based on `endpoint_id` and `data_type` when a synchronization task is completed or fails.
- **task_queue_name**: Specifies the SQS queue used by SilvaEngine for dispatching tasks in asynchronous workflows.