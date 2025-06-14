# -*- coding: utf-8 -*-
# @Time : 2025/5/29 15:25
# @Author : Garry-Host
# @FileName: main
import json

from prefect.blocks.notifications import CustomWebhookNotificationBlock

custom_webhook_block = CustomWebhookNotificationBlock.load("feishu")

payload = {
  "msg_type": "text",
  "content": {
    "text": "<at user_id=\"ou_xxx\">Tom</at> 新更新提醒"
  }
}

custom_webhook_block.notify(json.dumps(payload))

