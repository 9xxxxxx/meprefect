import requests
from prefect import get_run_logger
from requests_toolbelt import MultipartEncoder
from config import asbot_config
import json


class AsBot:
    def __init__(self,chat_name):
        self.app_id = asbot_config.APP_ID
        self.app_secret = asbot_config.APP_SECRET
        self.chat_name = chat_name
        self.token = self.get_token()
        self.chat_id = self.get_chat_id()


    def get_token(self):
        logger = get_run_logger()
        url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
        data = {
        "Content-Type": "application/json; charset=utf-8",
        "app_id": self.app_id,
        "app_secret": self.app_secret
        }
        response = requests.request("POST", url, data=data,timeout=30)
        logger.info("成功获取bearer token")
        return json.loads(response.text)['tenant_access_token']

    def get_chat_id(self):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/chats?page_size=20"
        payload = ''

        Authorization = 'Bearer ' + self.token
        headers = {
        'Authorization': Authorization
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        items = json.loads(response.text)['data']['items']
        for item in items:
            if item['name'] == self.chat_name:
                logger.info(f"成功获取{self.chat_name}的chat_id")
                return item['chat_id']

    def get_filekey(self, file_path, file_name, file_type, type_file):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/files"
        form = {'file_type': file_type,
                'file_name': file_name,
                'file': (file_name, open(file_path, 'rb'), type_file)}
        multi_form = MultipartEncoder(form)
        Authorization = 'Bearer ' + self.token
        headers = {'Authorization': Authorization, 'Content-Type': multi_form.content_type}
        response = requests.request("POST", url, headers=headers, data=multi_form)
        logger.info(f"上传文件获取file_key")
        if response.status_code == 200:
            logger.info("获取file_key成功")
            return json.loads(response.content)['data']['file_key']
        else:
            return '好像没拿到file key'

    def get_imagekey(self,path):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/images"
        form = {'image_type': 'message',
                'image': (open(path, 'rb'))}  # 需要替换具体的path
        multi_form = MultipartEncoder(form)
        headers = {'Authorization': f'Bearer {self.token}', 'Content-Type': multi_form.content_type}
        response = requests.request("POST", url, headers=headers, data=multi_form)
        decoded_string = response.content.decode('utf-8')
        image_key = json.loads(decoded_string)['data']['image_key']
        logger.info("上传图片获取image_key")
        if response.status_code == 200:
            logger.info("获取image_key成功")
            return image_key
        else:
            return '好像没拿到image key'

    def sendimage(self,image_path):
        logger = get_run_logger()
        msg = self.get_imagekey(image_path)
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type":"chat_id"}
        msgContent = {
            "image_key": msg,
        }

        req = {
            "receive_id": self.chat_id, # chat id
            "msg_type": "image",
            "content": json.dumps(msgContent)
        }
        payload = json.dumps(req)
        headers = {
            'Authorization': f'Bearer {self.token}', # your access token
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, data=payload)
        logger.info("正在发送图片~")
        if response.status_code == 200:
            logger.info("发送图片成功！")

    def sendfile(self, file_type, file_name, file_path,type_file):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}

        file_key = self.get_filekey(file_path, file_name, file_type, type_file)

        msgContent = {
            "file_key": file_key,
        }

        req = {
            "receive_id": self.chat_id,
            "msg_type": "file",
            "content": json.dumps(msgContent)
        }
        payload = json.dumps(req)
        Authorization = 'Bearer ' + self.token
        headers = {
            'Authorization': Authorization,
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, data=payload)
        logger.info("正在发送文件~")
        if response.status_code == 200:
            logger.info("发送文件成功")

    def send_file_to_陶健宏(self, file_type, file_name, file_path,type_file):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "user_id"}

        file_key = self.get_filekey(file_path, file_name, file_type, type_file)

        msgContent = {
            "file_key": file_key,
        }

        req = {
            "receive_id": "6e4997ed",
            "msg_type": "file",
            "content": json.dumps(msgContent)
        }
        payload = json.dumps(req)
        Authorization = 'Bearer ' + self.token
        headers = {
            'Authorization': Authorization,
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, data=payload)
        logger.info("正在发送文件到陶健宏~")
        if response.status_code == 200:
            logger.info("发送文件成功")


    def send_text_to_group(self,msg,):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        msgContent = {
            'text': msg
        }

        payload = {
            "receive_id": self.chat_id,  # chat id
            "msg_type": 'text',
            "content": json.dumps(msgContent)
        }
        headers = {
            'Authorization': f'Bearer {self.token}',  # your access token
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f'{response.status_code}-发送成功')  # Print Response
        else:
            logger.info('好像不行哦~')

    def send_text_to_person(self,msg,user_id):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "user_id"}
        msgContent = {
            "text": msg
        }

        payload = {
            "receive_id": user_id,  # user id
            "msg_type": "text",
            "content": json.dumps(msgContent)
        }

        headers = {
            'Authorization': f'Bearer {self.token}',  # your access token
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f'{response.status_code}-发送成功')  # Print Response
        else:
            logger.info('好像不行哦~')

    def send_post_to_group(self,msg):
        """

        :param msg:
        :return:
        """
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        payload = {
            "receive_id": self.chat_id,  # chat id
            "msg_type": "post",
            "content": json.dumps(msg)
        }
        print(payload)

        headers = {
            'Authorization': f'Bearer {self.token}',  # your access token
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f'{response.status_code}-发送成功')  # Print Response
        else:
            logger.info('好像不行哦~')

    def send_post_to_person(self,msg,user_id):
        # 消息的构造只需要一个msg_type指定post参数，content参数后面接zh-cn之后的内容,
        """
        :param msg: 传入zh-cn之后的内容
        :param user_id:
        :return:
        """
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "user_id"}
        payload = {
            "receive_id": user_id,  # chat id
            "msg_type": "post",
            "content": json.dumps(msg)
        }
        headers = {
            'Authorization': f'Bearer {self.token}',  # your access token
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f'{response.status_code}-发送成功')  # Print Response
        else:
            logger.info('好像不行哦~')


    def send_card_to_person(self,msg,user_id):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "user_id"}
        payload = {
            "receive_id": user_id,  # chat id
            "msg_type": "interactive",
            "content": json.dumps(msg)
        }
        headers = {
            'Authorization': f'Bearer {self.token}',  # your access token
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f'{response.status_code}-发送成功')  # Print Response
        else:
            logger.info('好像不行哦~')

    def send_card_to_group(self,msg):
        logger = get_run_logger()
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        payload = {
            "receive_id": self.chat_id,  # chat id
            "msg_type": "interactive",
            "content": msg
        }
        headers = {
            'Authorization': f'Bearer {self.token}',  # your access token
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, params=params, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f'{response.status_code}-发送成功')  # Print Response
        else:
            logger.info('好像不行哦~')
            print(response.text)


#消息示例
# payload = json.dumps({
#   "content": "{\"type\":\"template\",\"data\":{\"template_id\":\"AAqBc0EeBjtyz\",\"template_version_name\":\"1.0.2\",\"template_variable\":{\"title\":\"哇，真的是你啊\"}}}"
# })
# headers = {
#   'Authorization': 'Bearer t-g1042kjf6KMSYE44K3MX2FPROJCU6OLEMWCYLQ7Y',
#   'Content-Type': 'application/json'
# }

