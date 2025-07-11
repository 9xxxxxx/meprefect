import requests
import pandas as pd
import schedule
import time
from sqlalchemy import create_engine
from typing import Dict, List, Optional
import logging
import re

from xlwings import sheets

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeishuSpreadsheetManager:
    def __init__(self, spreadsheet_tokens: List[str], db_uri: str = None):
        """
        飞书表格管理器
        :param spreadsheet_tokens: 电子表格token列表
        :param db_uri: 数据库连接URI (示例: sqlite:///data.db)
        """
        self.app_id = "cli_a7e1e09d72fe500e"
        self.app_secret = "sIsx3hT20I4oQ2lrq0ydAf04LTmaKxP7"
        self.spreadsheet_tokens = spreadsheet_tokens
        self.session = requests.Session()
        self.access_token = self._get_access_token()
        self.db_engine = create_engine(db_uri) if db_uri else None
        
        # 配置请求头
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        })

    def _get_access_token(self, retries: int = 3) -> str:
        """获取租户访问令牌（带重试机制）"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        
        for attempt in range(retries):
            try:
                response = self.session.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
                if data.get("code") == 0:
                    return data["tenant_access_token"]
                logger.error(f"获取token失败: {data.get('msg')}")
            except Exception as e:
                logger.warning(f"获取token重试中 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(2**attempt)
        raise Exception("无法获取访问令牌")

    def _get_spreadsheet_info(self, spreadsheet_token: str) -> Dict:
        """获取电子表格元数据（修正后的正确API）"""
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0:
                return data["data"]["spreadsheet"]
            raise Exception(f"API错误: {data.get('msg')}")
        except Exception as e:
            logger.error(f"获取表格元数据失败: {str(e)}")
            return {"title": f"Spreadsheet_{spreadsheet_token[-6:]}"}

    def _list_sheets(self, spreadsheet_token: str) -> List[Dict]:
        """获取工作表列表（正确API）"""
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0:
                return data["data"]["sheets"]
            raise Exception(f"API错误: {data.get('msg')}")
        except Exception as e:
            logger.error(f"获取工作表列表失败: {str(e)}")
            return []

    def _read_sheet_data(self, spreadsheet_token: str, sheet_id: str, range: str) -> pd.DataFrame:
        """读取工作表数据"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!{range}"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0:
                values = data["data"]["valueRange"].get("values", [])
                if len(values) > 1:
                    return pd.DataFrame(values[1:], columns=values[0])
                return pd.DataFrame()
            raise Exception(f"API错误: {data.get('msg')}")
        except Exception as e:
            logger.error(f"读取数据失败: {str(e)}")
            return pd.DataFrame()

    def _get_valid_table_name(self, name: str) -> str:
        """生成合法表名"""
        # 替换非法字符
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # 确保以字母开头
        if not clean_name[0].isalpha():
            clean_name = 'tbl_' + clean_name
        return clean_name.lower()[:63]

    def read_all_data(self, range: str = "A:BT") -> Dict[str, pd.DataFrame]:
        """
        读取所有电子表格数据
        :return: {表格名_工作表名: DataFrame}
        """
        all_data = {}
        
        for token in self.spreadsheet_tokens:
            try:
                # 获取表格信息
                spreadsheet_info = self._get_spreadsheet_info(token)
                spreadsheet_name = spreadsheet_info.get("title", f"spreadsheet_{token[-6:]}")
                
                # 获取工作表列表
                sheets = self._list_sheets(token)
                # sheet_ids = list(map(lambda x:{x['title']:x['sheet_id']},sheets))
                
                logger.info(f"在表格 {spreadsheet_name} 中找到 {len(sheets)} 个工作表")
                
                for sheet in sheets:
                    sheet_name = sheet["title"]
                    combined_name = f"{spreadsheet_name}_{sheet_name}"
                    
                    # 读取数据
                    df = self._read_sheet_data(token, sheet["sheet_id"], range)
                    if not df.empty:
                        # 添加元数据
                        # df['_spreadsheet_token'] = token
                        # df['_sheet_id'] = sheet["sheet_id"]
                        df = df.drop(columns=[None])
                        all_data[combined_name] = df
                        logger.info(f"成功读取 {combined_name} {df.shape[0]}行,{df.shape[1]} 列)")
                    else:
                        logger.warning(f"工作表 {combined_name} 无数据")
                        
            except Exception as e:
                logger.error(f"处理表格 {token} 失败: {str(e)}")
                continue
                
        return all_data

    def save_to_database(self, data: Dict[str, pd.DataFrame], if_exists: str = 'replace'):
        """存储到数据库"""
        if not self.db_engine:
            raise Exception("未配置数据库连接")
        
        try:
            for raw_name, df in data.items():
                table_name = self._get_valid_table_name(raw_name)
                df.to_sql(
                    name=table_name,
                    con=self.db_engine,
                    index=False,
                    if_exists=if_exists
                )
                logger.info(f"已保存 {table_name} {df.shape[0]}行,{df.shape[1]} 列)")
        except Exception as e:
            logger.error(f"数据库保存失败: {str(e)}")
            raise

    def auto_sync(self, interval: int = 3600):
        """启动自动同步"""
        schedule.every(interval).seconds.do(self._sync_job)
        logger.info(f"自动同步已启动，间隔 {interval} 秒")
        
        while True:
            schedule.run_pending()
            time.sleep(1)

    def _sync_job(self):
        """执行同步任务"""
        try:
            data = self.read_all_data()
            if self.db_engine:
                self.save_to_database(data)
            logger.info("同步任务完成")
        except Exception as e:
            logger.error(f"同步失败: {str(e)}")

if __name__ == '__main__':
    # 配置示例
    SPREADSHEET_TOKENS = ['DiWssnFVLhcymFtJWnlctwVMnPf', 'RYcuszk7thaneqtqMioc3hQonxh', 'LiScsSTzChb1B7tX2OUcGuB2nyc']
    DB_URI = 'sqlite:///feishu_data.db'  # SQLite示例
    
    # 初始化管理器
    manager = FeishuSpreadsheetManager(
        spreadsheet_tokens=SPREADSHEET_TOKENS,
        db_uri=DB_URI
    )
    
    data = manager.read_all_data()
    data   
    # 执行一次同步
    # try:
    #     data = manager.read_all_data()
    #     manager.save_to_database(data)
    # except Exception as e:
    #     logger.error(f"初始化同步失败: {str(e)}")
    
    # 启动定时同步（每小时一次）
    # manager.auto_sync(interval=3600)
