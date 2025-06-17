# -*- coding: utf-8 -*-
# @Time : 2025/6/16 14:04
# @Author : Garry-Host
# @FileName: send_file_to_xy
from prefect import flow, task
import pymysql
import requests
import datetime

from sqlalchemy.testing.suite.test_reflection import users

from utils.asbot import AsBot

@task
def query_rma_count():
    """查询昨日吹风机退换货数量"""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.date.today().strftime('%Y-%m-%d')

    conn = pymysql.connect(
        host="172.16.100.133",
        port=3306,
        user="LJH",
        password="ljhyds666",
        database="demo",
        charset="utf8mb4"
    )
    with conn.cursor() as cursor:
        sql = f"""
            SELECT COUNT(new_rma_id) AS n 
            FROM maintenance_detail_ruiyun 
            WHERE DATE_FORMAT(new_checkon,'%Y-%m-%d %H:%i:%s') 
                  BETWEEN '{yesterday}' AND '{today}'
              AND applytype IN ('退货','换货')
              AND productmodel_name = '产成品-吹风机'
              AND new_returnstatus <> '异常'
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        count = result[0] if result else 0
    conn.close()
    return count

@flow(name="发送退换货整机数据给夏颖")
def feishu_notify_flow():
    count = query_rma_count()
    asbot = AsBot()
    """向指定用户发送飞书文本消息"""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    user_id = '11000245'
    text= f"{yesterday} 瑞云吹风机退换货数量 {count} 台"
    asbot.send_text_to_person(text,user_id)
    print(f"{datetime.datetime.now()} 已发送: {count} 台")

if __name__ == "__main__":
    feishu_notify_flow()


