# -*- coding: utf-8 -*-
# @Time : 2025/7/10 9:47
# @Author : Garry-Host
# @FileName: send_hour_bv
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import pandas as pd
from utils import asbot
conn = create_engine("mysql+pymysql://wty:laifen03@localhost:3306/demo")
query_fj = """
    SELECT DATE_ADD(DATE(`检测时间`), INTERVAL HOUR(`检测时间`) HOUR) AS `检测时间`, count(`单号`) AS `数量`
    FROM maintenance_ruiyun_realtime
    WHERE `业务类型` = '分拣'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`检测时间`), INTERVAL HOUR(`检测时间`) HOUR)
    ORDER BY 检测时间 DESC
    LIMIT 1;"""

query_wx = """
    SELECT DATE_ADD(DATE(`维修完成时间`), INTERVAL HOUR(`维修完成时间`) HOUR) AS `维修完成时间`, count(`单号`) AS `数量`
    FROM demo.maintenance_ruiyun_realtime
    WHERE `业务类型` = '维修'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`维修完成时间`), INTERVAL HOUR(`维修完成时间`) HOUR)
    ORDER BY 维修完成时间 DESC
    LIMIT 1;"""

query_zj = """
    SELECT DATE_ADD(DATE(`质检完成时间`), INTERVAL HOUR(`质检完成时间`) HOUR) AS `质检完成时间`, count(`单号`) AS `数量`
    FROM demo.maintenance_ruiyun_realtime
    WHERE `业务类型` = '质检'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`质检完成时间`), INTERVAL HOUR(`质检完成时间`) HOUR)
    ORDER BY 质检完成时间 DESC
    LIMIT 1;"""

query_fh = """
    SELECT DATE_ADD(DATE(`发货时间`), INTERVAL HOUR(`发货时间`) HOUR) AS `发货时间`, count(`单号`) AS `数量`
    FROM demo.maintenance_ruiyun_realtime
    WHERE `业务类型` = '发货'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`发货时间`), INTERVAL HOUR(`发货时间`) HOUR)
    ORDER BY 发货时间 DESC
    LIMIT 1;"""

@flow(name='发送寄修全链路业务量')
def getdata():
    fj = pd.read_sql(query_fj, conn)
    wx = pd.read_sql(query_wx, conn)
    zj = pd.read_sql(query_zj, conn)
    fh = pd.read_sql(query_fh, conn)
    now = datetime.now()
    this_hour = now.replace(minute=0, second=0, microsecond=0)
    last_hour = this_hour - timedelta(hours=1)

    bot = asbot.AsBot('人机')

    msg =  {
        "zh_cn": {
            "title": f"寄修全链路业务量播报,{last_hour.strftime('%H:%M')}-{this_hour.strftime('%H:%M')}",
            "content": [
                [
                    {
                        "tag": "text",
                        "text": f"分拣量：{int(fj.iloc[0,1])} 台\n",
                    }
                ],
                [
                    {
                        "tag": "text",
                        "text": f"维修量：{int(wx.iloc[0,1])} 台\n",
                    },
                ],
                [
                    {
                        "tag": "text",
                        "text": f"质检量：{int(zj.iloc[0,1])} 台\n",
                    },
                ],
                [
                    {
                        "tag": "text",
                        "text": f"发货量：{int(fh.iloc[0, 1])} 台\n",
                    },
                ],
                [
                    {
                        "tag": "at",
                        "user_id": "6e4997ed",
                    }
                ]
            ]
        }
    }
    bot.send_post_to_group(msg)

if __name__ == '__main__':
    getdata()
