# -*- coding: utf-8 -*-
# @Time : 2025/7/10 9:47
# @Author : Garry-Host
# @FileName: send_hour_bv
from utils.dataframe_to_image import export_dataframe_to_image_v2
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import pandas as pd
from prefect import task,flow
from utils import asbot
conn = create_engine("mysql+pymysql://wty:laifen03@localhost:3306/demo")


query_qs = """
    SELECT DATE_ADD(DATE(`旧件签收时间`), INTERVAL HOUR(`旧件签收时间`) HOUR) AS `时间`, count(`单号`) AS `数量`
    FROM maintenance_ruiyun_realtime
    WHERE `业务类型` = '签收'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`旧件签收时间`), INTERVAL HOUR(`旧件签收时间`) HOUR)
    ORDER BY 时间 DESC
    LIMIT 1;"""

query_fj = """
    SELECT DATE_ADD(DATE(`检测时间`), INTERVAL HOUR(`检测时间`) HOUR) AS `时间`, count(`单号`) AS `数量`
    FROM maintenance_ruiyun_realtime
    WHERE `业务类型` = '分拣'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`检测时间`), INTERVAL HOUR(`检测时间`) HOUR)
    ORDER BY 时间 DESC
    LIMIT 1;"""

query_wx = """
    SELECT DATE_ADD(DATE(`维修完成时间`), INTERVAL HOUR(`维修完成时间`) HOUR) AS `时间`, count(`单号`) AS `数量`
    FROM demo.maintenance_ruiyun_realtime
    WHERE `业务类型` = '维修'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`维修完成时间`), INTERVAL HOUR(`维修完成时间`) HOUR)
    ORDER BY 时间 DESC
    LIMIT 1;"""

query_zj = """
    SELECT DATE_ADD(DATE(`质检完成时间`), INTERVAL HOUR(`质检完成时间`) HOUR) AS `时间`, count(`单号`) AS `数量`
    FROM demo.maintenance_ruiyun_realtime
    WHERE `业务类型` = '质检'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`质检完成时间`), INTERVAL HOUR(`质检完成时间`) HOUR)
    ORDER BY 时间 DESC
    LIMIT 1;"""

query_fh = """
    SELECT DATE_ADD(DATE(`发货时间`), INTERVAL HOUR(`发货时间`) HOUR) AS `时间`, count(`单号`) AS `数量`
    FROM demo.maintenance_ruiyun_realtime
    WHERE `业务类型` = '发货'
      AND `产品类型` IN ('产成品-吹风机', '产成品-电动牙刷', '产成品-剃须刀')
      AND `申请类别` IN ('寄修/返修')
    GROUP BY DATE_ADD(DATE(`发货时间`), INTERVAL HOUR(`发货时间`) HOUR)
    ORDER BY 时间 DESC
    LIMIT 1;"""

@flow(name='发送寄修全链路业务量')
def getdata():
    qs = pd.read_sql(query_qs, conn)
    qs['业务类型']= '签收'
    fj = pd.read_sql(query_fj, conn)
    fj['业务类型'] = "分拣"
    wx = pd.read_sql(query_wx, conn)
    wx['业务类型'] = "维修"
    zj = pd.read_sql(query_zj, conn)
    zj['业务类型'] = "质检"
    fh = pd.read_sql(query_fh, conn)
    fh['业务类型'] = "发货"
    data = pd.concat([fj, wx, zj, fh])
    data = data.rename(columns={'数量':'完成量'})
    now = datetime.now()
    this_hour = now.replace(minute=0, second=0, microsecond=0)
    last_hour = this_hour - timedelta(hours=1)
    title = f"寄修全链路业务量播报：{last_hour.strftime('%H:%M')}-{this_hour.strftime('%H:%M')}"
    bot = asbot.AsBot('人机')
    new_add = [int(qs.iloc[0,1]),int(fj.iloc[0,1]),int(wx.iloc[0,1]),int(zj.iloc[0,1])]
    # pure_add = [int(qs.iloc[0,1])-int(fj.iloc[0,1]),int(qs.iloc[0,1])-int(fj.iloc[0,1]),int(qs.iloc[0,1])-int(fj.iloc[0,1]),int(qs.iloc[0,1])-int(fj.iloc[0,1])]
    data['新增量'] = new_add
    data['净增长'] = data['新增量'] - data['完成量']
    df = data[['业务类型','完成量','新增量','净增长']]

    msg =  {
        "zh_cn": {
            "title": f"寄修全链路业务量播报：{last_hour.strftime('%H:%M')}-{this_hour.strftime('%H:%M')}",
            "content": [
                [
                    {
                        "tag": "text",
                        "text": f"分拣量：新增{int(qs.iloc[0,1])}台，完成{int(fj.iloc[0,1])}台，净增长{int(qs.iloc[0,1])-int(fj.iloc[0,1])}台\n",
                    }
                ],
                [
                    {
                        "tag": "text",
                        "text": f"维修量：新增{int(fj.iloc[0,1])}台，完成{int(wx.iloc[0,1])}台，净增长{int(qs.iloc[0,1])-int(fj.iloc[0,1])}台\n",
                    },
                ],
                [
                    {
                        "tag": "text",
                        "text": f"质检量：新增{int(wx.iloc[0,1])}台，完成{int(zj.iloc[0,1])}台，净增长{int(qs.iloc[0,1])-int(fj.iloc[0,1])}台\n",
                    },
                ],
                [
                    {
                        "tag": "text",
                        "text": f"发货量：新增{int(zj.iloc[0,1])}台，完成{int(fh.iloc[0, 1])}台，净增长{int(qs.iloc[0,1])-int(fj.iloc[0,1])}台\n",
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
    path=r'E:\Dev\myprefect\data\table_output.png'
    export_dataframe_to_image_v2(df, path, title=title,image_size=(600,400))
    bot.sendimage(path)

if __name__ == '__main__':
    df = getdata()
