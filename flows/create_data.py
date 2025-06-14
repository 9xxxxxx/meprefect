# -*- coding: utf-8 -*-
# @Time : 2025/6/4 16:46
# @Author : Garry-Host
# @FileName: create_data
from prefect import flow, task,get_run_logger
import pymysql
import pandas as pd
from typing import Dict
from utils.asbot import AsBot
from datetime import datetime
# 数据库连接配置
db_config = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "LJH",
    "password": "ljhyds666",
    "database": "demo",
    "charset": "utf8mb4"
}

# DELETE 查询：删除 n_by_hour 表中过去 35 天到 1 天前的数据
delete_query = """
DELETE 
FROM demo.n_by_hour
WHERE ds BETWEEN CURRENT_DATE() - INTERVAL 35 DAY AND CURRENT_DATE() - INTERVAL 1 DAY
"""

# SELECT 查询：复杂 CTE 查询，统计签收、分拣、维修数据
select_query = """
with t AS
(
select new_signedon , new_checkon , laifen_servicecompletetime , laifen_qualityrecordtime , new_deliveriedon ,new_returnstatus , case when applytype in ('换货','退货') then '退换货' else '寄修/返修' end applytype , productmodel_name
from maintenance_detail_ruiyun 
where productmodel_name in ('产成品-吹风机','产成品-电动牙刷')
  and new_status not in ('已取消')
  and new_signedon is not null
  and (date(new_signedon) between current_date()-interval 36 day and current_date()
  or date(new_checkon) between current_date()-interval 36 day and current_date()
  or date(laifen_servicecompletetime) between current_date()-interval 36 day and current_date()
  or date(laifen_qualityrecordtime) between current_date()-interval 36 day and current_date()
  or date(new_deliveriedon) between current_date()-interval 36 day and current_date())
),
dw AS
(
SELECT *
from dt_week
),
-- 签收
t6 AS
(
select * , hour(new_signedon) hour
from t
where new_returnstatus not in ('异常')
),
t6_1 AS
(
select 
  week ,
  date(new_signedon) ds ,
  hour ,
  applytype ,
  productmodel_name ,
  count(new_signedon) n ,
  null t_sum ,
  null n_y ,
  null n_yn ,
  '签收' type ,
  1 rk
from t6 left join dw on date(t6.new_signedon) = dw.ds
where date(new_signedon) between current_date()-interval 35 day and current_date()-interval 1 day
group by week , ds ,hour ,applytype , productmodel_name
),
-- 分拣
t7 AS
(
select * , hour(new_checkon) hour , (UNIX_TIMESTAMP(new_checkon) - UNIX_TIMESTAMP(new_signedon))/3600 ts
from t
where new_checkon is not null
  and new_returnstatus not in ('异常')
),
t7_1 AS
(
select 
  week ,
  date(new_checkon) ds ,
  hour ,
  applytype ,
  productmodel_name ,
  count(new_checkon) n ,
  sum(ts) t_sum ,
  sum(case when (ts < 4 and applytype = '退换货') or (ts < 8 and applytype = '寄修/返修') then 1 else 0 end) n_y ,
  sum(case when ts between 0 and 40 then 1 else 0 end) n_yn ,
  '分拣' type ,
  2 rk
from t7 left join dw on date(t7.new_checkon) = dw.ds
where date(new_checkon) between current_date()-interval 35 day and current_date()-interval 1 day
group by week , ds ,hour ,applytype ,productmodel_name
),
-- 维修
t1 AS
(
select * , hour(laifen_servicecompletetime) hour , (UNIX_TIMESTAMP(laifen_servicecompletetime) - UNIX_TIMESTAMP(new_checkon))/3600 ts
from t
where laifen_servicecompletetime is not null and new_returnstatus not in ('异常','一检异常')
),
t1_1 AS
(
select 
  week ,
  date(laifen_servicecompletetime) ds ,
  hour ,
  applytype ,
  productmodel_name ,
  count(laifen_servicecompletetime) n ,
  sum(ts) t_sum ,
  sum(case when ts < 36 then 1 else 0 end) n_y ,
  sum(case when ts between 0 and 360 then 1 else 0 end) n_yn ,
  '维修' type ,
  3 rk
from t1 left join dw on date(t1.laifen_servicecompletetime) = dw.ds
where date(laifen_servicecompletetime) between current_date()-interval 35 day and current_date()-interval 1 day
group by week , ds ,hour ,applytype ,productmodel_name
),
-- 质检
t2 AS
(
select * , hour(laifen_qualityrecordtime) hour , (UNIX_TIMESTAMP(laifen_qualityrecordtime) - UNIX_TIMESTAMP(laifen_servicecompletetime))/3600 ts
from t
where laifen_qualityrecordtime is not null and new_returnstatus not in ('异常','一检异常')
),
t2_1 AS
(
select 
  week ,
  date(laifen_qualityrecordtime) ds ,
  hour ,
  applytype ,
  productmodel_name ,
  count(laifen_qualityrecordtime) n ,
  sum(ts) t_sum ,
  sum(case when ts < 18 then 1 else 0 end) n_y ,
  sum(case when ts between 0 and 180 then 1 else 0 end) n_yn ,
  '质检' type ,
  4 rk
from t2 left join dw on date(t2.laifen_qualityrecordtime) = dw.ds
where date(laifen_qualityrecordtime) between current_date()-interval 35 day and current_date()-interval 1 day
group by week , ds ,hour ,applytype ,productmodel_name
),
-- 发货
t3 AS
(
select * , hour(new_deliveriedon) hour , (UNIX_TIMESTAMP(new_deliveriedon) - UNIX_TIMESTAMP(laifen_qualityrecordtime))/3600 ts
from t
where new_deliveriedon is not null and new_returnstatus not in ('异常','一检异常')
),
t3_1 AS
(
select 
  week ,
  date(new_deliveriedon) ds ,
  hour ,
  applytype ,
  productmodel_name ,
  count(new_deliveriedon) n ,
  sum(ts) t_sum ,
  sum(case when ts < 10 then 1 else 0 end) n_y ,
  sum(case when ts between 0 and 100 then 1 else 0 end) n_yn ,
  '发货' type ,
  5 rk
from t3 left join dw on date(t3.new_deliveriedon) = dw.ds
where date(new_deliveriedon) between current_date()-interval 35 day and current_date()-interval 1 day
group by week , ds ,hour ,applytype ,productmodel_name
),
-- 寄修
t4 AS
(
select * , hour(new_deliveriedon) hour , (UNIX_TIMESTAMP(new_deliveriedon) - UNIX_TIMESTAMP(new_checkon))/3600 ts
from t
where new_deliveriedon is not null and new_returnstatus not in ('异常','一检异常')
),
t4_1 AS
(
select 
  week ,
  date(new_deliveriedon) ds ,
  hour ,
  applytype ,
  productmodel_name ,
  count(new_deliveriedon) n ,
  sum(ts) t_sum ,
  sum(case when ts < 64 then 1 else 0 end) n_y ,
  sum(case when ts between 0 and 640 then 1 else 0 end) n_yn ,
  '寄修_64' type ,
  6 rk
from t4 left join dw on date(t4.new_deliveriedon) = dw.ds
where date(new_deliveriedon) between current_date()-interval 35 day and current_date()-interval 1 day
group by week , ds ,hour ,applytype ,productmodel_name
),
-- 寄修_签收开始
t5 AS
(
select * , hour(new_deliveriedon) hour , (UNIX_TIMESTAMP(new_deliveriedon) - UNIX_TIMESTAMP(new_signedon))/3600 ts
from t
where new_deliveriedon is not null and new_returnstatus not in ('异常','一检异常')
),
t5_1 AS
(
select 
  week ,
  date(new_deliveriedon) ds ,
  hour ,
  applytype ,
  productmodel_name ,
  count(new_deliveriedon) n ,
  sum(ts) t_sum ,
  sum(case when ts < 72 then 1 else 0 end) n_y ,
  sum(case when ts between 0 and 720 then 1 else 0 end) n_yn ,
  '寄修_72' type ,
  7 rk
from t5 left join dw on date(t5.new_deliveriedon) = dw.ds
where date(new_deliveriedon) between current_date()-interval 35 day and current_date()-interval 1 day
group by week , ds ,hour ,applytype ,productmodel_name
)
select *
from t6_1
UNION
select *
from t7_1
UNION
select *
from t1_1
UNION
select *
from t2_1
UNION
select *
from t3_1
UNION
select *
from t4_1
UNION
select *
from t5_1
"""


# Task 1: 执行 DELETE 查询
@task(name="删除 n_by_hour 数据")
def delete_n_by_hour_data(db_config: Dict[str, str]) -> int:
    """
    删除 demo.n_by_hour 表中过去 35 天到 1 天前的数据。
    返回受影响的行数。
    """
    try:

        logger = get_run_logger()
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(delete_query)
        conn.commit()
        row_count = cursor.rowcount
        logger.info(f"已删除 {row_count} 行数据从 n_by_hour 表")
        return row_count
    except pymysql.MySQLError as e:
        logger.info(f"删除数据时出错: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Task 2: 执行 SELECT 查询并插入到 n_by_hour 表
@task(name="查询并插入统计数据")
def query_maintenance_data(db_config: Dict[str, str]) -> pd.DataFrame:
    """
    执行复杂的 CTE 查询，统计签收、分拣、维修数据，并将结果插入到 n_by_hour 表。
    返回查询结果作为 pandas DataFrame。
    """
    try:
        logger = get_run_logger()
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # 使用 DictCursor 返回字典格式结果

        # 执行查询
        cursor.execute(select_query)
        results = cursor.fetchall()
        df = pd.DataFrame(results)
        logger.info(f"查询到 {len(df)} 行数据")

        if not df.empty:
            # 准备插入查询
            insert_query = """
            INSERT INTO demo.n_by_hour (
                week, ds, hour, applytype, productmodel_name, n, t_sum, n_y, n_yn, type, rk
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # 转换 DataFrame 为插入所需的元组列表
            values = [
                (
                    row['week'],
                    row['ds'],
                    row['hour'],
                    row['applytype'],
                    row['productmodel_name'],
                    row['n'],
                    row['t_sum'] if pd.notnull(row['t_sum']) else None,
                    row['n_y'] if pd.notnull(row['n_y']) else None,
                    row['n_yn'] if pd.notnull(row['n_yn']) else None,
                    row['type'],
                    row['rk']
                )
                for _, row in df.iterrows()
            ]

            # 批量插入数据
            cursor.executemany(insert_query, values)
            conn.commit()
            logger.info(f"已插入 {cursor.rowcount} 行数据到 n_by_hour 表")
            asbot = AsBot('人机黄乾')
            asbot.send_text_to_group(f'{datetime.now().date()}成功刷新{cursor.rowcount}条bi数据')
    except pymysql.MySQLError as e:
        logger.info(f"查询或插入数据时出错: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Flow: 编排删除和查询插入任务
@flow(name="MySQL 数据处理工作流")
def mysql_data_processing_flow():
    """
    Prefect 工作流：先删除 n_by_hour 数据，再执行统计查询并插入结果。
    """
    # 执行删除任务
    deleted_rows = delete_n_by_hour_data(db_config)

    # 执行查询并插入任务，依赖删除任务完成
    query_maintenance_data(db_config, wait_for=[deleted_rows])



# 运行工作流
if __name__ == "__main__":
    result = mysql_data_processing_flow()
    print("工作流完成，结果预览：")
    print(result.head())