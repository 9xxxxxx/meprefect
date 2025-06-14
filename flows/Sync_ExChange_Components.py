# -*- coding: utf-8 -*-
# @Time : 2025/5/22 9:37
# @Author : Garry-Host
# @FileName: xx
from prefect import task, flow,get_run_logger
import requests
import time
import uuid
import hashlib
import pandas as pd
from sqlalchemy import create_engine
import json
from datetime import datetime, timedelta, UTC
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# 获取当前日期
def get_time_interverl_condition():
    logger = get_run_logger()
    current_date = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    # 计算起始日期（当前日期减去一天）
    start_date = current_date - timedelta(days=1)


    # 格式化为ISO 8601格式，包含时区信息
    start_iso = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = current_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 构造JSON对象
    json_obj = [{
        "name": "createdon",
        "val": [start_iso, end_iso],
        "op": "between"
    }]

    # 生成JSON字符串
    json_str = json.dumps(json_obj)
    logger.info(f'查询字符串为--{json_str}')
    return json_str


def get_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


@task
def generate_url(pageindex):
    logger = get_run_logger()
    logger.info(f"正在生成第{pageindex}页的URL")
    """
    从 API 获取数据并转换为 DataFrame
    """
    # 基本参数
    tenant = "laifen"
    api_name = "api/vlist/ExecuteQuery"
    timestamp = str(int(time.time() * 1000))
    reqid = str(uuid.uuid1())
    appid = "AS_department"
    # queryid = "a8532354-813f-a0c1-0000-070e7581f6a1"
    queryid = "a33f1c62-4028-beb0-0000-071ebfa85696"
    is_user_query = "true"
    is_preview = "false"
    pagesize = "5000"
    paging = "true"
    key = "u7BDpKHA6VSqTScpEqZ4cPKmYVbQTAxgTBL2Gtit"
    # orderby = "createdon descending"
    # extendConditions = quote([{"name":"new_checkon","val":"this-month","op":"this-month"}], safe='')
    # additionalConditions = quote({"createdon":"","new_signedon":"","new_checkon":"","laifen_qualityrecordtime":"","laifen_servicecompletetime":""}, safe='')
    # extendConditions = '[{"name":"createdon","val":["2025-01-01T00:00:00.000Z","2025-03-15T00:00:00.000Z"],"op":"between"}]'
    # extendConditions = '[{"name":"createdon","val":"before-today","op":"before-today"},{"name":"createdon","val":"180","op":"last-x-days"}]'

    # extendConditions = '[{"name":"createdon","val":["2025-05-01T16:00:00.000Z","2025-05-05T16:00:00.000Z"],"op":"between"}]'
    # extendConditions = get_time_interverl_condition()
    extendConditions = '[{"name":"createdon","val":"yesterday","op":"yesterday"}]'

    args = [appid, extendConditions, pageindex, pagesize, paging, reqid, tenant, timestamp, is_preview, is_user_query,
            queryid, key]

    """
    生成签名
    """

    sign_str = "".join(args)
    sign = hashlib.sha256(sign_str.encode('utf-8')).hexdigest().upper()
    # 构建 URL
    url = (
        f"https://ap6-openapi.fscloud.com.cn/t/{tenant}/open/{api_name}"
        f"?$tenant={tenant}&$timestamp={timestamp}&$reqid={reqid}&$appid={appid}"
        f"&queryid={queryid}&isUserQuery={is_user_query}&isPreview={is_preview}"
        f"&$pageindex={pageindex}&$pagesize={pagesize}&$paging={paging}"
        f"&$extendConditions={extendConditions}&$sign={sign}"
    )
    logger.info(f"成功生成第{pageindex}页的URL: {url}")
    return url

@task(retries=3, retry_delay_seconds=1)
def fetch_api_data(url, page):
    logger = get_run_logger()
    logger.info(f"正在获取第{page}页数据")
    # 发送 GET 请求
    session = get_session()
    response = session.get(url, timeout=30)
    if response.status_code != 200:
        raise Exception(f"API 请求失败，状态码: {response.status_code}")

    # 解析 JSON 数据
    data = response.json()
    entities = data["Data"]["Entities"]
    df = pd.DataFrame(entities)
    logger.info(f"第{page}页数据，已通过API获取成功获取")
    return df

@task
def extract_fields(df):
    logger = get_run_logger()
    api_data = pd.DataFrame()
    api_data = api_data.assign(
        服务单=df["new_srv_workorder_id"].apply(lambda x: x.get("name", None) if pd.notnull(x) else None),
        创建时间 = df['FormattedValues'].apply(lambda x: x.get('createdon',None) if pd.notnull(x) else None),
        核销状态 = df['FormattedValues'].apply(lambda x: x.get('new_returnstate',None) if pd.notnull(x) else None),
        新件名称 = df['new_productnew_id'].apply(lambda x: x.get("name", None) if pd.notnull(x) else None),
        新件编码=df["new_productcodenew"].apply(lambda x: x if pd.notnull(x) else None),
        创建者 = df['createdby'].apply(lambda x: x.get("name", None) if pd.notnull(x) else None),
        产品名称 = df['new_srv_productline_1.new_product_id'].apply(lambda x: x.get('name',None) if pd.notnull(x) else None),
        产品类别 = df['new_srv_productline_1.new_productgroup_id'].apply(lambda x: x.get("name", None) if pd.notnull(x) else None),
        故障现象 = df['new_srv_workorder_0.laifen_error_id'].apply(lambda x: x.get("name", None) if pd.notnull(x) else None),
        数量=df["new_qty"].apply(lambda x: x if pd.notnull(x) else None),
    )
    #    # 选择需要的列

    logger.info(f"成功提取所需数据,共{api_data.shape[1]}列")
    return api_data


@task
def get_ExChange_Compo_data():
    logger = get_run_logger()
    logger.info(f"正在下载更换配件的数据")
    url_first = generate_url.submit(str(1)).result()
    session = get_session()
    response = session.get(url_first)
    total_count = response.json()['Data']['TotalRecordCount']
    total_pages = total_count // 5000 + 1
    logger.info(f"更换配件数量共{total_count}单,共{total_pages}页数据")
    all_dataframes = []

    for page in range(1, total_pages + 1):
        url = generate_url.submit(str(page)).result()
        df_page = fetch_api_data.submit(url, page).result()
        logger.info(f"第{page}页数据已获取")
        all_dataframes.append(df_page)

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    logger.info("数据抓取完成")
    return extract_fields.submit(combined_df).result()


@flow(name="同步寄修昨日更换件配件明细数据到数据库")
def sync_exchange_components_flow():
    logger = get_run_logger()
    conn = create_engine("mysql+pymysql://root:000000@localhost/demo")
    try:
        qcdata = get_ExChange_Compo_data()
    except Exception as e:
        logger.error(e)
        return

    rows = qcdata.to_sql('srv_change_components', conn, if_exists='append', index=False)
    if rows:
        from utils.asbot import AsBot
        asbot = AsBot('人机黄乾')
        asbot.send_text_to_group(f'{datetime.now().date()}成功插入{rows}条更换配件数据')
        logger.info(f'成功插入{rows}条更换配件数据')
    else:
        logger.info('更换配件数据更新失败')


if __name__ == '__main__':
    sync_exchange_components_flow()