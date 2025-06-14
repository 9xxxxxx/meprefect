# -*- coding: utf-8 -*-
# @Time : 2025/5/29 18:01
# @Author : Garry-Host
# @FileName: send_file_tjh

from utils.asbot import AsBot
from utils.data_process import extractinfo
from config import asbot_config
import hashlib
import time
import uuid
import requests
import pandas as pd
from prefect import task,flow,get_run_logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# 获取寄修数据积压数据

def get_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


@task
def generate_requrl(pageindex,page,day):
    logger = get_run_logger()
    logger.info(f"正在生成第{page}页的URL")
    """
    从 API 获取数据并转换为 DataFrame
    """
    # 基本参数
    tenant = "laifen"
    api_name = "api/vlist/ExecuteQuery"
    timestamp = str(int(time.time() * 1000))
    reqid = str(uuid.uuid1())
    appid = "AS_department"
    queryid = "38c53a54-813f-a0e0-0000-06f40ebdeca5"
    is_user_query = "true"
    is_preview = "false"
    pagesize = "5000"
    paging = "true"
    key = "u7BDpKHA6VSqTScpEqZ4cPKmYVbQTAxgTBL2Gtit"
    orderby = "createdon descending"
    extendConditions = f'[{{"name":"new_signedon","val":"{day}","op":"last-x-days"}}]'
    args = [appid, extendConditions,orderby, pageindex, pagesize, paging, reqid, tenant, timestamp, is_preview,
            is_user_query, queryid, key]

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
        f"&$extendConditions={extendConditions}&$orderby={orderby}&$sign={sign}"
    )
    logger.info(f"成功生成第{page}页的URL: {url}")
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


def extract_need_data(df):
    logger = get_run_logger()
    df = df.assign(
        产品类型=df["new_productmodel_id"].apply(lambda x: x.get("name", None)),
        产品名称=df["new_product_id"].apply(lambda x: x.get("name", None)),
        旧件签收时间=df["FormattedValues"].apply(lambda x: x.get("new_signedon", None)),
        检测时间=df["FormattedValues"].apply(lambda x: x.get("new_checkon", None)),
        申请类别=df["FormattedValues"].apply(lambda x: x.get("new_srv_rma_0.new_applytype", None)),
        一检时间=df["FormattedValues"].apply(lambda x: x.get("laifen_onechecktime", None)),
        维修完成时间=df["FormattedValues"].apply(lambda x: x.get("laifen_servicecompletetime", None)),
        质检完成时间=df["FormattedValues"].apply(lambda x: x.get("laifen_qualityrecordtime", None)),
        单号=df['new_rma_id'].apply(lambda x: x.get('name', None)),
        分拣人员=df['laifen_systemuser2_id'].apply(lambda x: x.get('name', None) if pd.notnull(x) else None),
        处理状态=df["FormattedValues"].apply(lambda x: x.get("new_srv_rma_0.new_status", None)),
        旧件处理状态=df["FormattedValues"].apply(lambda x: x.get("new_returnstatus", None)),
        检测结果=df["FormattedValues"].apply(lambda x: x.get("new_solution", None)),
        故障现象=df['new_error_id'].apply(lambda x: x.get('name', None) if pd.notnull(x) else None),
        发货时间=df['new_deliveriedon'],
        一检人员=df['laifen_systemuser_id'].apply(lambda x: x.get('name', None) if pd.notnull(x) else None),
        发货状态=df['FormattedValues'].apply(lambda x: x.get('new_srv_rma_0.new_deliverstatus', None)),
        产品序列号=df['new_userprofilesn'],
        服务人员=df['new_srv_workorder_1.new_srv_worker_id'].apply(
            lambda x: x.get('name', None) if pd.notnull(x) else None),
        单据来源=df["FormattedValues"].apply(lambda x: x.get("new_srv_rma_0.new_fromsource", None)),
        创建时间=df["FormattedValues"].apply(lambda x: x.get("createdon", None)),
    )
    #    # 选择需要的列
    df = df[[
        '单号', '产品类型', '产品名称', '处理状态', '旧件处理状态', '检测结果', '申请类别', '旧件签收时间',
        '检测时间', '一检时间', '维修完成时间', '质检完成时间', '故障现象', '发货时间', '发货状态',
        '一检人员', '产品序列号', '分拣人员', '服务人员', '单据来源', '创建时间'
    ]]
    logger.info(f"成功提取所需数据,共{df.shape[1]}列")
    return df

@task
def get_sf_data(days=15):
    logger = get_run_logger()
    logger.info(f"正在下载最近{days}天的数据")
    pageindex = "1"
    url = generate_requrl(pageindex,'0',days)

    rs = requests.get(url)
    count = rs.json()['Data']['TotalRecordCount']
    logger.info(f"最近{days}天签收业务量共{count}单,共{count // 5000 + 2}页数据")
    datas = []

    for i in range(1, count // 5000 + 2):
        url = generate_requrl(str(i),i,days)
        data = fetch_api_data(url, i)
        logger.info(f"第{i}页数据已获取")
        datas.append(data)

    df = pd.concat(datas, ignore_index=True)
    df = extract_need_data(df)
    logger.info(f"已成功下载最近{days}天的数据")
    return df

@flow(name='发送寄修数据详细给陶健宏')
def send_file_to_tjh():
    logger = get_run_logger()
    data = get_sf_data()
    outpath = asbot_config.get_output_file_path()
    extractinfo(data, outpath)
    asbot = AsBot('人机')
    asbot.send_file_to_陶健宏("xls", file_name=asbot_config.get_send_file_name(), file_path=outpath,
                   type_file=asbot_config.TYPE_FILE)
    logger.info(f'{outpath}发送成功')

if __name__ == '__main__':
    send_file_to_tjh()