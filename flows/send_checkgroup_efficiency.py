# -*- coding: utf-8 -*-
# @Time : 2025/6/6 10:33
# @Author : Garry-Host
# @FileName: check_efficency
from typing import Any
import requests
import time
import uuid
import hashlib
import pandas as pd
from datetime import date
import json
from datetime import datetime
from prefect import task, flow,get_run_logger
from utils.asbot import AsBot

@task
def generate_requrl(pageindex):
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
    queryid = "38c53a54-813f-a0e0-0000-06f40ebdeca5"
    is_user_query = "true"
    is_preview = "false"
    pagesize = "5000"
    paging = "true"
    key = "u7BDpKHA6VSqTScpEqZ4cPKmYVbQTAxgTBL2Gtit"
    # orderby = "createdon descending"
    # extendConditions = quote([{"name":"new_checkon","val":"this-month","op":"this-month"}], safe='')
    # additionalConditions = quote({"createdon":"","new_signedon":"","new_checkon":"","laifen_qualityrecordtime":"","laifen_servicecompletetime":""}, safe='')
    extendConditions = '[{"name":"new_checkon","val":"this-month","op":"this-month"}]'

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


@task
def fetch_api_data(url, page):
    logger = get_run_logger()
    logger.info(f"正在获取第{page}页数据")
    # 发送 GET 请求
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API 请求失败，状态码: {response.status_code}")

    # 解析 JSON 数据
    data = response.json()
    entities = data["Data"]["Entities"]

    df = pd.DataFrame(entities)
    logger.info(f"第{page}页数据，已通过API获取成功获取")
    return df

@task
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
        物流单号=df['new_srv_rma_0.new_returnlogisticsnumber'],
        产品序列号=df['new_userprofilesn'],
        服务人员=df['new_srv_workorder_1.new_srv_worker_id'].apply(lambda x: x.get('name', None) if pd.notnull(x) else None),
        单据来源=df["FormattedValues"].apply(lambda x: x.get("new_srv_rma_0.new_fromsource", None)),
        创建时间=df["FormattedValues"].apply(lambda x: x.get("createdon", None)),
    )
    #    # 选择需要的列
    df = df[[
        '单号', '产品类型', '产品名称', '处理状态', '旧件处理状态', '检测结果', '申请类别', '旧件签收时间',
        '检测时间', '一检时间', '维修完成时间', '质检完成时间', '故障现象', '发货时间', '发货状态',
        '一检人员', '产品序列号', '物流单号', '分拣人员', '服务人员', '单据来源', '创建时间'
    ]]
    logger.info(f"成功提取所需数据,共{df.shape[1]}列")
    return df

@task
def get_cg_efficiency_data():
    logger = get_run_logger()
    logger.info(f"正在下载当月的数据")
    url = generate_requrl("1")
    rs = requests.get(url)
    # print(rs.text)
    count = rs.json()['Data']['TotalRecordCount']
    logger.info(f"当月分拣业务量共{count}单,共{count // 5000 + 2}页数据")
    datas = []

    for i in range(1, count // 5000 + 2):
        url = generate_requrl(str(i))
        data = fetch_api_data(url, i)
        logger.info(f"第{i}页数据已获取")
        datas.append(data)

    df = pd.concat(datas, ignore_index=True)
    df = extract_need_data(df)
    logger.info(f"已成功下载当月数据")
    return df
@task
def process_checkgroup_efficiency_data():
    logger = get_run_logger()
    df = get_cg_efficiency_data()
    data = df.query("检测时间.notnull() & 旧件签收时间.notnull() & 申请类别 != '寄修/返修'& 处理状态 != '已取消'").copy()


    # pj = data.query("产品类型 == '产成品-吹风机配件' or 产品类型 == '产成品-电动牙刷配件'").copy()
    # pj['独立配件'] = pj.groupby('物流单号')['单号'].transform('nunique')
    # pj = pj[pj['独立配件'] ==1 ].query("产品类型 == '产成品-吹风机配件' or 产品类型 == '产成品-电动牙刷配件'")
    # pj = pj.drop(['独立配件'],axis=1)

    # data = data.query("产品类型 == '产成品-吹风机' or 产品类型 == '产成品-电动牙刷'").copy()
    # data = pd.concat([data, pj], ignore_index=True)


    data['旧件签收时间'] = pd.to_datetime(data['旧件签收时间'])
    data['检测时间'] = pd.to_datetime(data['检测时间'])
    data['日期'] = data['检测时间'].dt.date
    data['时效'] = (data['检测时间'] - data['旧件签收时间']).dt.total_seconds() / 3600
    data['时效类型'] = pd.cut(data['时效'], bins=[0, 4, 8, 12,2480], labels=['4小时内', '4-8小时', '8-12小时','超12小时'])
    history = data[data['日期'] < date.today()].copy()
    today = data[data['日期'] == date.today()].copy()
    today.to_excel('wd.xlsx',index=False)
    history = pd.DataFrame(history['时效类型'].value_counts())
    history['占比'] = history['count'] / history['count'].sum()
    history = history.rename(columns={'count': '数量'})
    history = history.fillna(0)
    history['占比'] = history['占比'].apply(lambda x: f'{round(x * 100)}%')
    history = history.astype(str)

    today = pd.DataFrame(today['时效类型'].value_counts())
    today['占比'] = today['count'] / today['count'].sum()
    today['比率'] = today['count'] / today['count'].sum()
    today = today.rename(columns={'count': '数量'})
    today = today.fillna(0)
    today['占比'] = today['占比'].apply(lambda x: f'{round(x * 100)}%')
    today = today.astype(str)
    today['比率'] = today['比率'].astype(float)

    return history, today

@task
def build_chart_data(title,value1,value2,value3,value4,rate1,rate2,rate3,rate4):

    mock_data = [
        {
            "type": f"4小时内：{rate1}",
            "value": value1
        },
        {
            "type": f"4-8小时：{rate2}",
            "value": value2
        },
        {
            "type": f"8-12小时：{rate3}",
            "value": value3
        },
        {
            "type": f"超12小时：{rate4}",
            "value": value4
        },
    ]
    data = ({
        "type": "pie",
        "percent": "true",
        "title": {
            "text": title
        },
        "data": {
            "values": mock_data
        },
        "valueField": "value",
        "categoryField": "type",
        "outerRadius": 0.8,
        "innerRadius": 0.4,
        "padAngle": 0.6,
        "legends": {
            "visible": True,
        },
        "padding": {
            "left": 2,
            "top": 2,
            "bottom": 2,
            "right": 0
        },
        "label": {
            "visible": "true",
        },
        "pie": {
            "style": {
                "cornerRadius": 8
            },
            "state": {
                "hover": {
                    "outerRadius": 0.85,
                    "stroke": '#000',
                    "lineWidth": 1
                },
                "selected": {
                    "outerRadius": 0.85,
                    "stroke": '#000',
                    "lineWidth": 1
                }
            }
        },
    })
    return data


@task
def build_card_message() -> tuple[str, Any]:
    logger = get_run_logger()
    """
    构造可动态配置的嵌套JSON消息

    :param receive_id: 接收方ID
    :param template_id: 模板ID
    :param template_version: 模板版本
    :param template_vars: 模板变量字典（可自由增减字段）
    :param msg_type: 消息类型，默认为 interactive
    :return: 完整消息字典
    """
    # 构造内层 content 结构
    history, today = process_checkgroup_efficiency_data()
    variables = {
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "table_1": build_chart_data("分拣退换时效分布(当月历史)", history.loc["4小时内", "数量"],
                                    history.loc["4-8小时", "数量"], history.loc["8-12小时", "数量"],history.loc["超12小时", "数量"],
                                    history.loc["4小时内", "占比"], history.loc["4-8小时", "占比"],
                                    history.loc["8-12小时", "占比"],history.loc["超12小时", "占比"]),

        "table_2": build_chart_data("分拣退换时效分布(当日)", today.loc["4小时内", "数量"],
                                    today.loc["4-8小时", "数量"], today.loc["8-12小时", "数量"],today.loc["超12小时", "数量"],
                                    today.loc["4小时内", "占比"], today.loc["4-8小时", "占比"],
                                    today.loc["8-12小时", "占比"],today.loc["超12小时", "占比"]),

        "count4":history.loc["4小时内", "数量"],
        "count4_8":history.loc["4-8小时", "数量"],
        "count_over8":history.loc["8-12小时", "数量"],
        "count_over12":history.loc["超12小时", "数量"],
        "y_4_rate":history.loc["4小时内", "占比"],
        "y_4_8_rate":history.loc["4-8小时", "占比"],
        "y_over8_rate":history.loc["8-12小时", "占比"],
        "y_over12_rate":history.loc["超12小时", "占比"],
        "count4_today":today.loc["4小时内", "数量"],
        "count48_today":today.loc["4-8小时", "数量"],
        "count_over8_today":today.loc["8-12小时", "数量"],
        "count_over12_today":today.loc["超12小时", "数量"],
        "y_4_rate_today":today.loc["4小时内", "占比"],
        "y_4_8_rate_today":today.loc["4-8小时", "占比"],
        "y_over8_rate_today":today.loc["8-12小时", "占比"],
        "y_cover12_rate_today":today.loc["超12小时", "占比"],

    }
    content = {
        "type": "template",
        "data": {
            "template_id": "AAqBc0EeBjtyz",
            "template_version_name": "1.0.22",
            "template_variable": variables  # 动态变量部分
        }
    }
    logger.info("数据卡片构建成功")
    # 构造完整消息
    return json.dumps(content, ensure_ascii=False),today.loc["4小时内", "比率"]


@flow(name='发送分拣时效数据')
def send_checkgroup_efficiency_flow():
    logger = get_run_logger()
    logger.info('发送当月以及当天分拣退换货时效数据')
    payload,condition = build_card_message()
    asbot = AsBot("【售后维修部】吐槽群")
    asbot.send_card_to_group(payload)
    time.sleep(1)
    logger.info('发送当月以及当天分拣退换货时效数据成功')
