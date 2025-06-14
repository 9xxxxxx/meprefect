# -*- coding: utf-8 -*-
# @Time : 2025/5/29 17:53
# @Author : Garry-Host
# @FileName: data_process

import pandas as pd

def extractinfo(df, outpath):
    # print(f'正在处理-{path}')
    myrows = df.query(" 发货状态.isnull() or 发货状态 == '待安排发货' ")
    rydf = myrows.query(" 申请类别 == '寄修/返修' ")
    data_0 = rydf.query("处理状态 != '已取消'")
    data = data_0.query("产品类型 == '产成品-电动牙刷' or 产品类型 == '产成品-吹风机'")

    # 待发货
    tobedeliver = data.query(" 质检完成时间.notnull()")
    # 待维修
    tobefix = data.query("质检完成时间.isnull() and 维修完成时间.isnull()")
    # 待质检
    tobeqc = data.query(" 维修完成时间.notnull() and 质检完成时间.isnull()")


    in_maintenance = tobefix.query(" 旧件处理状态 == '维修中'")
    checked = tobefix.query("旧件处理状态 == '已检测' ")
    first_Detected = tobefix.query(" 旧件处理状态 == '已一检'")
    Signed = tobefix.query(" 旧件处理状态 == '已签收'")
    Exception_all = data.query("检测结果 == '异常'")

    with pd.ExcelWriter(outpath, engine='xlsxwriter') as writer:
        data.to_excel(writer, sheet_name='数据源', index=False)
        tobedeliver.to_excel(writer, sheet_name='待发货', index=False)
        tobeqc.to_excel(writer, sheet_name='待质检', index=False)

        tobefix.to_excel(writer, sheet_name='待维修', index=False)
        in_maintenance.to_excel(writer, sheet_name='待维修-维修中', index=False)
        first_Detected.to_excel(writer, sheet_name='待维修-已一检待派工', index=False)
        checked.to_excel(writer, sheet_name='待维修-已分拣待一检', index=False)
        Signed.to_excel(writer, sheet_name='待分拣-已签收待分拣', index=False)
        Exception_all.to_excel(writer, sheet_name='异常', index=False)

    print(f'处理完成，文件保存至-{outpath}')

