import pandas as pd
from google.protobuf.util.json_format_pb2 import PROTOCOL

all_sheets = pd.read_excel(r"C:\Users\garry\Desktop\你懂的BOM.xlsx", sheet_name=None)

all_sku = all_sheets['整机编码']
components = all_sheets['物料分类']


# for i in components:
#     print(components[i])


# 获取bom名称以及物料类型名称
boms = list(all_sheets.keys())[3:15]
boms_name = list(zip(boms[::2], boms[1::2]))
bill = list(all_sheets['物料分类'].columns)[:12]
component_names = list(zip(bill[::2], bill[1::2]))


def update_universal_component(component_name, boms, errors_bom):
    # 过滤出通用和辅料类型的BOM
    normal_boms = boms.query('类型 == "辅料" or 类型 == "通用"')
    errors_bom_u = errors_bom.copy()

    mask = normal_boms['标识'] == component_name
    if mask.any():
        code_value = normal_boms.loc[mask, '新编码'].iloc[0]
        # 仅更新非空值
        errors_bom_u.loc[errors_bom_u[component_name].notnull(), component_name] = code_value
    else:
        errors_bom_u.loc[errors_bom_u[component_name].notnull(), component_name] = '无此物料/无编码'
    return errors_bom_u  # 确保无论如何都返回DataFrame


# change 1
spus = ['LF03-SE','LFHDSE-Lite','MINI-Lite','LFHD-SE2','LFHDMini','LF03']


# 专用部件编码更新
def update_component_specical(boms, universal_component, errors_bom, color, special_components, sku, spu, cplx, zjmc,code):
    print(f'正在处理{spu}')
    errors_bom_s = errors_bom.copy()
    for comp in universal_component.dropna():
        print(f'通用配件-{comp}')
        errors_bom_s = update_universal_component(comp, boms, errors_bom_s)


    new_df = errors_bom_s.copy()
    new_df.insert(0, 'SKU', sku)
    new_df.insert(0, 'SPU', spu, )
    new_df.insert(0, '产品类型', cplx)
    new_df.insert(0, '整机名称', zjmc)
    new_df.insert(0, '整机编码', code)


    # 筛选颜色BOM
    source_df = boms.query('类型.str.contains(@color)', engine='python').copy()
    for comp in special_components.dropna():
        print(comp)
        mask = source_df['标识'] == comp
        if mask.any():
            print(sku,color, f'专用配件-{comp}')
            try:
                # 查询物料编码
                code_value = source_df.loc[mask, '新编码'].iloc[0]  # 取第一个匹配值
            except Exception as e:
                code_value = None
                print(e)
                print(code_value)
            if code_value:
                new_df.loc[errors_bom_s[comp].notnull(), comp] = code_value
            else:
                new_df.loc[errors_bom_s[comp].notnull(), comp] = '什么玩意'
        else:
             new_df.loc[errors_bom_s[comp].notnull(), comp] = '无此物料/无编码'
    return new_df


for spu_name, bom, comp in zip(spus, boms_name, component_names):
    print(spu_name, bom[0],bom[1],comp[0],comp[1])
    product_model = all_sku.query(f'SPU == "{spu_name}"')
    pd.concat(
        [update_component_specical(boms=all_sheets[bom[0]], universal_component=components[comp[1]],special_components=components[comp[0]],
                                   errors_bom=all_sheets[bom[1]], color=color, sku=sku, spu=spu, cplx=cplx, zjmc=zjmc,
                                   code=code) for color, sku, spu, cplx, zjmc, code in
         zip(product_model['颜色'], product_model['SKU'], product_model['SPU'], product_model['产品类型'],
             product_model['整机名称'], product_model['整机编码'])],
        ignore_index=True
    ).to_excel(f'./output/{spu_name}寄修BOM.xlsx', index=False)

    # print(components[comp[0]])
print('大功告成@------------------------@！！！！！！')