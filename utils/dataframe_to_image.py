# -*- coding: utf-8 -*-
# @Time : 2025/7/10 14:59
# @Author : Garry-Host
# @FileName: dataframe_to_image
import plotly.graph_objects as go

def export_dataframe_to_image_v2(
    df,
    output_path,
    title="DataFrame Export",
    image_size=(800, 500),
    font_family="Arial",  # 确保兼容的字体
    font_size=18
):
    """
    将 pandas DataFrame 导出为图片，增强样式显示效果。

    参数：
    - df: pandas DataFrame，需要导出的数据
    - output_path: str，图片保存路径
    - title: str，图片标题
    - image_size: tuple，图片尺寸 (宽, 高)，默认 (800, 500)
    - font_family: str，字体样式，默认 "Arial"
    - font_size: int，字体大小，默认 18
    """
    # 配色方案
    header_color = [ "#007BFF", '#007BFF', '#007BFF', '#8081cf', '#8081cf', '#3cc08e', '#3cc08e']  # 表头背景色
    header_font_color = "white"  # 表头字体颜色
    cell_fill_colors = ["#F9F9F9", "#FFFFFF"]  # 单元格条纹背景
    cell_font_color = "#333333"  # 默认单元格字体颜色

    # 动态设置字体颜色：带 + 号的数字为红色
    font_colors = []
    for col in df.columns:
        col_colors = []
        for value in df[col]:
            if isinstance(value, str) and "+" in value:  # 判断是否包含 + 号
                col_colors.append("red")  # 红色
            # elif isinstance(value, str) and "-" in value: # 判断是否包含 - 号
            #     col_colors.append("00ff80")  # 绿色
            else:
                col_colors.append(cell_font_color)  # 默认颜色
        font_colors.append(col_colors)

    # 创建表格
    table = go.Figure(data=[go.Table(
        columnwidth=[1.5,1, 1.2, 1, 1.2, 1, 1.2],  # 列宽设置
        header=dict(
            values=[f"<b>{col}</b>" for col in df.columns],
            fill_color=header_color,
            align="center",
            font=dict(family=font_family, size=font_size, color=header_font_color),
            line_color="white",  # 表头边框颜色
            height=40  # 增大表头高度
        ),
        cells=dict(
            values=[df[col] for col in df.columns],
            fill_color=[cell_fill_colors * (len(df) // 2 + 1)],
            align="center",
            font=dict(family=font_family, size=font_size - 2),
            line_color="#E5E5E5",  # 单元格边框颜色
            height=30,  # 增大单元格高度
            font_color=font_colors  # 动态设置字体颜色
        )
    )])

    # 布局设置
    table.update_layout(
        title=dict(
            text=f"<b>{title}</b>",
            font=dict(family=font_family, size=font_size + 4, color="#007BFF"),
            x=0.5,
            xanchor="center"
        ),
        margin=dict(l=20, r=20, t=70, b=20),  # 边距优化
        paper_bgcolor="white",  # 背景色
        width=image_size[0],
        height=image_size[1]
    )

    # 保存图片
    table.write_image(output_path, width=image_size[0], height=image_size[1], scale=3)
    print(f"图片已保存到: {output_path}")
