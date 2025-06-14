import time
from datetime import datetime
import os


# 数据库配置
DATABASE = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "000000",
    "database": "demo"
}
# dialect+driver://username:password@host:port/database

DB_STRING = rf'mysql+pymysql://{DATABASE["user"]}:{DATABASE["password"]}@{DATABASE["host"]}:{DATABASE["port"]}/{DATABASE["database"]}'

# API 配置
APP_ID = "cli_a7e1e09d72fe500e"
APP_SECRET = "sIsx3hT20I4oQ2lrq0ydAf04LTmaKxP7"

# 文件类型配置
TYPE_FILE = "application/vnd.ms-excel"


# 时间相关函数
def show_in_group():
    """返回当前时间的格式化字符串"""
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def showdate():
    """返回当前日期的格式化字符串"""
    return datetime.now().strftime("%Y-%m-%d %H-%M")


# 文件路径生成函数
def get_input_file_path():
    """生成输入文件路径"""
    filename = fr"瑞云积压数据{showdate()}"
    return os.path.join("../data/input/", filename) + '.xlsx'


def get_output_file_path():
    """生成输出文件路径"""
    outfilename = f"瑞云系统未发货清单截至{showdate()}.xlsx"
    return fr'E:\Dev\myprefect\data\output\{outfilename}'

def get_send_file_name():
    outfilename = f"瑞云系统未发货清单截至{showdate()}.xlsx"
    return outfilename


# 文件路径
image_path1 = r'../data/image/data.png'
image_path2 = r'../data/image/data0.png'
image_path3 = r'../data/image/data1.png'
image_path4 = r'../data/image/data2.png'

# 标题生成函数
def get_jxjy_title1():
    """生成寄修数据实时汇报标题"""
    return f'寄修数据实时汇报--{show_in_group()}'

def get_fjjy_title2():
    """生成退换货分拣数据实时汇报标题"""
    return f'退换货分拣数据实时汇报--{show_in_group()}'

def get_wl_title3():
    """生成物流单分布实时汇报标题"""
    return f'物流单分布实时汇报--{show_in_group()}'

def get_crm_title4():
    """生成一天内创单实时汇报标题"""
    return f'一天内创单量--{show_in_group()}'