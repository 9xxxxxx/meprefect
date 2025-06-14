# -*- coding: utf-8 -*-
# @Time : 2025/5/29 11:29
# @Author : Garry-Host
# @FileName: app


import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# 初始化webdriver
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--window-size=1200,800')
    # 你可以选择设置更多的浏览器选项
    driver = webdriver.Chrome(options=options)
    return driver


# 登录函数
def login(driver, username, password):
    driver.get("https://ap6.fscloud.com.cn/t/laifen")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'username')))

    driver.find_element(By.ID, 'username').send_keys(username)
    driver.find_element(By.ID, 'password').send_keys(password)
    driver.find_element(By.ID, 'kc-login').click()
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="global-app"]/div/div/div[1]/div/div/div[2]/ul/li[3]/div')))
    print("登录成功")


# 导航到目标页面
def navigate_to_page(driver):
    # 点击逆向管理
    time.sleep(2)


    driver.find_element(By.XPATH, '//*[@id="global-app"]/div/div/div[1]/div/div/div[2]/ul/li[4]/div/div[1]/span').click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="global-app"]/div/div/div[1]/div/div/div[2]/ul/li[4]/ul/li[1]/span').click()

    print("导航到服务单成功")
    time.sleep(2)

# 处理每个单号查询
def process_id(driver, id, actions, ztlb, sjlb):
    print(f'正在查找{id}')

    # 输入id查询
    driver.find_element(By.XPATH,'//*[@id="__qiankun_microapp_wrapper_for_mainweb__"]/div[1]/div/div/div[1]/div[1]/div[1]/div[1]/div/div[4]/div/div/div/input').send_keys(id)
    actions.send_keys(Keys.ENTER).perform()
    time.sleep(3)


    try:

        driver.find_element(By.XPATH,'//*[@id="__qiankun_microapp_wrapper_for_mainweb__"]/div[1]/div/div/div[1]/div[2]/div/div/div/div[1]/div[5]/div[2]/table/tbody/tr/td[2]/div/div/a/span').click()
        time.sleep(3)

        serve_progress = driver.find_element(By.XPATH,
                                             '//*[@id="e5e5bc56-ee39-4481-bdfb-3955360601f7"]/div[2]/div/div[2]/div/ul')
        status = serve_progress.find_elements(By.XPATH, './*')

        # for statu in status:
        #     time.sleep(1)
        #     detail = statu.find_element(By.XPATH, "./li/div[2]/div[1]")
        #     info = detail.find_elements(By.XPATH, './*')
        #
        #     ztlb.append(info[0].text)
        #     sjlb.append(info[1].text)
        #     print(f"查找到{id},状态--{info[0].text},时间--{info[1].text}\n")

        detail = status[0].find_element(By.XPATH, "./li/div[2]/div[1]")
        info = detail.find_elements(By.XPATH, './*')

        ztlb.append(info[0].text)
        sjlb.append(info[1].text)
        print(f"查找到{id},状态--{info[0].text},时间--{info[1].text}\n")
        # 点击返回
        driver.find_element(By.XPATH,
                            '//*[@id="__qiankun_microapp_wrapper_for_mainweb__"]/div[1]/div/div/form/div[1]/div[1]/button/span').click()
    except Exception as e:
        print(f"查找{id}时发生错误: {e}")
        ztlb.append(f'{id} 查询失败')
        sjlb.append('')


    finally:
        time.sleep(3)
        #  清除输入框
        driver.find_element(By.XPATH,
                            '//*[@id="__qiankun_microapp_wrapper_for_mainweb__"]/div[1]/div/div/div[1]/div[1]/div[1]/div[1]/div/div[4]/div/div/div/input').clear()


# 主函数
def main():
    try:
        driver = init_driver()
        login(driver, 'huangqian', 'Laifen@2022')
        navigate_to_page(driver)

        actions = ActionChains(driver)
        # statudesc = '待质检'

        result = {}
        dhlb,ztlb,sjlb = [],[],[]

        path = r"C:\Users\garry\Desktop\瑞云单号.xlsx"
        data = pd.read_excel(path)
        for id in data['单号']:
            id = id.strip()
            dhlb.append(id)
            process_id(driver, id, actions, ztlb, sjlb)
    except Exception as e:
        print(e)
    finally:
        # 输出结果到 DataFrame 并保存为 CSV
        result['单号'] = dhlb
        result['当前状态'] = ztlb
        result['时间'] = sjlb
        # print(result)
        df = pd.DataFrame(result)
        df.to_excel('result.xlsx', index=False)

        driver.quit()

if __name__ == '__main__':
    main()

