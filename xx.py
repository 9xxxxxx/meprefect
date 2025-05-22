# -*- coding: utf-8 -*-
# @Time : 2025/5/22 9:37
# @Author : Garry-Host
# @FileName: xx
from prefect import flow
from main import etl_flow

if __name__ == "__main__":
    # 需要先初始化项目
    project.register_flow(etl_flow)

    # 部署到指定工作池
    etl_flow.deploy(
        name="my-deployment",
        work_pool_name="my-work-pool",
        image="my-docker-image:latest"  # 容器化部署时使用
    )