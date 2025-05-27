# -*- coding: utf-8 -*-
# @Time : 2025/5/22 9:02
# @Author : Garry-Host
# @FileName: main.py
from prefect import flow, task, get_run_logger

@task
def extract():
    logger = get_run_logger()
    logger.info("Extracting data")
    return [1, 2, 3]

@task
def transform(data):
    logger = get_run_logger()
    logger.info("Transforming data")
    return [x * 2 for x in data]

@task
def load(data):
    logger = get_run_logger()
    logger.info(f"Loaded {len(data)} items")

@task
def save(data):
    logger = get_run_logger()
    logger.info(f"Saving {len(data)} items")

@flow(log_prints=True)
def etl_flow(message: str):
    logger = get_run_logger()
    data = extract()
    transformed = transform(data)
    load(transformed)
    save(transformed)
    logger.info(f'{message} done')

if __name__ == "__main__":
    etl_flow()
