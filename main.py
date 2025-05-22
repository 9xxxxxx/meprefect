# -*- coding: utf-8 -*-
# @Time : 2025/5/22 9:02
# @Author : Garry-Host
# @FileName: main.py
from prefect import flow, task


@task
def extract():
    return [1, 2, 3]

@task
def transform(data):
    return [x * 2 for x in data]

@task
def load(data):
    print(f"Loaded {len(data)} items")

@task
def save(data):
    print(f"Saving {len(data)} items")

@flow
def etl_flow():
    data = extract()
    transformed = transform(data)
    load(transformed)
    save(transformed)


