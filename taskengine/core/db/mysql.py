#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 16:27
# @Author  : Leason
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ENGINE_URL = "mysql+pymysql://root:123456@127.0.0.1:3306/taskflow?charset=utf8mb4"


def register_database():
    from sqlalchemy.pool import NullPool
    engine = create_engine(ENGINE_URL, poolclass=NullPool)
    # 创建DBSession类型:
    db_session = sessionmaker(bind=engine)
    return db_session


db_session = register_database()
