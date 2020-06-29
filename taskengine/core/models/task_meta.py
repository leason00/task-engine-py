#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-06-29 22:10
# @Author  : leason

from sqlalchemy import Column, Integer, String, DateTime, Boolean, func

from taskengine.core.models.models import Base, FormatMixin
from taskengine.db.mysql import db_session


class TaskMeta(Base, FormatMixin):
    """
        任务队列表
    """
    __tablename__ = 'task_meta'
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')