#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-06-29 22:01
# @Author  : leason
from sqlalchemy import Column, Integer, String

from taskengine.core.models.models import Base, FormatMixin
from taskengine.core.db.mysql import db_session


class StepMeta(Base, FormatMixin):
    """
        任务每一步信息元数据表
    """
    __tablename__ = 'step_meta'

    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'每个步骤名字')
    order_id = Column(Integer, nullable=False, server_default="0", comment=u'每个步骤名字')

    @staticmethod
    def get_steps(task_key):
        session = db_session()
        try:
            result = session.query(StepMeta).filter(StepMeta.task_key == task_key).order_by(
                StepMeta.order_id).all()
            if result:
                return result
            return []
        finally:
            session.close()