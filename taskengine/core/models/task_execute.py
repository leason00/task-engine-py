#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-06-29 22:10
# @Author  : leason
from sqlalchemy import Column, Integer, String, DateTime, Boolean, func

from taskengine.core.const import TaskStatus
from taskengine.core.models.models import Base, FormatMixin
from taskengine.db.mysql import db_session


class TaskExecute(Base, FormatMixin):
    """
        任务队列表
    """
    __tablename__ = 'task_execute'
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'start step name')
    status = Column(String(100), nullable=False, server_default=TaskStatus.created, comment=u'status')
    params = Column(String(1024), nullable=False, server_default='', comment=u'参数')
    context = Column(String(2048), nullable=False, server_default='', comment=u'上下文')

    @staticmethod
    def init_by_dict(task_dict):
        task_queue = TaskExecute(**task_dict)
        return task_queue

    @staticmethod
    def get_task_by_id(id):
        """
        get task by id
        :param id:
        :return:
        """
        session = db_session()
        try:
            result = session.query(TaskExecute).filter(TaskExecute.id == id).first()
            if not result:
                raise Exception("get task error. no task id is {}".format(id))
            return result

        finally:
            session.close()

    @staticmethod
    def get_created_task_queue(limit=10):
        # 从task queue获取待执行的任务
        session = db_session()
        try:
            result = session.query(TaskExecute).filter(TaskExecute.status == TaskStatus.created).order_by(
                TaskExecute.id.desc()).limit(limit).offset(0).all()
            if result:
                return result
            return []
        finally:
            session.close()

    def update_status(self, status, step_name=None):
        """
        更新task的状态
        :param status:
        :param step_name: 方便结束或者中断的时候设置step name
        :return:
        """
        session = db_session()
        try:

            data = {
                "status": status
            }
            if step_name:
                data["step_name"] = step_name
            result = session.query(TaskExecute).filter(
                TaskExecute.id == self.id,

            ).update(data)
            if result == 0:
                raise Exception("no match")

            session.commit()
        finally:
            session.close()

    def save_step_context(self, context):
        # 保存任务上下文
        session = db_session()
        try:
            result = session.query(TaskExecute).filter(
                TaskExecute.id == self.id
            ).update({
                "context": context
            })
            if result == 0:
                raise Exception("no match")

            session.commit()
        finally:
            session.close()