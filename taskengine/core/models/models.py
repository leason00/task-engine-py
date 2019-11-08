#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 16:20
# @Author  : Leason
import json

from sqlalchemy import Column, Integer, String, DateTime, func, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from taskengine.core.const import TaskStatus, TaskStepInfoStatus
from taskengine.db.mysql import db_session

Base = declarative_base()


class FormatMixin(object):

    @property
    def json_formatted(self):
        dict_ = {}
        for key in self.__mapper__.c.keys():
            value = getattr(self, key)
            dict_[key] = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else value
        return dict_


class TaskQueue(Base, FormatMixin):
    """
        任务队列表
    """
    __tablename__ = 'task_queue'
    id = Column(Integer, primary_key=True, comment=u'主键ID')
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'start step name')
    status = Column(String(100), nullable=False, server_default=TaskStatus.created, comment=u'status')
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'创建时间')
    update_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'更新时间')
    params = Column(String(1024), nullable=False, server_default='', comment=u'参数')

    @staticmethod
    def init_by_dict(task_dict):
        task_queue = TaskQueue(**task_dict)
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
            result = session.query(TaskQueue).filter(TaskQueue.id == id).first()
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
            result = session.query(TaskQueue).filter(TaskQueue.status == TaskStatus.created).order_by(
                TaskQueue.id.desc()).limit(limit).offset(0).all()
            if result:
                return result
            return []
        finally:
            session.close()

    @staticmethod
    def update_status(task_queue_id, status, step_name=None):
        """
        更新task的状态
        :param task_queue_id:
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
                data[step_name] = step_name
            result = session.query(TaskQueue).filter(
                TaskQueue.id == task_queue_id,

            ).update(data)
            if result == 0:
                raise Exception("no match")

            session.commit()
        finally:
            session.close()


class TaskStepMeta(Base, FormatMixin):
    """
        任务每一步信息元数据表
    """
    __tablename__ = 'task_step_info'
    id = Column(Integer, primary_key=True, comment=u'主键ID')
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'每个步骤名字')
    index = Column(String(100), nullable=False, server_default='', comment=u'每个步骤名字')
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'创建时间')
    update_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'更新时间')

    @staticmethod
    def get_steps(task_key):
        session = db_session()
        try:
            result = session.query(TaskStepMeta).filter(TaskStepMeta.task_key == task_key).order_by(
                TaskStepMeta.index).all()
            if result:
                return result
            return []
        finally:
            session.close()


class TaskStepInfo(Base, FormatMixin):
    """
        实例化的每步信息表
    """
    __tablename__ = 'task_step_meta'
    id = Column(Integer, primary_key=True, comment=u'主键ID')
    task_queue_id = Column(Integer, primary_key=True, comment=u'任务id')
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'每个步骤名字')
    context = Column(String(2048), nullable=False, server_default='', comment=u'上下文')
    exec_log = Column(Text, nullable=False, server_default='', comment=u'该步骤的执行日志')
    status = Column(String(100), server_default=TaskStepInfoStatus.wait, comment=u'当前步骤执行状态 未执行0 1已执行 2执行中 3中断')
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'创建时间')
    update_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'更新时间')

    @staticmethod
    def get_steps_by_task_id(task_queue_id):
        session = db_session()
        try:
            print(task_queue_id)
            result = session.query(TaskStepInfo).filter(TaskStepInfo.task_queue_id == task_queue_id).order_by(
                TaskStepInfo.id.desc()).all()
            print(result)
            if result:
                return result
            return []
        finally:
            session.close()

    @staticmethod
    def insert_steps(step_infos):
        # 跟task的新任务，实例化这个task
        session = db_session()
        try:
            for step_info in step_infos:
                session.add(TaskStepInfo(
                    task_queue_id=step_info["task_queue_id"],
                    task_key=step_info["task_key"],
                    step_name=step_info["step_name"],
                    context=json.dumps({})
                ))
            session.commit()
        finally:
            session.close()

    @staticmethod
    def save_step_context(context, step_name, task_queue_id):
        # 保存任务上下文
        session = db_session()
        try:
            result = session.query(TaskStepInfo).filter(
                TaskStepInfo.task_queue_id == task_queue_id,
                TaskStepInfo.step_name == step_name,

            ).update({
                "context": context
            })
            if result == 0:
                raise Exception("no match")

            session.commit()
        finally:
            session.close()

    @staticmethod
    def get_step(task_queue_id, step_name):
        # 获取真正执行的某一步的信息
        session = db_session()
        try:
            result = session.query(TaskStepInfo).filter(TaskStepInfo.task_queue_id == task_queue_id,
                                                        TaskStepInfo.step_name == step_name).first()
            if result:
                return result
            raise Exception("error")
        finally:
            session.close()

    @staticmethod
    def set_step_status(task_queue_id, step_name, status):
        # todo 限定status的取值
        # 设置该step的状态
        session = db_session()
        try:
            result = session.query(TaskStepInfo).filter(
                TaskStepInfo.task_queue_id == task_queue_id,
                TaskStepInfo.step_name == step_name,

            ).update({
                "status": status
            })
            if result == 0:
                raise Exception("no match")

            session.commit()
        finally:
            session.close()







