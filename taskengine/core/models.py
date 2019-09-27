#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 16:20
# @Author  : Leason
import json

from sqlalchemy import Column, Integer, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from taskengine.core.const import Status
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
    status = Column(Integer, nullable=False, server_default='0', comment=u'status')
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'创建时间')
    params = Column(String(1024), nullable=False, server_default='', comment=u'参数')


# class TaskInfo(Base, FormatMixin):
#     """
#         任务信息表
#     """
#     __tablename__ = 'task_info'
#     id = Column(Integer, primary_key=True, comment=u'主键ID')
#     task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')


class TaskStepInfo(Base, FormatMixin):
    """
        任务每一步信息表
    """
    __tablename__ = 'task_step_info'
    id = Column(Integer, primary_key=True, comment=u'主键ID')
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'每个步骤名字')


class TaskStepMeta(Base, FormatMixin):
    """
        真正执行的每步信息表
    """
    __tablename__ = 'task_step_meta'
    id = Column(Integer, primary_key=True, comment=u'主键ID')
    task_queue_id = Column(Integer, primary_key=True, comment=u'任务id')
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'每个步骤名字')
    context = Column(String(2048), nullable=False, server_default='', comment=u'上下文')
    status = Column(Integer, server_default=0, comment=u'当前步骤执行状态 未执行0 1已执行 2执行中 3中断')


def get_task_queue():
    session = db_session()
    try:
        result = session.query(TaskQueue).filter(TaskQueue.status == Status.wait).order_by(TaskQueue.created_at.desc()).limit(1).offset(0).all()
        if result:
            return result
        return []
    finally:
        session.close()


def get_task_step_info(task_key):
    session = db_session()
    try:
        result = session.query(TaskStepInfo).filter(TaskStepInfo.task_key == task_key).order_by(TaskStepInfo.id).all()
        if result:
            return result
        return []
    finally:
        session.close()


def get_task_step_meta(task_queue_id):
    session = db_session()
    try:
        print(task_queue_id)
        result = session.query(TaskStepMeta).filter(TaskStepMeta.task_queue_id == task_queue_id).order_by(TaskStepMeta.id.desc()).all()
        print(result)
        if result:
            return result
        return []
    finally:
        session.close()


def get_task_step_record(task_queue_id, step_name):
    # 获取真正执行的某一步的信息
    session = db_session()
    try:
        result = session.query(TaskStepMeta).filter(TaskStepMeta.task_queue_id == task_queue_id, TaskStepMeta.step_name==step_name).first()
        if result:
            return result
        raise Exception("error")
    finally:
        session.close()


def insert_task_step_meta_info(step_infos):
    # 跟局queen的新任务，新增该任务TaskStepMeta
    session = db_session()
    try:
        for step_info in step_infos:
            session.add(TaskStepMeta(
                task_queue_id=step_info["task_queue_id"],
                task_key=step_info["task_key"],
                step_name=step_info["step_name"],
                context=json.dumps({})
            ))
        session.commit()
    finally:
        session.close()


def save_task_step_meta_context(context, step_name, task_queue_id):
    # 保存任务上下文
    session = db_session()
    try:
        result = session.query(TaskStepMeta).filter(
            TaskStepMeta.task_queue_id == task_queue_id,
            TaskStepMeta.step_name == step_name,

        ).update({
            "context": context
        })
        if result == 0:
            raise Exception("no match")

        session.commit()
    finally:
        session.close()