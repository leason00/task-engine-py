#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-06-29 22:04
# @Author  : leason
import json

from sqlalchemy import Column, Integer, String, DateTime, Boolean, func, Text

from taskengine.core.const import StepStatus
from taskengine.core.models.models import Base, FormatMixin
from taskengine.db.mysql import db_session


class StepExecute(Base, FormatMixin):
    """
        每步执行信息
    """
    __tablename__ = 'step_execute'

    task_execute_id = Column(Integer, primary_key=True, comment=u'任务id')
    task_key = Column(String(100), nullable=False, server_default='', comment=u'task key')
    step_name = Column(String(100), nullable=False, server_default='', comment=u'每个步骤名字')
    order_id = Column(String(100), nullable=False, server_default='', comment=u'顺序')
    timeout = Column(String(100), nullable=False, server_default='', comment=u'超时时间')
    context = Column(String(2048), nullable=False, server_default='', comment=u'上下文')
    exec_log = Column(Text, server_default='no log', comment=u'该步骤的执行日志')
    status = Column(String(100), server_default=StepStatus.waiting, comment=u'当前步骤执行状态 未执行0 1已执行 2执行中 3中断')

    start_time = Column(DateTime, nullable=False, server_default=func.now(), comment=u'开始时间')
    end_time = Column(DateTime, nullable=False, server_default=func.now(), comment=u'结束时间')

    @staticmethod
    def get_steps_by_task_id(task_queue_id):
        session = db_session()
        try:
            print(task_queue_id)
            result = session.query(StepExecute).filter(StepExecute.task_execute_id == task_queue_id).order_by(
                StepExecute.id.desc()).all()
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
                session.add(StepExecute(
                    task_execute_id=step_info["task_execute_id"],
                    task_key=step_info["task_key"],
                    step_name=step_info["step_name"],
                    status=step_info.get("status", StepStatus.waiting),
                    exec_log=step_info.get("exec_log", "no log"),
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
            result = session.query(StepExecute).filter(
                StepExecute.task_execute_id == task_queue_id,
                StepExecute.step_name == step_name,

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
            result = session.query(StepExecute).filter(StepExecute.task_execute_id == task_queue_id,
                                                       StepExecute.step_name == step_name).first()
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
            result = session.query(StepExecute).filter(
                StepExecute.task_execute_id == task_queue_id,
                StepExecute.step_name == step_name,

            ).update({
                "status": status
            })
            if result == 0:
                raise Exception("no match")

            session.commit()
        finally:
            session.close()
