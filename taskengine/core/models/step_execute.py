#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-06-29 22:04
# @Author  : leason

from sqlalchemy import Column, Integer, String, DateTime, func, Text

from taskengine.core.const import StepStatus
from taskengine.core.models.models import Base, FormatMixin
from taskengine.core.db.mysql import db_session


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
    exec_log = Column(Text, server_default='no log', comment=u'该步骤的执行日志')
    status = Column(String(100), server_default=StepStatus.waiting, comment=u'当前步骤执行状态 未执行0 1已执行 2执行中 3中断')

    start_time = Column(DateTime, nullable=False, server_default=func.now(), comment=u'开始时间')
    end_time = Column(DateTime, nullable=False, server_default=func.now(), comment=u'结束时间')

    @staticmethod
    def get_steps_by_task_id(task_queue_id):
        session = db_session()
        try:
            result = session.query(StepExecute).filter(StepExecute.task_execute_id == task_queue_id).order_by(
                StepExecute.id.desc()).all()
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
                    exec_log=step_info.get("exec_log", "no log")
                ))
            session.commit()
        finally:
            session.close()

    @staticmethod
    def get_step(task_execute_id, step_name):
        # 获取真正执行的某一步的信息
        session = db_session()
        try:
            result = session.query(StepExecute).filter(StepExecute.task_execute_id == task_execute_id,
                                                       StepExecute.step_name == step_name).first()
            if result:
                return result
            raise Exception("error")
        finally:
            session.close()

    def set_step_status(self, status):
        # todo 限定status的取值
        # 设置该step的状态
        session = db_session()
        try:
            result = session.query(StepExecute).filter(
                StepExecute.task_execute_id == self.task_execute_id,
                StepExecute.step_name == self.step_name,

            ).update({
                "status": status
            })
            if result == 0:
                raise Exception("no match")

            session.commit()
        finally:
            session.close()

    def add_exec_log(self, record):
        session = db_session()
        try:
            result = session.query(StepExecute).filter(
                StepExecute.id == self.id,
            ).first()
            if result:
                if result.exec_log:
                    result.exec_log = "{}\n{}".format(result.exec_log, record)

                session.flush()
                session.commit()
        finally:
            session.close()
