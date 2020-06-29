#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 14:42
# @Author  : Leason

"""
任务流执行base class
"""
import json

from taskengine.core.models.step_execute import StepExecute


class BaseEngine(object):

    def __init__(self, task):
        """
        task queue model
        :param task: task queue model
        """
        try:
            self.params = json.loads(task.params)
        except:
            self.params = {}
        self.context = {

        }

    @staticmethod
    def task_key(task_key):
        return False

    def save_context(self, step_name, task_queue_id):
        """
        把context持久化,
        :return:
        """
        StepExecute.save_step_context(json.dumps(self.context), step_name, task_queue_id)

    def serialize_context(self, context):
        """
        把持久化的context 重新序列化
        :return:
        """
        self.context = json.loads(context)

