#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 14:42
# @Author  : Leason

"""
任务流执行单位
"""
import json

from taskengine.core.models import save_task_step_meta_context


class BaseEngine(object):

    def __init__(self, task):
        """
        task queue model
        :param task: task queue model
        """
        self.params = task.params
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
        save_task_step_meta_context(json.dumps(self.context), step_name, task_queue_id)

    def serialize_context(self, context):
        """
        把持久化的context 重新序列化
        :return:
        """
        self.context = json.loads(context)

