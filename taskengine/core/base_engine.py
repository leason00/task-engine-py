#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 14:42
# @Author  : Leason

"""
任务流执行base class
"""
import json


class BaseEngine(object):

    def __init__(self, task):
        """
        task instance
        :param task: task queue model
        """
        self.task = task
        try:
            self.params = json.loads(task.params)
        except:
            self.params = {}
        self.context = {

        }

    @staticmethod
    def task_key(task_key):
        return False

    def save_context(self):
        """
        把context持久化,
        :return:
        """
        self.task.save_step_context(json.dumps(self.context))

    def serialize_context(self):
        """
        把持久化的context 重新序列化
        :return:
        """
        if self.task.context:
            self.context = json.loads(self.task.context)
