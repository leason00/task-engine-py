#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 17:14
# @Author  : Leason
import time

from taskengine.core.base_engine import BaseEngine


class Test(BaseEngine):

    @staticmethod
    def task_key(task_key):
        return task_key == "test_key_2"

    def step_one(self):
        time.sleep(10)
        self.context["step_one"] = "step_1"

    def step_two(self):
        self.context["step_one"] = "step_1"

    def step_finish(self):
        pass