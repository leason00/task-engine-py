#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 17:14
# @Author  : Leason
import time

from taskengine.core.base_engine import BaseEngine
__all__ = [
    'Test'
]


class Test(BaseEngine):

    @staticmethod
    def task_key(task_key):
        return task_key == "test_key_2"

    def step_one(self):
        self.logger.info("step_one doing ")
        self.logger.info("get context{}".format(self.context))
        time.sleep(10)
        self.context["step_one"] = "step_1"

    def step_two(self):
        self.logger.info("step_two doing ")
        self.logger.info("get context{}".format(self.context))

        time.sleep(10)
        self.context["step_two"] = "step_2"
        # raise Exception("test error")

    def step_three(self):
        self.step_two()
        self.logger.info("step_three doing ")
        time.sleep(10)

        self.logger.error("get context{}".format(self.context))
        time.sleep(10)
        self.context["step_three"] = "step_3"
