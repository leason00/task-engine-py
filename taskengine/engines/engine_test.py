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
        print("step_one doing ")
        print("get context{}".format(self.context))
        time.sleep(10)
        self.context["step_one"] = "step_1"

    def step_two(self):
        print("step_two doing ")
        print("get context{}".format(self.context))

        time.sleep(10)
        self.context["step_two"] = "step_2"
        # raise Exception("test error")

    def step_three(self):
        print("step_three doing ")
        print("get context{}".format(self.context))
        time.sleep(10)
        self.context["step_three"] = "step_3"
