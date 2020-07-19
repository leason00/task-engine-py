#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 14:26
# @Author  : Leason
from .engine_test import Test
from .deploy.engine_test2 import Test2 as Test2

TASK_MAP = {
    "test_key_2": Test,
    "test_key_1": Test2,
}