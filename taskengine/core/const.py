#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 15:20
# @Author  : Leason


class Status(object):
    wait = 0  # 等待执行
    doing = 1  # 执行中
    interrupt = 2  # 中断
    finnish = 3  # 执行完
