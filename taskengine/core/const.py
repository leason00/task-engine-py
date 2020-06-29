#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 15:20
# @Author  : Leason


class TaskStatus(object):
    created = "created"  # 等待执行
    waiting = "waiting"  # 等待执行
    doing = "doing"  # 执行中
    fail = "fail"  # 中断
    done = "done"  # 执行完


class StepStatus(object):
    # 未执行0已执行1执行中2中断3
    waiting = "waiting"
    doing = "doing"
    done = "done"
    fail = "fail"
