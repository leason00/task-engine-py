#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 15:20
# @Author  : Leason


class TaskStatus(object):
    created = "created"  # 等待执行
    scheduling = "scheduling"  # 等待执行
    doing = "doing"  # 执行中
    interrupt = "interrupt"  # 中断
    finnish = "finnish"  # 执行完


class TaskStepInfoStatus(object):
    # 未执行0已执行1执行中2中断3
    wait = "wait"
    doing = "doing"
    finnish = "finnish"
    interrupt = "interrupt"
