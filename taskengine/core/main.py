#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 14:39
# @Author  : Leason
"""
保存本地task key  和 代码map
"""
import time

from taskengine.core.models import get_task_queue, get_task_step_info, get_task_step_meta, insert_task_step_meta_info, \
    get_task_step_record
from taskengine.engines.engine_test import Test

task_map = {
    "test": Test
}


class TaskEngine(object):

    def __init__(self, task):
        self.task = task

    def get_engine(self):
        """
        获取需要执行的task
        根据task key获取本地代码
        :return:
        """
        print(self.task.task_key)
        return task_map[self.task.task_key]

    def serialize_context(self):
        """
        把持久化的context 重新序列化
        :return:
        """
        pass

    def get_steps(self):
        """
        获取steps
        获取完整steps列表
        判断step meta存不存在这个task queue
        不存在插入step meta 否则是中断任务重新开始
        :return:
        """
        steps = get_task_step_info(self.task.task_key)
        if get_task_step_meta(self.task.id):
            raise Exception("no support")
        else:
            # 插入step meta
            step_infos = []
            for step in steps:
                step_infos.append({
                    "step_name": step.step_name,
                    "task_queue_id": self.task.id,
                    "task_key": self.task.task_key,
                })
            print(step_infos)
            insert_task_step_meta_info(step_infos)
            return steps

    def do_steps(self):
        """
        根据拿到的steps
        第一个未执行的开始执行
        执行完更新context
        :return:
        """
        engine = self.get_engine()(self.task)
        steps = self.get_steps()
        for step in steps:
            # 读取上下文
            record_step_meta = get_task_step_record(self.task.id, step.step_name)
            getattr(engine, "serialize_context")(record_step_meta.context)
            # 执行
            getattr(engine, step.step_name)()
            # 保存执行上线文
            getattr(engine, "save_context")(step.step_name, self.task.id)


class Main(object):
    def __init__(self):
        """
        获取数据库所有注册的task
        获取本地所有的task
        """

    def get_tasks(self):
        """
        获取当前老的待执行的task
        :return:
        """
        return get_task_queue()

    def do(self):
        """
        干活入口
        :return:
        """
        tasks = self.get_tasks()
        print(tasks)
        for task in tasks:
            TaskEngine(task).do_steps()

def main():
    """

    :return:
    """
    print("start...")
    main_func = Main()
    while True:
        main_func.do()
        time.sleep(5)

if __name__ == "__main__":
    main()
