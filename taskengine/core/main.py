#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 14:39
# @Author  : Leason
"""
保存本地task key  和 代码map
"""
import json
import time
import traceback

import nsq

from taskengine.core.const import TaskStepInfoStatus, TaskStatus
from taskengine.core.models.models import TaskQueue, TaskStepMeta, TaskStepInfo
from taskengine.engines.engine_test import Test

# todo task的获取采用从代码路径里读取
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
        steps = TaskStepMeta.get_steps(self.task.task_key)
        if not steps:
            raise Exception("this task {} is not support".format(self.task.task_key))

        step_instances = TaskStepInfo.get_steps_by_task_id(self.task.id)
        if not step_instances:
            # 第一次执行
            # 实例化task，将task step插入
            step_infos = []
            for step in steps:
                step_infos.append({
                    "step_name": step.step_name,
                    "task_queue_id": self.task.id,
                    "task_key": self.task.task_key,
                })
            print(step_infos)
            TaskStepInfo.insert_steps(step_infos)
        return steps

    def do_steps(self):
        """
        根据拿到的steps
        第一个未执行的开始执行
        执行完更新context
        :return:
        """
        # todo 获取task的方式待修改
        engine = self.get_engine()(self.task)
        # 设置task执行中
        TaskQueue.update_status(self.task.id, TaskStatus.doing)
        # 获取顺序的steps
        steps = self.get_steps()
        for step in steps:
            # 从 task里指定的那个step开始执行，这个step可以是第一次的first step，也可以是重试的任意一个step
            if step.step_name != self.task.step_name:
                continue
            # 获取当前步骤的信息
            step_instance = TaskStepInfo.get_step(self.task.id, step.step_name)
            # 设置该step为执行中
            TaskStepInfo.set_step_status(self.task.id, step.step_name, TaskStepInfoStatus.doing)
            # 将上下文信息写出engine class
            engine.serialize_context(step_instance.context)
            # 执行对应步骤的class method
            try:
                getattr(engine, step.step_name)()
            except Exception as e:
                # 执行失败设置该step执行失败， 并且设置task执行失败，且记录该task所执行到的step,interrupt
                TaskStepInfo.set_step_status(self.task.id, step.step_name, TaskStepInfoStatus.interrupt)
                TaskQueue.update_status(self.task.id, TaskStatus.interrupt, step_name=step.step_name)
                print(traceback.format_exc())
                # todo 执行过程中的所有日志存到mysql
            finally:
                # todo 执行日志输出到数据库 https://blog.csdn.net/J_Object/article/details/80179535
                # 执行完之后将上线文重新保存
                engine.save_context(step.step_name, self.task.id)
                # 执行完之后修改该step的finish状态
                TaskStepInfo.set_step_status(self.task.id, step.step_name, TaskStepInfoStatus.finnish)

        # 所有步骤执行完之后，设置该task执行成功，task name为最后一步 do_end
        TaskQueue.update_status(self.task.id, TaskStatus.finnish, step_name=steps[len(steps)-1].step_name)


class Main(object):
    def __init__(self):
        """
        获取数据库所有注册的task
        获取本地所有的task
        """

    def do(self, task_id):
        """
        干活入口
        :return:
        """
        task = TaskQueue.get_task_by_id(task_id)
        TaskEngine(task).do_steps()


# def main():
#     """
#
#     :return:
#     """
#     print("start...")
#     main_func = Main()
#     while True:
#         main_func.do()
#         time.sleep(5)


def handler(message):
    message.enable_async()
    message_dict =json.loads(message.data)
    task_id = message_dict["id"]
    Main().do(task_id)




if __name__ == '__main__':
    reader = nsq.Reader(
        topic='leason-test',
        channel='test2',
        sub_ordered=True,
        lookupd_http_addresses=['sqs-qa.s.qima-inc.com:4161'],
        message_handler=handler,
        requeue_delay=2,
        user_agent='youzan pynsq'
    )
    nsq.run()
