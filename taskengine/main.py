#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-06-29 22:48
# @Author  : leason
import time

import redis

from taskengine.core.main import Main


class Task(object):
    def __init__(self):
        self.rcon = redis.StrictRedis(host='localhost', db=0)
        self.queue = 'task_engine_queue'

    def listen_task(self):
        while True:
            task = self.rcon.rpop(self.queue)

            if task:
                task_id = task.split(":")[0]
                task_key = task.split(":")[1]
                print("Task get", task_key)
                Main().do(task_id)
            else:
                time.sleep(1)


if __name__ == '__main__':
    print('listen task queue')
    # Task().listen_task()
    Main().do(8)
