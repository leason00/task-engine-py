#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-09-27 19:59
# @Author  : Leason
import json

from taskengine.core.const import TaskStatus
from taskengine.core.models.models import TaskQueue
import nsq
import tornado.ioloop


def main():

    def pub_message():
        # 获取created的task放入nsql

        tasks = TaskQueue.get_created_task_queue()
        for task in tasks:
            TaskQueue.update_status(task.id, TaskStatus.scheduling)
            writer.pub(json.dumps(task.json_formatted), callback=finish_pub)
            print("pub task {}".format(task.task_key))

    def finish_pub(conn, data):
        print(data)

    # query_lookupd_tcp('sqs-qa.s.qima-inc.com:4161', "leason-test")
    writer = nsq.Writer(topic="leason-test", part="0", lookupd_http_addresses='sqs-qa.s.qima-inc.com:4161')
    # writer = nsq.Writer(['10.9.4.61:4150'])
    tornado.ioloop.PeriodicCallback(pub_message, 1000).start()
    nsq.run()