#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-07-17 20:18
# @Author  : leason
import logging
import time

LOGGERS = {}


class LoggerHandle(logging.Handler):
    def __init__(self, step_instance):
        """
        step 执行的实例
        :param step_instance:
        """
        self.step_instance = step_instance
        logging.Handler.__init__(self)

    def emit(self, record):
        """
        Emit a record.
        """
        self.step_instance.add_exec_log(self.format(record))

    def format(self, record):
        ct = time.localtime(record.created)
        record.asctime = time.strftime("%Y-%m-%d %H:%M:%S", ct)
        format_str = "{asctime} {task_key}/{step_name} {levelname} {process} {message}".format(
            asctime=record.asctime,
            task_key=self.step_instance.task_key,
            step_name=self.step_instance.step_name,
            levelname=record.levelname,
            process=record.process,
            message=record.getMessage()
        )
        return format_str


def get_logger_for_task(step_instance, log_level='debug'):
    """
    Set up a logger which logs all the messages with level DEBUG and above to stderr.
    """
    logger_name = 'task.%s.step.%s' % (step_instance.task_key, step_instance.step_name)

    if logger_name not in LOGGERS:
        level_name = log_level.upper()
        log_level_constant = getattr(logging, level_name, logging.DEBUG)
        logger = logging.getLogger(logger_name)

        console = logging.StreamHandler()
        console.setLevel(log_level_constant)

        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        logger.addHandler(console)

        db_log = LoggerHandle(step_instance)
        db_log.setLevel(log_level_constant)
        logger.addHandler(db_log)

        logger.setLevel(log_level_constant)
        LOGGERS[logger_name] = logger
    else:
        logger = LOGGERS[logger_name]

    return logger
