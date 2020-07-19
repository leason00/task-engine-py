#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-07-19 16:34
# @Author  : leason
import importlib
import os


def gci(filepath, fils):
    # 遍历filepath下所有文件，包括子目录
    files = os.listdir(filepath)
    for fi in files:

        fi_d = os.path.join(filepath, fi)
        if os.path.isdir(fi_d):
            gci(fi_d, fils)
        else:
            fils.append(os.path.join(filepath, fi_d))


def get_task_files(path):
    fils = []
    gci(path, fils)
    sub_fils = []
    for i in fils:
        if os.path.isfile(i) and os.path.splitext(i)[1] == '.py' and not i.endswith("__init__.py"):
            sub_fils.append(i.split(".py")[0])
    return sub_fils


def load_tasks(task_key):
    project_path = os.path.abspath('..')
    engine_path = project_path + "/engines"
    task_paths = get_task_files(engine_path)
    for task_path in task_paths:
        import_path = "engines{}".format(task_path.split(engine_path)[1])
        p = project_path.split("/")
        pd = p[len(p) - 1]
        import_module = "{}.{}".format(pd, ".".join(import_path.split("/")))
        module_t = importlib.import_module(import_module)
        for class_name in dir(module_t):
            if class_name.startswith("__"):
                continue
            class_x = getattr(module_t, class_name)
            if hasattr(class_x, "task_key") and getattr(class_x, "task_key")(task_key):
                return class_x
    raise Exception("task key is {} is not found".format(task_key))


if __name__ == "__main__":
    class_x = load_tasks("test_key_1")
    print(class_x.__name__)
