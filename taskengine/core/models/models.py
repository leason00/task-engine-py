#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-03-15 16:20
# @Author  : Leason
import json

from sqlalchemy import Column, Integer, String, DateTime, func, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class FormatMixin(object):
    id = Column(Integer, primary_key=True, comment=u'主键ID')
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'创建时间')
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), comment=u'更新时间')
    is_deleted = Column(Boolean, nullable=False, server_default="0", comment=u'逻辑删除')


    @property
    def json_formatted(self):
        dict_ = {}
        for key in self.__mapper__.c.keys():
            value = getattr(self, key)
            dict_[key] = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else value
        return dict_








