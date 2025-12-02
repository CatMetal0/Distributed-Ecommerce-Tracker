# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import MEDIUMINT
from sqlalchemy.sql.expression import text


class MysqlPriorityAttemptMixin:
    priority = Column("priority", MEDIUMINT(unsigned=True), index=True, nullable=True)
    attempt = Column("attempt", MEDIUMINT(unsigned=True), nullable=True, server_default=text("0"))
