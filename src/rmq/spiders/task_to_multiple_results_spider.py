# -*- coding: utf-8 -*-
from rmq.extensions import RPCTaskConsumer
from rmq.spiders import TaskBaseSpider


class TaskToMultipleResultsSpider(TaskBaseSpider):
    name = "multiple"

    def __init__(self, *args, **kwargs):
        super(TaskToMultipleResultsSpider, self).__init__(*args, **kwargs)
        self.completion_strategy = RPCTaskConsumer.CompletionStrategies.REQUESTS_BASED
