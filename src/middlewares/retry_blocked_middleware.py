import json

from scrapy import Spider
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Request, Response
from twisted.python.failure import Failure

from rmq.utils import RMQConstants, TaskStatusCodes
from utils import LoggerMixin


class RetryBlockedMiddleware(LoggerMixin):
    MAX_LOCAL_RETRIES = 3
    BLOCKED_CODES = {403, 429, 503}

    def process_response(self, request: Request, response: Response, spider: Spider):
        if response.status not in self.BLOCKED_CODES:
            return response

        retries = request.meta.get("blocked_retry_count", 0)

        self.logger.warning(
            f"Blocked ({response.status}) for {request.url} [try {retries + 1}/{self.MAX_LOCAL_RETRIES}]"
        )

        # Local retry
        delivery_tag = request.meta.get(RMQConstants.DELIVERY_TAG_META_KEY.value)
        if retries < self.MAX_LOCAL_RETRIES:
            spider.processing_tasks.handle_response(delivery_tag, response.status)
            new_meta = request.meta.copy()
            new_meta["blocked_retry_count"] = retries + 1
            new_meta["original_url"] = request.url
            new_req = request.replace(
                dont_filter=True,
                headers=request.headers,
                cb_kwargs=request.cb_kwargs,
                meta=new_meta
            )
            return new_req


        # No more retries
        task = spider.processing_tasks.get_task(delivery_tag)
        if task is None:
            self.logger.error(f"No task found for delivery_tag={delivery_tag}")
            raise IgnoreRequest(f"Blocked response {response.status}")

        # Prefer errback path if present → it will inject error into Task via @rmq_errback
        if request.errback:
            failure = Failure(IgnoreRequest(f"HTTP {response.status} after retries: {request.url}"))
            failure.request = request
            failure.response = response
            return request.errback(failure)


        spider.processing_tasks.set_status(delivery_tag, TaskStatusCodes.ERROR.value)
        spider.processing_tasks.set_exception(
            delivery_tag,
            json.dumps({"message": f"HTTP {response.status} after retries: {request.url}", "traceback": None})
        )

        try:
            task.ack()
        finally:
            spider.processing_tasks.remove_task(delivery_tag)
        # Stop further processing of this response
        raise IgnoreRequest(f"Blocked response {response.status}")

