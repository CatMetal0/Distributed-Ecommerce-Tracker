import json
import re
from math import ceil
from typing import List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy
from scrapy.http import Response

from items import DetailProductPageItem
from rmq.pipelines import ItemProducerPipeline
from rmq.spiders import TaskToMultipleResultsSpider
from rmq.utils import RMQConstants, TaskStatusCodes, get_import_full_name
from rmq.utils.decorators import rmq_callback
from settings import CATEGORY_QUILL_TASK, CATEGORY_RESULTS


class CategorySpiderQuill(TaskToMultipleResultsSpider):

    name = "quill_category_spider"
    custom_settings = {
        "USER_AGENT": None,
        "ITEM_PIPELINES": {
            get_import_full_name(ItemProducerPipeline): 310,
        },
    }

    allowed_domains = ["www.quill.com"]

    def __init__(self, *args, **kwargs):
        super(CategorySpiderQuill, self).__init__(*args, **kwargs)
        self.task_queue_name = CATEGORY_QUILL_TASK
        self.result_queue_name = CATEGORY_RESULTS


    def start_requests(self):
        if False:
            yield


    def next_request(self, _delivery_tag, msg_body):
        data = json.loads(msg_body)
        url = data["url"]
        self.logger.debug(f"New task received: {url}")
        return scrapy.Request(
            url,
            callback=self.parse,
            errback=self._errback,
            meta={
                    RMQConstants.DELIVERY_TAG_META_KEY.value: _delivery_tag,
                    "msg_body": data,
                },
            )


    def _extract_product_urls(self, response: Response) -> List[str]:
        # Extracts product URLs from the category page.
        # This XPath was already correct for quill.com's product grid.
        product_hrefs = response.xpath('//span[@class = "body-xs d-block search-product-name-wrap"]/a/@href').getall()
        return [response.urljoin(href) for href in product_hrefs]


    def _extract_page_count(self, response: Response) -> int:
        # Determines total number of pages based on total product count on the site and
        # number of products on the current(first) page
        try:
            displaying_count_text = response.xpath(
                "//*[@id=\"Pager\"]//div[contains(text(), \"Displaying\")]/text()"
                ).get()
            f_part_text: str = displaying_count_text.split("of")[0]
            product_count_text = str(f_part_text.split("-")[-1]).strip()
            products_on_page = int(re.sub(r"\D", "", product_count_text))

            if products_on_page == 0:
                self.logger.warning(f"No products found on page: {response.url}. Defaulting to 1 page.")
                return 1

            all_products_text = response.xpath('//span[@class="txtXL"]/text()').get()

            if not all_products_text:
                self.logger.warning(f"Could not find product count text on {response.url}. Defaulting to 1 page.")
                return 1

            apc = int(re.sub(r"\D", "", all_products_text))
            if apc == 0:
                 self.logger.warning(f"Extracted 0 products from count string on {response.url}. Defaulting to 1 page.")
                 return 1
            page_count = ceil(apc / products_on_page)
        except (ValueError, TypeError):
             self.logger.error(f"Could not parse product count from text: '{all_products_text}'. Defaulting to 1 page.")
             return 1

        return page_count


    def _build_paged_url(self, base_url: str, page_number: int) -> str:
        parsed = urlparse(base_url)
        params = parse_qs(parsed.query)

        params["page"] = [str(page_number)]
        new_query = urlencode(params, doseq=True)

        return urlunparse(parsed._replace(query=new_query))

    def _update_current_task(self, delivery_tag):
        
        task_obj = self.processing_tasks.get_task(delivery_tag)
        if task_obj:
            task_obj.payload["item_count"] = task_obj.scheduled_items


    @rmq_callback
    def parse(self, response: Response):
        url = response.url
        delivery_tag = response.meta.get(RMQConstants.DELIVERY_TAG_META_KEY.value)
        page = 1
        page_count = 1
        first_page = True
        task = response.meta.get("msg_body")
        task_id = task.get("task_id", None)
        session_id = task.get("session_id", None)
        meta = {
            RMQConstants.DELIVERY_TAG_META_KEY.value: delivery_tag,
            "msg_body": task,
        }
        try:
            product_urls = self._extract_product_urls(response)

            if "page=" not in url:
                page_count = self._extract_page_count(response)
            else:
                params = parse_qs(urlparse(url).query)
                if 'page' in params and params['page']:
                    page = int(params["page"][0])
                if page > 1:
                    first_page = False


            for idx, p_url in enumerate(product_urls):
                self.logger.info(f"position: {idx + 1 + 24 * (page - 1)}, page: {page} url:{url}")
                yield DetailProductPageItem(
                    prudct_url=p_url,
                    meta={
                        "position": idx + 1 + 24 * (page - 1),
                        "session_id": session_id,
                        "task_id": task_id,
                    }
                )

            if first_page and page_count > 1:
                
                for page_inx in range(2, min(page_count + 1, 140)): # broken paggination on site
                                                                    # 139 it's max page
                    next_page_url = self._build_paged_url(response.url, page_inx)
                    self.logger.debug(f"Queueing next page: {next_page_url}")
                    yield scrapy.Request(
                        url=next_page_url,
                        callback=self.parse,
                        errback=self.parse_error,
                        meta=meta
                    )

            self._update_current_task(delivery_tag)

        except (ValueError, KeyError, TypeError) as error:
            error_msg = f"Error while parsing: {str(error)}"
            self._inject_soft_exception_to_task(
                delivery_tag, TaskStatusCodes.ERROR.value, error_msg
            )
            self.logger.error(error_msg)

        except Exception as un_error:
            error_msg = f"Unhandled exception: {un_error} (URL: {url})"
            self._inject_exception_to_task(delivery_tag, un_error)
            self.logger.error(error_msg)

        
    def parse_error(self, failure):
        self.logger.warning(
            f"Failed to fetch paginated page: {failure.request.url} - Error: {failure.value}"
        )
