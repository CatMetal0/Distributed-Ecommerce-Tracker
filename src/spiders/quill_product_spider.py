import json

import scrapy
from jmespath import search as json_search
from scrapy.http import Response
from scrapy.utils.project import get_project_settings

from items.product_items import ProductItem
from rmq.extensions import RPCTaskConsumer
from rmq.pipelines import ItemProducerPipeline
from rmq.spiders import TaskToSingleResultSpider
from rmq.utils import get_import_full_name
from rmq.utils.decorators import rmq_callback, rmq_errback


class QuillProductSpider(TaskToSingleResultSpider):
    """
    Listens to 'product.quill.task',
    parses product data, and yields a single 'ProductItem' to the results queue.
    """
    name = 'quill_product_spider'

    custom_settings = {

        "ITEM_PIPELINES": {
            get_import_full_name(ItemProducerPipeline): 310,
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        settings = get_project_settings()
        self.task_queue_name = settings.get('PRODUCT_QUILL_TASK')
        self.result_queue_name = settings.get('RMQ_QUEUE_RESULTS')
        self.replies_queue_name = settings.get('RMQ_QUEUE_REPLIES')

        self.completion_strategy = RPCTaskConsumer.CompletionStrategies.REQUESTS_BASED

    def start_requests(self):
        if False:
            yield

    def next_request(self, _delivery_tag, msg_body) -> scrapy.Request:
        """
        Consumes a message from the task queue.
        """
        try:
            message = json.loads(msg_body)
            task_id = message.get('task_id')
            url = message.get('url')
            session_id = message.get('session_id')

            if not task_id or not url or not session_id:
                raise ValueError("Invalid message (missing task_id, url, or session_id)")
            return scrapy.Request(
                url,
                callback=self.parse,
                errback=self._errback,
                meta={
                    'task_id': task_id,
                    'session_id': session_id,
                    'original_url': url,
                    'delivery_tag': _delivery_tag, 
                    'msg_body': msg_body,
                },
                dont_filter=True
            )
        except Exception as e:
            self.logger.error(f"Error in next_request: {e}")
            raise

    def _extract_name(self, response: Response):
        name = response.xpath(
            '//h1[contains(@class, "skuName")]/text()'
        ).get()

        if name:
            return name
        return None

    def _extract_brand(self, json_data: dict):
        brand = json_search("brand", json_data)

        if brand:
            return brand
        return None

    def _extract_category(self, response: Response):
        category = response.xpath(
            "//ol/li/a/span/text()"
        ).getall()[-1]

        if category:
            return category
        return None

    def _extract_description(self, response: Response):
        description = response.xpath(
            '//div[contains(@class, "text-left") and contains(@class, "text-justify")]/span[2]/text()'
        ).get()

        if description:
            return description
        return None

    def _extract_usual_price(self, response: Response):
        price = response.xpath(
            '//div[@class="body-sm mb-2 d-flex fg-jet-tint savings-price-section align-items-center flex-wrap"]'
            '/span[@class="elp-percentage"]/del[@class="p-0 fg-jet-tint"]/text()'
        ).get()

        if price:
            return price.strip().lstrip("$")

        return None

    def _extract_current_price(self, response: Response):
        price = response.xpath(
            '//div[contains(@class,"savings-highlight-wrap")]/span[contains(@class,"savings-highlight")]/text()'
        ).get()

        if price:
            return price.strip().lstrip('$')
        return None

    def _extract_url_images(self, response: Response):
        url_images = response.xpath(
            '//img[@id="SkuPageMainImg"]/@src'
        ).get()

        if url_images:
            return url_images
        return None


    def _extract_additional_attributes(self, response):

        rows = response.xpath(
            "//div[@class='row row-cols-2 pt-4 body-xs row-cols-md-4']/div"
        )

        attributes = {}
        for i in range(0, len(rows), 2):
            key = rows[i].xpath(".//span/text()").get()
            if not key:
                continue
            key = key.strip()

            value_parts = rows[i + 1].xpath(".//text()").getall()
            value = " ".join(v.strip() for v in value_parts if v.strip())

            attributes[key] = value

        return json.dumps(attributes, ensure_ascii=False)

    def _extract_json_schema(self, response: Response) -> dict:
        json_text = response.xpath(
            "//script[@id=\"SEOSchemaJson\" and contains(text(), '\"@type\":\"Product\"')]/text()"
        ).get(r"{}")
        json_data = {}
        try:
            json_data = json.loads(json_text)
        except json.JSONDecodeError as err:
            self.logger.error(f"Error while conver json_scheme into dict. error:{err}\n schema:{json_text}")
        return json_data

    def _extract_rating(self, json_data: dict) -> float:
        rating = json_search("aggregateRating.ratingValue", json_data)
        if not rating:
            return None
        return rating

    @rmq_callback
    def parse(self, response):
        """
        Handles the response and yields a single ProductItem.
        """
        task_id = response.meta['task_id']
        session_id = response.meta['session_id']
        original_url = response.meta['original_url']
        msg_body = response.meta.get('msg_body', {})
        msg_body_dict = json.loads(msg_body)
        position = msg_body_dict.get('position', None)
        json_data = self._extract_json_schema(response)

        item = ProductItem()

        item['product_task_id'] = task_id
        item['session_id'] = session_id
        item['product_url'] = original_url
        item['position'] = position

        item['name'] = self._extract_name(response)
        item['brand'] = self._extract_brand(json_data)
        item['category'] = self._extract_category(response)
        item['description'] = self._extract_description(response)
        item['usual_price'] = self._extract_usual_price(response)
        item['current_price'] = self._extract_current_price(response)
        item['url_images'] = self._extract_url_images(response)
        item['rating'] = self._extract_rating(json_data)
        
        item['quantity'] = None
        item['product_availability'] = None
        item['additional_attributes'] = self._extract_additional_attributes(response)
        yield item

    @rmq_errback
    def _errback(self, failure):
        """
        Handles Scrapy-level errors.
        """
        task_id = failure.request.meta['task_id']
        self.logger.error(f"Scrapy Error {failure.value} (Task: {task_id})")
        pass