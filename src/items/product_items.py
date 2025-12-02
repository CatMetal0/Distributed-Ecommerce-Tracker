from scrapy import Field

from rmq.items.rmq_item import RMQItem


class ProductItem(RMQItem):

    product_task_id = Field()
    session_id = Field()
    position= Field()
    product_url = Field()
    name = Field()
    category = Field()
    rating = Field()
    description = Field()
    usual_price = Field()
    current_price = Field()
    product_availability = Field()
    quantity = Field()
    brand = Field()
    url_images = Field()
    additional_attributes = Field()
