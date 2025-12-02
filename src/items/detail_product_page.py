from scrapy import Field

from rmq.items import RMQItem


class DetailProductPageItem(RMQItem):
    prudct_url = Field()
    meta = Field()


