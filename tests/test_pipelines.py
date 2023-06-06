import pytest
from scrapy import exceptions

from eupas.pipelines import DuplicatesPipeline


@pytest.fixture()
def d_pipeline(simple_spider):
    d_pipeline = DuplicatesPipeline()
    d_pipeline.open_spider(simple_spider)
    return d_pipeline


def test_duplicate_pipeline(d_pipeline, simple_item, simple_spider):
    d_pipeline.process_item(simple_item, simple_spider)
    with pytest.raises(exceptions.DropItem):
        d_pipeline.process_item(simple_item, simple_spider)
