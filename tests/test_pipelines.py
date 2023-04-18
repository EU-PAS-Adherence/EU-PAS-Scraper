import pytest
from scrapy import spiders, exceptions

from encepp.pipelines import DuplicatesPipeline


@pytest.fixture()
def simple_spider():
    return spiders.Spider(name='pytest')


@pytest.fixture()
def d_pipeline(simple_spider):
    d_pipeline = DuplicatesPipeline()
    d_pipeline.open_spider(simple_spider)
    return d_pipeline


@pytest.fixture(params=[1234, 999, 'Hello'])
def simple_item(request):
    return {'eu_pas_register_number': f'EUPAS{request.param}'}


def test_duplicate_pipeline(d_pipeline, simple_item, simple_spider):
    d_pipeline.process_item(simple_item, simple_spider)
    with pytest.raises(exceptions.DropItem):
        d_pipeline.process_item(simple_item, simple_spider)
