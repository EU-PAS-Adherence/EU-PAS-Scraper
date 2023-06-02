from pathlib import Path

import pytest
from scrapy import spiders
from scrapy.settings import Settings
from scrapy.crawler import Crawler

from encepp.spiders.encepp_spider import EU_PAS_Extractor
import encepp.settings as settings_module


@pytest.fixture()
def tmp_path(tmp_path):
    path = Path('.tmp/')
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture()
def simple_spider():
    return spiders.Spider(name='pytest')


@pytest.fixture(params=[1234, 999, 'Hello'])
def simple_item(request):
    return {'eu_pas_register_number': f'EUPAS{request.param}'}


@pytest.fixture()
def project_settings():
    settings = Settings()
    settings.setmodule(settings_module)
    settings.set('TWISTED_REACTOR',
                 "twisted.internet.selectreactor.SelectReactor")
    settings.set("FEEDS", None)
    return settings


@pytest.fixture()
def crawler(project_settings):
    return Crawler(EU_PAS_Extractor, project_settings)
